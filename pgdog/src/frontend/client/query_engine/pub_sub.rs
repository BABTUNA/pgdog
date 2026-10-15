use super::*;
use crate::backend::pool::Error as PoolError;
use crate::frontend::client::query_engine::fake::FakeResponse;
use crate::net::ProtocolMessage;

impl QueryEngine {
    /// Pub/sub is off by default. Answer LISTEN and NOTIFY with an error the client
    /// can recover from, instead of closing its connection.
    ///
    /// Returns `true` if the error was sent.
    async fn pub_sub_disabled(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        client_request: &ClientRequest,
    ) -> Result<bool, Error> {
        if config().config.general.pub_sub_enabled() {
            return Ok(false);
        }

        if let Some(transaction) = context.transaction.as_mut() {
            transaction.set_error();
        }
        self.error_response(
            context,
            client_request,
            ErrorResponse::unsupported(&PoolError::PubSubDisabled),
        )
        .await?;

        Ok(true)
    }

    pub(super) async fn listen(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        client_request: &ClientRequest,
        channel: &str,
        shard: Shard,
    ) -> Result<(), Error> {
        if self.pub_sub_disabled(context, client_request).await? {
            return Ok(());
        }

        self.backend.listen(channel, shard).await?;
        self.fake_command_response(
            context,
            &client_request.messages,
            &FakeResponse::command("LISTEN"),
        )
        .await?;

        Ok(())
    }

    pub(super) async fn notify(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        client_request: &ClientRequest,
        channel: &str,
        payload: &str,
        shard: &Shard,
    ) -> Result<(), Error> {
        if self.pub_sub_disabled(context, client_request).await? {
            return Ok(());
        }

        if context.in_transaction() {
            // Buffer the NOTIFY command if we're in a transaction
            self.notify_buffer
                .add(channel.to_string(), payload.to_string(), shard.clone());
        } else {
            // Send immediately if not in transaction
            self.backend.notify(channel, payload, shard.clone()).await?;
        }
        self.fake_command_response(
            context,
            &client_request.messages,
            &FakeResponse::command("NOTIFY"),
        )
        .await?;
        Ok(())
    }

    pub(super) async fn unlisten(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        client_messages: &[ProtocolMessage],
        channel: &str,
    ) -> Result<(), Error> {
        self.backend.unlisten(channel);
        self.fake_command_response(context, client_messages, &FakeResponse::command("UNLISTEN"))
            .await?;
        Ok(())
    }

    pub(super) async fn flush_notify(&mut self) -> Result<(), Error> {
        for notify_cmd in self.notify_buffer.drain() {
            self.backend
                .notify(&notify_cmd.channel, &notify_cmd.payload, notify_cmd.shard)
                .await?;
        }
        Ok(())
    }
}
