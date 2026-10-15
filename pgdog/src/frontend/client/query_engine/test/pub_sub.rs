use std::io::ErrorKind;
use std::time::Duration;

use tokio::time::timeout;

use crate::{
    expect_message,
    net::{BindComplete, CommandComplete, ErrorResponse, Parameters, ParseComplete, ReadyForQuery},
};

use super::prelude::*;

#[tokio::test]
async fn extended_unlisten_flush_does_not_emit_ready_for_query() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;
    client.client.client_request = ClientRequest::from(vec![
        Parse::new_anonymous("UNLISTEN pgdog_pipeline_listen").into(),
        Bind::new_statement("").into(),
        Execute::new().into(),
        Flush.into(),
    ]);

    let (mut context, client_request) = QueryEngineContext::new(&mut client.client);
    client
        .engine
        .unlisten(
            &mut context,
            &client_request.messages,
            "pgdog_pipeline_listen",
        )
        .await
        .unwrap();

    expect_message!(client.read().await, ParseComplete);
    expect_message!(client.read().await, BindComplete);
    assert_eq!(
        expect_message!(client.read().await, CommandComplete).command(),
        "UNLISTEN"
    );

    let mut unexpected = [0];
    let error = client
        .conn
        .try_read(&mut unexpected)
        .expect_err("Flush-only extended response must not include ReadyForQuery");
    assert_eq!(error.kind(), ErrorKind::WouldBlock);
}

/// Regression for https://github.com/pgdogdev/pgdog/issues/1808.
#[tokio::test]
async fn pub_sub_disabled_returns_error_and_keeps_connection() {
    let mut client = SpawnedClient::new_sharded(Parameters::default()).await;

    timeout(Duration::from_secs(5), async {
        for query in [
            "LISTEN probe_ch",
            "NOTIFY probe_ch, 'hello'",
            "LISTEN probe_ch",
        ] {
            client.send(Query::new(query)).await;
            let error = expect_message!(client.read().await, ErrorResponse);
            assert_eq!(error.severity, "ERROR", "{query}");
            assert_eq!(error.code, "0A000", "{query}");
            assert_eq!(error.message, "pub/sub disabled", "{query}");
            assert_eq!(
                expect_message!(client.read().await, ReadyForQuery).status,
                'I'
            );
        }

        // Inside a transaction the error aborts it, like any other error.
        client.send(Query::new("BEGIN")).await;
        expect_message!(client.read().await, CommandComplete);
        assert_eq!(
            expect_message!(client.read().await, ReadyForQuery).status,
            'T'
        );

        client.send(Query::new("NOTIFY probe_ch")).await;
        assert_eq!(
            expect_message!(client.read().await, ErrorResponse).code,
            "0A000"
        );
        assert_eq!(
            expect_message!(client.read().await, ReadyForQuery).status,
            'E'
        );

        client.send(Query::new("ROLLBACK")).await;
        expect_message!(client.read().await, CommandComplete);
        assert_eq!(
            expect_message!(client.read().await, ReadyForQuery).status,
            'I'
        );
    })
    .await
    .expect("connection closed instead of returning an error");
}
