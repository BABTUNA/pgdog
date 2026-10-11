//! Client authentication tests.

use pgdog_config::{AuthType, PassthroughAuth};
use tokio::io::AsyncWriteExt;

use crate::{
    config::{config, set},
    expect_message,
    net::{Authentication, ErrorResponse, Parameters, Password},
};

use super::SpawnedClient;

/// Connect to the admin database and answer the plaintext password
/// request with the given password.
async fn login_admin(password: &str) -> SpawnedClient {
    let cfg = config();
    let mut params = Parameters::default();
    params.insert("user", cfg.config.admin.user.as_str());
    params.insert("database", cfg.config.admin.name.as_str());

    let mut client = SpawnedClient::new_with_login(params).await;

    // Both the admin and the passthrough branches request the password
    // in plaintext; what matters is what happens with the answer.
    let request = expect_message!(client.read().await, Authentication);
    assert!(matches!(request, Authentication::ClearTextPassword));

    client.send(Password::new_password(password)).await;
    client
}

/// Admin connections must be authenticated against the admin password even
/// when passthrough auth is enabled. Regression test for the passthrough
/// branch running first and accepting any password for the admin database.
#[tokio::test]
async fn test_admin_password_checked_with_passthrough_auth() {
    crate::logger();
    crate::config::load_test();

    let mut cfg = (*config()).clone();
    cfg.config.general.auth_type = AuthType::Plain;
    cfg.config.general.passthrough_auth = PassthroughAuth::EnabledPlain;
    cfg.config.admin.password = "admin-password".into();
    set(cfg).unwrap();

    // The wrong password is rejected instead of being passed through.
    let mut client = login_admin("not-the-admin-password").await;
    let error = ErrorResponse::try_from(client.read().await).unwrap();
    assert_eq!(error.code, "28000");
    client.join().await;

    // The correct password is accepted.
    let mut client = login_admin("admin-password").await;
    let response = expect_message!(client.read().await, Authentication);
    assert!(matches!(response, Authentication::Ok));
    client.read_until('Z').await;
    client.join().await;
}

/// Connect as a user and database that aren't configured.
async fn login_unknown(auth_type: AuthType) -> SpawnedClient {
    crate::logger();
    crate::config::load_test();

    let mut cfg = (*config()).clone();
    cfg.config.general.auth_type = auth_type;
    set(cfg).unwrap();

    let mut params = Parameters::default();
    params.insert("user", "nobody");
    params.insert("database", "nowhere");
    SpawnedClient::new_with_login(params).await
}

/// Unknown users get an auth request before the error, like Postgres, so health
/// checks like HAProxy pgsql-check see PgDog as up.
#[tokio::test]
async fn test_unknown_user_gets_auth_request() {
    for auth_type in [AuthType::Md5, AuthType::Scram, AuthType::Plain] {
        let mut client = login_unknown(auth_type).await;

        let request = expect_message!(client.read().await, Authentication);
        match auth_type {
            AuthType::Md5 => assert!(matches!(request, Authentication::Md5(_))),
            AuthType::Scram => assert!(matches!(request, Authentication::Sasl(_))),
            _ => assert!(matches!(request, Authentication::ClearTextPassword)),
        }

        let answer = match auth_type {
            AuthType::Scram => Password::sasl_initial("n,,n=,r=nonce"),
            _ => Password::new_password("password"),
        };
        client.send(answer).await;

        let error = ErrorResponse::try_from(client.read().await).unwrap();
        assert_eq!(error.code, "28000", "{auth_type}");
        client.join().await;
    }
}

/// HAProxy closes the connection as soon as it gets the auth request.
#[tokio::test]
async fn test_unknown_user_hangs_up_after_auth_request() {
    let mut client = login_unknown(AuthType::Scram).await;

    let request = expect_message!(client.read().await, Authentication);
    assert!(matches!(request, Authentication::Sasl(_)));

    client.conn.shutdown().await.unwrap();
    client.join().await;
}

/// Trust never asks for a password, so unknown users are still rejected right away.
#[tokio::test]
async fn test_unknown_user_trust_rejected() {
    let mut client = login_unknown(AuthType::Trust).await;

    let error = ErrorResponse::try_from(client.read().await).unwrap();
    assert_eq!(error.code, "28000");
    client.join().await;
}
