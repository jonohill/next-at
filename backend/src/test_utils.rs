use crate::ContextData;

pub fn init() {
    dotenvy::from_filename(".dev.vars").ok();
    env_logger::try_init().ok();
}

pub fn at() -> crate::at::client::AtClient {
    init();
    crate::at::client::AtClient::new().unwrap()
}

#[cfg(test)]
pub async fn ctx() -> ContextData {
    init();

    todo!();

    // ContextData {
    //     at_client: at(),
    //     // db: db().await,
    // }
}
