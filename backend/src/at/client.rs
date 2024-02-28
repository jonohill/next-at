use crate::gtfs::structure::realtime::FeedMessage;

use super::error::AtResult;
use url::Url;

#[derive(serde::Deserialize)]
struct RealtimeResponse<T> {
    response: T,
}

#[derive(Clone)]
pub struct AtClient {
    client: reqwest::Client,
}

impl AtClient {
    pub fn new() -> AtResult<AtClient> {

        let client = AtClient {
            client: reqwest::Client::builder()
                .build()
                .unwrap(),
        };

        Ok(client)
    }

    fn url(path: &str) -> Url {
        Url::parse("https://at-proxy.heaps.dev/")
            .unwrap()
            .join(path)
            .unwrap()
    }

    async fn request<T>(&self, url: Url) -> AtResult<T>
    where
        T: serde::de::DeserializeOwned,
    {
        log::debug!("Requesting {}", url);
        let response = self.client.get(url).send().await?;

        let data_str = response.text().await?;
        log::trace!("Response: {}", data_str);
        let data = serde_json::from_str(&data_str)?;

        Ok(data)
    }

    pub async fn get_realtime_feed(&self) -> AtResult<FeedMessage> {
        let url = AtClient::url("realtime.json");
        let RealtimeResponse::<FeedMessage> { response } = self.request(url).await?;
        Ok(response)
    }
}
