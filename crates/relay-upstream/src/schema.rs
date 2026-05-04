// SPDX-License-Identifier: MIT

use thiserror::Error;
use url::Url;

#[derive(Debug, Error)]
pub enum SchemaFetchError {
    #[error("invalid url: {0}")]
    Url(String),
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("server returned status {0}")]
    Status(u16),
}

/// GET `{host}/v1/database/{database}/schema?version=9`.
///
/// Returns the raw SATS-JSON body. Caller is responsible for parsing.
pub async fn fetch_schema(host: &Url, database: &str) -> Result<Vec<u8>, SchemaFetchError> {
    let mut url = host.clone();
    match url.scheme() {
        "ws" => url
            .set_scheme("http")
            .map_err(|_| SchemaFetchError::Url("scheme rewrite ws->http failed".into()))?,
        "wss" => url
            .set_scheme("https")
            .map_err(|_| SchemaFetchError::Url("scheme rewrite wss->https failed".into()))?,
        "http" | "https" => {}
        other => return Err(SchemaFetchError::Url(format!("unsupported scheme: {other}"))),
    }
    let mut path = url.path().trim_end_matches('/').to_string();
    path.push_str("/v1/database/");
    path.push_str(database);
    path.push_str("/schema");
    url.set_path(&path);
    url.query_pairs_mut()
        .clear()
        .append_pair("version", "9");

    let response = reqwest::get(url).await?;
    let status = response.status();
    if !status.is_success() {
        return Err(SchemaFetchError::Status(status.as_u16()));
    }
    Ok(response.bytes().await?.to_vec())
}
