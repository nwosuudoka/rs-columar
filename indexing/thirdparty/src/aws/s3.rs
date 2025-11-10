use super::common::Result;
use aws_config::meta::region;
use aws_sdk_s3 as s3;

pub struct S3 {
    client: s3::Client,
    bucket: String,
}

const DEFAULT_REGION: &str = "us-east-2";

pub async fn get_client(region: String) -> std::result::Result<s3::Client, s3::Error> {
    let region: Option<String> = Option::Some(region);
    let region_provider =
        region::RegionProviderChain::first_try(region.map(s3::config::Region::new))
            .or_default_provider()
            .or_else(s3::config::Region::new(DEFAULT_REGION.to_string()));
    println!("Region: {}", region_provider.region().await.unwrap());
    let shard_config = aws_config::from_env().region(region_provider).load().await;
    Ok(s3::Client::new(&shard_config))
}

impl S3 {
    pub async fn new(region: String, bucket: String) -> Result<Self> {
        let client = get_client(region.clone()).await?;
        Ok(S3 { client, bucket })
    }

    pub async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut continuation_token: Option<String> = None;
        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);

            if let Some(token) = &continuation_token {
                request = request.continuation_token(token);
            }

            let resp = request.send().await?;

            if let Some(contents) = resp.contents {
                keys.extend(
                    contents
                        .into_iter()
                        .filter(|obj| obj.size().is_some_and(|s| s > 0))
                        .filter_map(|obj| obj.key().map(|s| s.to_string())),
                )
            }
            match resp.is_truncated {
                Some(true) => {
                    continuation_token = resp.next_continuation_token.map(|s| s.to_string());
                }
                _ => break,
            }
        }
        Ok(keys)
    }
}


