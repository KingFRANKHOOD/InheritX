use crate::api_error::ApiError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{error, info, warn};
use uuid::Uuid;

const EXTERNAL_MAX_DEVIATION_BPS: i64 = 500; // 5%

#[derive(Debug, Clone)]
struct ExternalQuote {
    source: &'static str,
    price: Decimal,
}

/// Price feed source types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PriceFeedSource {
    Pyth,
    Chainlink,
    Custom,
}

impl PriceFeedSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            PriceFeedSource::Pyth => "pyth",
            PriceFeedSource::Chainlink => "chainlink",
            PriceFeedSource::Custom => "custom",
        }
    }
}

/// Asset price data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetPrice {
    pub asset_code: String,
    pub price: Decimal,
    pub timestamp: DateTime<Utc>,
    pub source: String,
}

/// Price feed configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceFeedConfig {
    pub id: Uuid,
    pub asset_code: String,
    pub source: String,
    pub feed_id: String,
    pub is_active: bool,
    pub last_updated: Option<DateTime<Utc>>,
}

/// Collateral valuation data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollateralValuation {
    pub plan_id: Uuid,
    pub asset_code: String,
    pub amount: Decimal,
    pub current_price: Decimal,
    pub valuation_usd: Decimal,
    pub collateral_ratio: Decimal,
    pub last_updated: DateTime<Utc>,
}

/// Price feed service trait
#[async_trait]
pub trait PriceFeedService: Send + Sync {
    /// Get current price for an asset
    async fn get_price(&self, asset_code: &str) -> Result<AssetPrice, ApiError>;

    /// Get price history for an asset
    async fn get_price_history(
        &self,
        asset_code: &str,
        limit: i64,
    ) -> Result<Vec<AssetPrice>, ApiError>;

    /// Register a new price feed
    async fn register_feed(
        &self,
        asset_code: &str,
        source: PriceFeedSource,
        feed_id: &str,
    ) -> Result<PriceFeedConfig, ApiError>;

    /// Update price for an asset
    async fn update_price(&self, asset_code: &str, price: Decimal) -> Result<AssetPrice, ApiError>;

    /// Calculate collateral valuation
    async fn calculate_valuation(
        &self,
        asset_code: &str,
        amount: Decimal,
    ) -> Result<CollateralValuation, ApiError>;

    /// Get all active price feeds
    async fn get_active_feeds(&self) -> Result<Vec<PriceFeedConfig>, ApiError>;
}

/// In-memory price cache with database persistence
pub struct DefaultPriceFeedService {
    db: PgPool,
    price_cache: Arc<RwLock<HashMap<String, AssetPrice>>>,
    cache_ttl_secs: u64,
    http_client: Client,
}

impl DefaultPriceFeedService {
    pub fn new(db: PgPool, cache_ttl_secs: u64) -> Self {
        Self {
            db,
            price_cache: Arc::new(RwLock::new(HashMap::new())),
            cache_ttl_secs,
            http_client: Client::builder()
                .timeout(Duration::from_secs(3))
                .build()
                .unwrap_or_else(|_| Client::new()),
        }
    }

    /// Check if cached price is still valid
    fn is_cache_valid(&self, timestamp: DateTime<Utc>) -> bool {
        let age = Utc::now().signed_duration_since(timestamp).num_seconds() as u64;
        age < self.cache_ttl_secs
    }

    fn coin_gecko_id(asset_code: &str) -> Option<&'static str> {
        match asset_code.to_uppercase().as_str() {
            "BTC" => Some("bitcoin"),
            "ETH" => Some("ethereum"),
            "USDC" => Some("usd-coin"),
            "XLM" => Some("stellar"),
            _ => None,
        }
    }

    fn binance_symbol(asset_code: &str) -> Option<&'static str> {
        match asset_code.to_uppercase().as_str() {
            "BTC" => Some("BTCUSDT"),
            "ETH" => Some("ETHUSDT"),
            "USDC" => Some("USDCUSDT"),
            "XLM" => Some("XLMUSDT"),
            _ => None,
        }
    }

    fn prices_within_threshold(a: Decimal, b: Decimal) -> bool {
        let denominator = if a.abs() > b.abs() { a.abs() } else { b.abs() };
        if denominator <= Decimal::ZERO {
            return false;
        }

        let bps = (a - b).abs() * Decimal::from(10_000) / denominator;
        bps <= Decimal::from(EXTERNAL_MAX_DEVIATION_BPS)
    }

    fn select_redundant_price(quotes: &[ExternalQuote]) -> Option<Decimal> {
        match quotes {
            [] => None,
            [single] => Some(single.price),
            [first, second] => {
                if Self::prices_within_threshold(first.price, second.price) {
                    Some((first.price + second.price) / Decimal::from(2))
                } else {
                    warn!(
                        "External price sources deviated beyond threshold; preferring primary source"
                    );
                    Some(first.price)
                }
            }
            _ => {
                let mut prices = quotes.iter().map(|q| q.price).collect::<Vec<_>>();
                prices.sort();
                Some(prices[prices.len() / 2])
            }
        }
    }

    async fn fetch_from_coingecko(&self, asset_code: &str) -> Option<ExternalQuote> {
        let id = Self::coin_gecko_id(asset_code)?;
        let url = format!(
            "https://api.coingecko.com/api/v3/simple/price?ids={}&vs_currencies=usd",
            id
        );

        let response = match self.http_client.get(url).send().await {
            Ok(resp) => resp,
            Err(err) => {
                warn!("CoinGecko fetch failed for {}: {}", asset_code, err);
                return None;
            }
        };

        let payload = match response.json::<Value>().await {
            Ok(value) => value,
            Err(err) => {
                warn!(
                    "CoinGecko response parse failed for {}: {}",
                    asset_code, err
                );
                return None;
            }
        };

        let price = payload
            .get(id)
            .and_then(|entry| entry.get("usd"))
            .and_then(|v| v.as_f64())
            .and_then(Decimal::from_f64_retain);

        price.map(|value| ExternalQuote {
            source: "coingecko",
            price: value,
        })
    }

    async fn fetch_from_binance(&self, asset_code: &str) -> Option<ExternalQuote> {
        let symbol = Self::binance_symbol(asset_code)?;
        let url = format!(
            "https://api.binance.com/api/v3/ticker/price?symbol={}",
            symbol
        );

        let response = match self.http_client.get(url).send().await {
            Ok(resp) => resp,
            Err(err) => {
                warn!("Binance fetch failed for {}: {}", asset_code, err);
                return None;
            }
        };

        let payload = match response.json::<Value>().await {
            Ok(value) => value,
            Err(err) => {
                warn!("Binance response parse failed for {}: {}", asset_code, err);
                return None;
            }
        };

        let price = payload
            .get("price")
            .and_then(Value::as_str)
            .and_then(|v| Decimal::from_str(v).ok());

        price.map(|value| ExternalQuote {
            source: "binance",
            price: value,
        })
    }

    async fn persist_external_price(&self, asset_price: &AssetPrice) {
        if let Err(err) = sqlx::query(
            r#"
            INSERT INTO asset_price_history (asset_code, price, price_timestamp, source)
            VALUES ($1, $2, $3, $4)
            "#,
        )
        .bind(&asset_price.asset_code)
        .bind(asset_price.price.to_string())
        .bind(asset_price.timestamp)
        .bind(&asset_price.source)
        .execute(&self.db)
        .await
        {
            warn!(
                "Failed to persist external price for {}: {}",
                asset_price.asset_code, err
            );
        }

        if let Err(err) =
            sqlx::query("UPDATE price_feeds SET last_updated = $1 WHERE asset_code = $2")
                .bind(asset_price.timestamp)
                .bind(&asset_price.asset_code)
                .execute(&self.db)
                .await
        {
            warn!(
                "Failed to update feed timestamp for {}: {}",
                asset_price.asset_code, err
            );
        }
    }

    async fn fetch_external_price_redundant(&self, asset_code: &str) -> Option<AssetPrice> {
        let mut quotes = Vec::new();

        if let Some(quote) = self.fetch_from_coingecko(asset_code).await {
            quotes.push(quote);
        }

        if let Some(quote) = self.fetch_from_binance(asset_code).await {
            quotes.push(quote);
        }

        if quotes.is_empty() {
            return None;
        }

        let selected_price = Self::select_redundant_price(&quotes)?;
        let source = quotes
            .iter()
            .map(|q| q.source)
            .collect::<Vec<_>>()
            .join("+");
        let now = Utc::now();

        let asset_price = AssetPrice {
            asset_code: asset_code.to_string(),
            price: selected_price,
            timestamp: now,
            source,
        };

        self.persist_external_price(&asset_price).await;

        {
            let mut cache = self.price_cache.write().await;
            cache.insert(asset_code.to_string(), asset_price.clone());
        }

        Some(asset_price)
    }

    /// Initialize default price feeds (USDC)
    pub async fn initialize_defaults(&self) -> Result<(), ApiError> {
        // Check if USDC feed already exists
        let existing = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM price_feeds WHERE asset_code = 'USDC')",
        )
        .fetch_one(&self.db)
        .await
        .map_err(|e| {
            error!("Failed to check existing price feeds: {}", e);
            ApiError::Internal(anyhow::anyhow!("Database error"))
        })?;

        if !existing {
            sqlx::query(
                r#"
                INSERT INTO price_feeds (asset_code, source, feed_id, is_active)
                VALUES ('USDC', 'custom', 'usdc-usd', true)
                "#,
            )
            .execute(&self.db)
            .await
            .map_err(|e| {
                error!("Failed to initialize default price feeds: {}", e);
                ApiError::Internal(anyhow::anyhow!("Database error"))
            })?;

            info!("Initialized default USDC price feed");
        }

        Ok(())
    }
}

#[async_trait]
impl PriceFeedService for DefaultPriceFeedService {
    async fn get_price(&self, asset_code: &str) -> Result<AssetPrice, ApiError> {
        // Check cache first
        {
            let cache = self.price_cache.read().await;
            if let Some(cached_price) = cache.get(asset_code) {
                if self.is_cache_valid(cached_price.timestamp) {
                    return Ok(cached_price.clone());
                }
            }
        }

        // Try redundant external fetch before falling back to database.
        if let Some(external_price) = self.fetch_external_price_redundant(asset_code).await {
            return Ok(external_price);
        }

        // Fall back to last known database value.
        let price_record = sqlx::query_as::<_, (String, String)>(
            r#"
            SELECT price::text, price_timestamp::text
            FROM asset_price_history
            WHERE asset_code = $1
            ORDER BY price_timestamp DESC
            LIMIT 1
            "#,
        )
        .bind(asset_code)
        .fetch_optional(&self.db)
        .await
        .map_err(|e| {
            error!("Failed to fetch price from database: {}", e);
            ApiError::Internal(anyhow::anyhow!("Database error"))
        })?
        .ok_or_else(|| {
            warn!("No price found for asset: {}", asset_code);
            ApiError::NotFound(format!("Price not found for asset: {}", asset_code))
        })?;

        let price = Decimal::from_str(&price_record.0).map_err(|e| {
            error!("Failed to parse price: {}", e);
            ApiError::Internal(anyhow::anyhow!("Invalid price format"))
        })?;

        let timestamp = chrono::DateTime::parse_from_rfc3339(&price_record.1)
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .map_err(|e| {
                error!("Failed to parse timestamp: {}", e);
                ApiError::Internal(anyhow::anyhow!("Invalid timestamp format"))
            })?;

        let asset_price = AssetPrice {
            asset_code: asset_code.to_string(),
            price,
            timestamp,
            source: "custom".to_string(),
        };

        // Update cache
        {
            let mut cache = self.price_cache.write().await;
            cache.insert(asset_code.to_string(), asset_price.clone());
        }

        Ok(asset_price)
    }

    async fn get_price_history(
        &self,
        asset_code: &str,
        limit: i64,
    ) -> Result<Vec<AssetPrice>, ApiError> {
        let records = sqlx::query_as::<_, (String, String, String)>(
            r#"
            SELECT price::text, price_timestamp::text, source
            FROM asset_price_history
            WHERE asset_code = $1
            ORDER BY price_timestamp DESC
            LIMIT $2
            "#,
        )
        .bind(asset_code)
        .bind(limit)
        .fetch_all(&self.db)
        .await
        .map_err(|e| {
            error!("Failed to fetch price history: {}", e);
            ApiError::Internal(anyhow::anyhow!("Database error"))
        })?;

        let mut prices = Vec::new();
        for (price_str, timestamp_str, source) in records {
            let price = Decimal::from_str(&price_str).map_err(|e| {
                error!("Failed to parse price: {}", e);
                ApiError::Internal(anyhow::anyhow!("Invalid price format"))
            })?;

            let timestamp = chrono::DateTime::parse_from_rfc3339(&timestamp_str)
                .map(|dt| dt.with_timezone(&chrono::Utc))
                .map_err(|e| {
                    error!("Failed to parse timestamp: {}", e);
                    ApiError::Internal(anyhow::anyhow!("Invalid timestamp format"))
                })?;

            prices.push(AssetPrice {
                asset_code: asset_code.to_string(),
                price,
                timestamp,
                source,
            });
        }

        Ok(prices)
    }

    async fn register_feed(
        &self,
        asset_code: &str,
        source: PriceFeedSource,
        feed_id: &str,
    ) -> Result<PriceFeedConfig, ApiError> {
        let id = Uuid::new_v4();

        sqlx::query(
            r#"
            INSERT INTO price_feeds (id, asset_code, source, feed_id, is_active)
            VALUES ($1, $2, $3, $4, true)
            ON CONFLICT (asset_code) DO UPDATE
            SET source = $3, feed_id = $4, is_active = true, updated_at = CURRENT_TIMESTAMP
            "#,
        )
        .bind(id)
        .bind(asset_code)
        .bind(source.as_str())
        .bind(feed_id)
        .execute(&self.db)
        .await
        .map_err(|e| {
            error!("Failed to register price feed: {}", e);
            ApiError::Internal(anyhow::anyhow!("Database error"))
        })?;

        info!(
            "Registered price feed for {} from {}",
            asset_code,
            source.as_str()
        );

        Ok(PriceFeedConfig {
            id,
            asset_code: asset_code.to_string(),
            source: source.as_str().to_string(),
            feed_id: feed_id.to_string(),
            is_active: true,
            last_updated: None,
        })
    }

    async fn update_price(&self, asset_code: &str, price: Decimal) -> Result<AssetPrice, ApiError> {
        // Validate price feed exists
        let _feed = sqlx::query_scalar::<_, String>(
            "SELECT source FROM price_feeds WHERE asset_code = $1 AND is_active = true",
        )
        .bind(asset_code)
        .fetch_optional(&self.db)
        .await
        .map_err(|e| {
            error!("Failed to check price feed: {}", e);
            ApiError::Internal(anyhow::anyhow!("Database error"))
        })?
        .ok_or_else(|| {
            ApiError::BadRequest(format!("Price feed not found for asset: {}", asset_code))
        })?;

        let now = Utc::now();

        // Insert price history
        sqlx::query(
            r#"
            INSERT INTO asset_price_history (asset_code, price, price_timestamp, source)
            VALUES ($1, $2, $3, 'custom')
            "#,
        )
        .bind(asset_code)
        .bind(price.to_string())
        .bind(now)
        .execute(&self.db)
        .await
        .map_err(|e| {
            error!("Failed to update price: {}", e);
            ApiError::Internal(anyhow::anyhow!("Database error"))
        })?;

        // Update last_updated in price_feeds
        sqlx::query("UPDATE price_feeds SET last_updated = $1 WHERE asset_code = $2")
            .bind(now)
            .bind(asset_code)
            .execute(&self.db)
            .await
            .map_err(|e| {
                error!("Failed to update feed timestamp: {}", e);
                ApiError::Internal(anyhow::anyhow!("Database error"))
            })?;

        let asset_price = AssetPrice {
            asset_code: asset_code.to_string(),
            price,
            timestamp: now,
            source: "custom".to_string(),
        };

        // Update cache
        {
            let mut cache = self.price_cache.write().await;
            cache.insert(asset_code.to_string(), asset_price.clone());
        }

        info!("Updated price for {}: {}", asset_code, price);

        Ok(asset_price)
    }

    async fn calculate_valuation(
        &self,
        asset_code: &str,
        amount: Decimal,
    ) -> Result<CollateralValuation, ApiError> {
        let asset_price = self.get_price(asset_code).await?;

        let valuation_usd = amount * asset_price.price;
        let collateral_ratio = Decimal::from(100); // 100% for now, can be adjusted

        Ok(CollateralValuation {
            plan_id: Uuid::nil(), // Will be set by caller
            asset_code: asset_code.to_string(),
            amount,
            current_price: asset_price.price,
            valuation_usd,
            collateral_ratio,
            last_updated: asset_price.timestamp,
        })
    }

    async fn get_active_feeds(&self) -> Result<Vec<PriceFeedConfig>, ApiError> {
        let feeds = sqlx::query_as::<_, (Uuid, String, String, String, Option<String>)>(
            r#"
            SELECT id, asset_code, source, feed_id, last_updated::text
            FROM price_feeds
            WHERE is_active = true
            ORDER BY asset_code
            "#,
        )
        .fetch_all(&self.db)
        .await
        .map_err(|e| {
            error!("Failed to fetch active feeds: {}", e);
            ApiError::Internal(anyhow::anyhow!("Database error"))
        })?;

        let mut result = Vec::new();
        for (id, asset_code, source, feed_id, last_updated_str) in feeds {
            let last_updated = last_updated_str.and_then(|ts| {
                chrono::DateTime::parse_from_rfc3339(&ts)
                    .map(|dt| dt.with_timezone(&chrono::Utc))
                    .ok()
            });

            result.push(PriceFeedConfig {
                id,
                asset_code,
                source,
                feed_id,
                is_active: true,
                last_updated,
            });
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::{DefaultPriceFeedService, ExternalQuote};
    use rust_decimal_macros::dec;

    #[test]
    fn picks_average_when_sources_are_close() {
        let quotes = vec![
            ExternalQuote {
                source: "coingecko",
                price: dec!(100),
            },
            ExternalQuote {
                source: "binance",
                price: dec!(102),
            },
        ];

        let selected = DefaultPriceFeedService::select_redundant_price(&quotes);
        assert_eq!(selected, Some(dec!(101)));
    }

    #[test]
    fn prefers_primary_when_sources_deviate_too_much() {
        let quotes = vec![
            ExternalQuote {
                source: "coingecko",
                price: dec!(100),
            },
            ExternalQuote {
                source: "binance",
                price: dec!(120),
            },
        ];

        let selected = DefaultPriceFeedService::select_redundant_price(&quotes);
        assert_eq!(selected, Some(dec!(100)));
    }

    #[test]
    fn supports_known_external_asset_mappings() {
        assert_eq!(
            DefaultPriceFeedService::coin_gecko_id("btc"),
            Some("bitcoin")
        );
        assert_eq!(
            DefaultPriceFeedService::binance_symbol("ETH"),
            Some("ETHUSDT")
        );
        assert_eq!(DefaultPriceFeedService::coin_gecko_id("UNKNOWN"), None);
    }
}
