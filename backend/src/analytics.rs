use crate::api_error::ApiError;
use crate::app::AppState;
use crate::auth::AuthenticatedAdmin;
use crate::service::{
    AdminService, ClaimMetricsService, LendingMonitoringService, PlanStatisticsService,
    RevenueMetricsService, UserMetricsService, YieldReportFilters, YieldReportingService,
};
use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Deserialize)]
pub struct RevenueRangeQuery {
    #[serde(default = "default_range")]
    pub range: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct YieldSummaryQuery {
    pub asset_code: Option<String>,
    pub user_id: Option<String>,
    pub plan_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct YieldHistoryQuery {
    #[serde(default = "default_range")]
    pub range: String,
    pub asset_code: Option<String>,
    pub user_id: Option<String>,
    pub plan_id: Option<String>,
}

fn default_range() -> String {
    "monthly".to_string()
}

fn parse_uuid_filter(
    raw_value: Option<String>,
    field_name: &str,
) -> Result<Option<Uuid>, ApiError> {
    raw_value
        .map(|value| {
            Uuid::parse_str(value.trim())
                .map_err(|error| ApiError::BadRequest(format!("Invalid {field_name}: {error}")))
        })
        .transpose()
}

fn build_yield_filters(
    asset_code: Option<String>,
    user_id: Option<String>,
    plan_id: Option<String>,
) -> Result<YieldReportFilters, ApiError> {
    Ok(YieldReportFilters {
        asset_code: asset_code
            .map(|value| value.trim().to_uppercase())
            .filter(|value| !value.is_empty()),
        user_id: parse_uuid_filter(user_id, "userId")?,
        plan_id: parse_uuid_filter(plan_id, "planId")?,
    })
}

/// GET /api/admin/analytics/overview
/// Returns high-level protocol metrics: total revenue, plans, claims, users.
async fn get_overview(
    State(state): State<Arc<AppState>>,
    AuthenticatedAdmin(_admin): AuthenticatedAdmin,
) -> Result<Json<Value>, ApiError> {
    let metrics = AdminService::get_metrics_overview(&state.db).await?;
    Ok(Json(json!({
        "status": "success",
        "data": {
            "totalRevenue": metrics.total_revenue,
            "totalPlans": metrics.total_plans,
            "totalClaims": metrics.total_claims,
            "activePlans": metrics.active_plans,
            "totalUsers": metrics.total_users,
        }
    })))
}

/// GET /api/admin/analytics/users
/// Returns user growth metrics: total, new (7d/30d), active.
async fn get_user_metrics(
    State(state): State<Arc<AppState>>,
    AuthenticatedAdmin(_admin): AuthenticatedAdmin,
) -> Result<Json<Value>, ApiError> {
    let metrics = UserMetricsService::get_user_growth_metrics(&state.db).await?;
    Ok(Json(json!({
        "status": "success",
        "data": metrics
    })))
}

/// GET /api/admin/analytics/plans
/// Returns plan statistics broken down by status.
async fn get_plan_metrics(
    State(state): State<Arc<AppState>>,
    AuthenticatedAdmin(_admin): AuthenticatedAdmin,
) -> Result<Json<Value>, ApiError> {
    let stats = PlanStatisticsService::get_plan_statistics(&state.db).await?;
    Ok(Json(json!({
        "status": "success",
        "data": stats
    })))
}

/// GET /api/admin/analytics/claims
/// Returns claim processing statistics.
async fn get_claim_metrics(
    State(state): State<Arc<AppState>>,
    AuthenticatedAdmin(_admin): AuthenticatedAdmin,
) -> Result<Json<Value>, ApiError> {
    let stats = ClaimMetricsService::get_claim_statistics(&state.db).await?;
    Ok(Json(json!({
        "status": "success",
        "data": stats
    })))
}

/// GET /api/admin/analytics/revenue?range=daily|weekly|monthly
/// Returns time-series revenue breakdown. Defaults to monthly.
async fn get_revenue_metrics(
    State(state): State<Arc<AppState>>,
    AuthenticatedAdmin(_admin): AuthenticatedAdmin,
    Query(params): Query<RevenueRangeQuery>,
) -> Result<Json<Value>, ApiError> {
    let breakdown = RevenueMetricsService::get_revenue_breakdown(&state.db, &params.range).await?;
    Ok(Json(json!({
        "status": "success",
        "data": breakdown
    })))
}

/// GET /api/admin/analytics/lending
/// Returns DeFi lending pool metrics: TVL, utilization rate, active loans.
async fn get_lending_metrics(
    State(state): State<Arc<AppState>>,
    AuthenticatedAdmin(_admin): AuthenticatedAdmin,
) -> Result<Json<Value>, ApiError> {
    let metrics = LendingMonitoringService::get_lending_metrics(&state.db).await?;
    Ok(Json(json!({
        "status": "success",
        "data": metrics
    })))
}

/// GET /api/admin/analytics/yield
/// Returns vault yield and APY aggregated by asset-level vault.
async fn get_yield_summary(
    State(state): State<Arc<AppState>>,
    AuthenticatedAdmin(_admin): AuthenticatedAdmin,
    Query(params): Query<YieldSummaryQuery>,
) -> Result<Json<Value>, ApiError> {
    let filters = build_yield_filters(params.asset_code, params.user_id, params.plan_id)?;
    let summary =
        YieldReportingService::get_yield_summary(&state.db, filters, state.yield_service.as_ref())
            .await?;

    Ok(Json(json!({
        "status": "success",
        "data": summary
    })))
}

/// GET /api/admin/analytics/yield/history?range=daily|weekly|monthly
/// Returns earnings history from realized interest accruals.
async fn get_earnings_history(
    State(state): State<Arc<AppState>>,
    AuthenticatedAdmin(_admin): AuthenticatedAdmin,
    Query(params): Query<YieldHistoryQuery>,
) -> Result<Json<Value>, ApiError> {
    let filters = build_yield_filters(params.asset_code, params.user_id, params.plan_id)?;
    let history =
        YieldReportingService::get_earnings_history(&state.db, filters, &params.range).await?;

    Ok(Json(json!({
        "status": "success",
        "data": history
    })))
}

/// Aggregated dashboard endpoint — all metrics in one request.
/// GET /api/admin/analytics/dashboard
async fn get_dashboard(
    State(state): State<Arc<AppState>>,
    AuthenticatedAdmin(_admin): AuthenticatedAdmin,
) -> Result<Json<Value>, ApiError> {
    let (overview, users, plans, claims, lending) = tokio::try_join!(
        AdminService::get_metrics_overview(&state.db),
        UserMetricsService::get_user_growth_metrics(&state.db),
        PlanStatisticsService::get_plan_statistics(&state.db),
        ClaimMetricsService::get_claim_statistics(&state.db),
        LendingMonitoringService::get_lending_metrics(&state.db),
    )?;

    Ok(Json(json!({
        "status": "success",
        "data": {
            "overview": {
                "totalRevenue": overview.total_revenue,
                "totalPlans": overview.total_plans,
                "totalClaims": overview.total_claims,
                "activePlans": overview.active_plans,
                "totalUsers": overview.total_users,
            },
            "users": users,
            "plans": plans,
            "claims": claims,
            "lending": lending,
        }
    })))
}

pub fn analytics_router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/admin/analytics/dashboard", get(get_dashboard))
        .route("/api/admin/analytics/overview", get(get_overview))
        .route("/api/admin/analytics/users", get(get_user_metrics))
        .route("/api/admin/analytics/plans", get(get_plan_metrics))
        .route("/api/admin/analytics/claims", get(get_claim_metrics))
        .route("/api/admin/analytics/revenue", get(get_revenue_metrics))
        .route("/api/admin/analytics/lending", get(get_lending_metrics))
        .route("/api/admin/analytics/yield", get(get_yield_summary))
        .route(
            "/api/admin/analytics/yield/history",
            get(get_earnings_history),
        )
}
