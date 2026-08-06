use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    Json,
};
use serde::Deserialize;

use crate::{
    error::AppError,
    handlers::{check_user_access, extract_api_key, validate_api_key},
    services::task_manager::InAppNotification,
    AppState,
};

#[derive(Deserialize, utoipa::IntoParams)]
pub struct NotificationsQuery {
    pub user_id: i32,
}

#[derive(Deserialize, utoipa::IntoParams)]
pub struct ClearNotificationsQuery {
    pub user_id: i32,
    /// When true, only already-read notifications are removed.
    #[serde(default)]
    pub read_only: bool,
}

async fn authorize(state: &AppState, headers: &HeaderMap, user_id: i32) -> Result<(), AppError> {
    let api_key = extract_api_key(headers)?;
    if !validate_api_key(state, &api_key).await? {
        return Err(AppError::unauthorized("Invalid API key"));
    }
    if !check_user_access(state, &api_key, user_id).await? {
        return Err(AppError::forbidden("You can only manage your own notifications!"));
    }
    Ok(())
}

/// List the user's live durable notifications (messages/alerts). Expired ones are
/// dropped server-side. Powers the Activity center on load and as a WS fallback.
#[utoipa::path(
    get,
    path = "/notifications",
    tag = "notifications",
    params(NotificationsQuery),
    security(("api_key" = [])),
    responses(
        (status = 200, description = "The user's notifications", body = Vec<InAppNotification>),
        (status = 401, description = "Invalid or missing API key"),
        (status = 403, description = "Not authorized for this user"),
    ),
)]
pub async fn list_notifications(
    Query(query): Query<NotificationsQuery>,
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Vec<InAppNotification>>, AppError> {
    authorize(&state, &headers, query.user_id).await?;
    let notifs = state.task_manager.list_notifications(query.user_id).await?;
    Ok(Json(notifs))
}

/// Mark one notification read.
#[utoipa::path(
    post,
    path = "/notifications/{id}/read",
    tag = "notifications",
    params(("id" = String, Path), NotificationsQuery),
    security(("api_key" = [])),
    responses(
        (status = 200, description = "Marked read"),
        (status = 401, description = "Invalid or missing API key"),
        (status = 403, description = "Not authorized for this user"),
    ),
)]
pub async fn mark_notification_read(
    Path(id): Path<String>,
    Query(query): Query<NotificationsQuery>,
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, AppError> {
    authorize(&state, &headers, query.user_id).await?;
    state.task_manager.mark_notification_read(query.user_id, &id).await?;
    Ok(Json(serde_json::json!({ "status": "ok" })))
}

/// Dismiss (delete) one notification.
#[utoipa::path(
    post,
    path = "/notifications/{id}/dismiss",
    tag = "notifications",
    params(("id" = String, Path), NotificationsQuery),
    security(("api_key" = [])),
    responses(
        (status = 200, description = "Dismissed"),
        (status = 401, description = "Invalid or missing API key"),
        (status = 403, description = "Not authorized for this user"),
    ),
)]
pub async fn dismiss_notification(
    Path(id): Path<String>,
    Query(query): Query<NotificationsQuery>,
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, AppError> {
    authorize(&state, &headers, query.user_id).await?;
    state.task_manager.dismiss_notification(query.user_id, &id).await?;
    Ok(Json(serde_json::json!({ "status": "ok" })))
}

/// Clear all (or all read) notifications for the user.
#[utoipa::path(
    post,
    path = "/notifications/clear",
    tag = "notifications",
    params(ClearNotificationsQuery),
    security(("api_key" = [])),
    responses(
        (status = 200, description = "Cleared"),
        (status = 401, description = "Invalid or missing API key"),
        (status = 403, description = "Not authorized for this user"),
    ),
)]
pub async fn clear_notifications(
    Query(query): Query<ClearNotificationsQuery>,
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, AppError> {
    authorize(&state, &headers, query.user_id).await?;
    state
        .task_manager
        .clear_notifications(query.user_id, query.read_only)
        .await?;
    Ok(Json(serde_json::json!({ "status": "ok" })))
}
