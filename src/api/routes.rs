use axum::{
    extract::Request,
    http::{header, Method, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};

use super::AppState;
use crate::api::handlers::{get_config, get_status, health_check, start_service, stop_service, get_logs, clear_logs};
use crate::api::sse::stream_updates;

#[cfg(feature = "web")]
use super::serve_embedded_files;

/// 写操作（POST）携带 Origin 时要求其 authority 与 Host 一致，拦截跨站伪造请求；
/// 非 POST、无 Origin 的同源/直连请求放行。该校验不是身份认证，仅是控制面的一道来源约束。
async fn origin_guard(req: Request, next: Next) -> Response {
    if req.method() == Method::POST
        && let Some(origin) = req.headers().get(header::ORIGIN).and_then(|v| v.to_str().ok())
    {
        let origin_matches_host = req.headers().get(header::HOST).and_then(|v| v.to_str().ok())
            .map(|host| origin.rsplit_once("://").map(|(_, authority)| authority == host).unwrap_or(false))
            .unwrap_or(false);
        if !origin_matches_host {
            return (StatusCode::FORBIDDEN, "cross-origin request rejected").into_response();
        }
    }
    next.run(req).await
}

pub fn create_router(state: AppState) -> Router {
    let router = Router::new()
        .route("/api/status", get(get_status))
        .route("/api/config", get(get_config))
        .route("/api/start", post(start_service))
        .route("/api/stop", post(stop_service))
        .route("/api/health", get(health_check))
        .route("/api/stream", get(stream_updates))
        .route("/api/logs", get(get_logs))
        .route("/api/logs/clear", post(clear_logs))
        .layer(middleware::from_fn(origin_guard))
        .with_state(state);

    #[cfg(feature = "web")]
    let router = router.fallback(serve_embedded_files);

    router
}