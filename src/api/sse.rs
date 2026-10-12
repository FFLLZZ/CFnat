use std::convert::Infallible;
use std::time::Duration;

use axum::{
    extract::State,
    response::{sse::Event, Sse},
};
use serde::Serialize;
use tokio_stream::{Stream, StreamExt};

use super::AppState;
use crate::core::{ServiceConfig, StatusInfo};

#[derive(Serialize)]
struct StreamUpdate {
    status: StatusInfo,
    config: ServiceConfig,
}

pub async fn stream_updates(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    // 挂起恢复后不补跑历史 tick，只推送一次当前状态
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let stream = tokio_stream::wrappers::IntervalStream::new(interval)
        .filter_map(move |_| {
            let update = StreamUpdate {
                status: state.service.build_full_status(),
                config: state.service.get_config(),
            };

            // 序列化失败时跳过本次推送，避免 panic 中断 SSE 流
            Event::default().json_data(update).ok().map(Ok)
        });

    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(10))
    )
}