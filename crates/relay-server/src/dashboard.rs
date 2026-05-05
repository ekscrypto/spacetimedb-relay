// SPDX-License-Identifier: MIT

//! Built-in dashboard. `/dashboard` returns flat-ASCII HTML that polls
//! `/dashboard.json` every 2s and re-renders client-side.

use axum::extract::State;
use axum::response::{Html, IntoResponse, Json};

use crate::metrics::MetricsSnapshot;
use crate::AppState;

pub async fn data(State(state): State<AppState>) -> impl IntoResponse {
    let snapshot: MetricsSnapshot = state.metrics.snapshot();
    Json(snapshot)
}

pub async fn page() -> Html<&'static str> {
    Html(PAGE)
}

const PAGE: &str = include_str!("dashboard.html");
