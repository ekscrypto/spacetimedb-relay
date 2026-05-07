// Stub metrics. SDK calls `.with_label_values(&self.db_name)` where db_name
// is `Box<str>`, so the arg type is `&Box<str>` (not &[&str]); accept anything.
pub struct WebsocketReceived;
impl WebsocketReceived {
    pub fn with_label_values<L>(&self, _: L) -> Counter { Counter }
}
pub struct WebsocketReceivedMsgSize;
impl WebsocketReceivedMsgSize {
    pub fn with_label_values<L>(&self, _: L) -> Histogram { Histogram }
}
pub struct Counter;
impl Counter { pub fn inc(&self) {} }
pub struct Histogram;
impl Histogram { pub fn observe(&self, _: f64) {} }
pub struct ClientMetrics {
    pub websocket_received: WebsocketReceived,
    pub websocket_received_msg_size: WebsocketReceivedMsgSize,
}
pub static CLIENT_METRICS: ClientMetrics = ClientMetrics {
    websocket_received: WebsocketReceived,
    websocket_received_msg_size: WebsocketReceivedMsgSize,
};
