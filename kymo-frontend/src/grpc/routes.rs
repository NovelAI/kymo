//! Typed frontend view of the shared frozen WebSocket route manifest.
//!
//! A wrapper cannot accidentally pair a path with the wrong protobuf request
//! or response: the route value carries both types into `WsClient`.

use std::marker::PhantomData;

use super::{proto, ws_rpc};

#[derive(Clone, Copy)]
pub struct Rpc<Req, Resp> {
    pub path: &'static str,
    pub marker: PhantomData<fn(Req) -> Resp>,
}

impl<Req, Resp> Rpc<Req, Resp> {
    const fn new(path: &'static str) -> Self {
        Self {
            path,
            marker: PhantomData,
        }
    }
}

macro_rules! declare_routes {
    ($(($name:ident, $request:ident, $response:ident, $method:ident)),* $(,)?) => {
        $(
            pub const $name: Rpc<proto::$request, proto::$response> =
                Rpc::new(ws_rpc::$name);
        )*
    };
}

ws_rpc::browser_rpc_routes!(declare_routes);
