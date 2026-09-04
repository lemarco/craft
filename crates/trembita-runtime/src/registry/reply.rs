use std::time::Duration;

use tokio::sync::oneshot;

/// A one-shot reply channel embedded in a message to implement "ask"
/// (request/response). The handler calls [`reply`](RpcReplyPort::reply) with
/// the response; the caller awaits it via [`ActorRef::ask`] / [`PoolRef::ask`].
///
/// A port is backed either by an **in-process** channel (local `ask`) or, for a
/// cross-node `ask` arriving over `/actor/deliver`, by a **wire** channel that
/// `postcard`-encodes the reply and returns it in the [`DeliverAck`]
/// (cross-node-actors, cluster-routing). A [`WireReplyPort`] is turned into a typed one via
/// [`WireReplyPort::reply_port`] inside [`UserActor::decode_ask`].
///
/// [`DeliverAck`]: trembita_proto::DeliverAck
pub struct RpcReplyPort<R> {
    sink: Reply<R>,
}

/// A cross-node reply: either the `postcard`-encoded bytes, or the reason the
/// handler's reply value could not be serialized. Carrying the failure lets the
/// serve side report a real encode error instead of a silent "no reply".
pub(crate) type WireReply = Result<Vec<u8>, String>;

/// Where an [`RpcReplyPort`]'s reply is delivered.
enum Reply<R> {
    /// In-process caller (local `ask`).
    Local(oneshot::Sender<R>),
    /// Cross-node caller: serialize `R` and hand the result back over the wire.
    Wire {
        tx: oneshot::Sender<WireReply>,
        encode: fn(&R) -> Result<Vec<u8>, trembita_proto::CodecError>,
    },
}

impl<R> RpcReplyPort<R> {
    /// A port backed by an in-process one-shot channel (local `ask`).
    pub(super) fn local(tx: oneshot::Sender<R>) -> Self {
        Self {
            sink: Reply::Local(tx),
        }
    }

    /// Send the response back to the asker. Returns `Err(value)` if the caller
    /// already gave up (dropped the pending `ask`) or, for a cross-node reply,
    /// if the value could not be encoded.
    ///
    /// A cross-node encode failure is *also* signalled to the receiving node
    /// (over the wire channel) so the asker sees a real encode error rather than
    /// a reply that silently never arrives.
    ///
    /// # Errors
    /// Returns the unsent `value` if the receiving `ask` was dropped or the
    /// reply could not be serialized.
    pub fn reply(self, value: R) -> Result<(), R> {
        match self.sink {
            Reply::Local(tx) => tx.send(value),
            Reply::Wire { tx, encode } => match encode(&value) {
                Ok(bytes) => tx.send(Ok(bytes)).map_err(|_| value),
                Err(e) => {
                    // Surface the encode failure to the serve side; the handler
                    // still gets `value` back as undelivered.
                    let _ = tx.send(Err(e.to_string()));
                    Err(value)
                }
            },
        }
    }
}

/// An opaque reply channel for a cross-node `ask`, handed to
/// [`UserActor::decode_ask`]. Convert it to the typed [`RpcReplyPort`] your
/// message variant expects with [`reply_port`](WireReplyPort::reply_port); the
/// reply is `postcard`-encoded and returned in the `DeliverAck`.
pub struct WireReplyPort {
    tx: oneshot::Sender<WireReply>,
}

impl WireReplyPort {
    /// Adapt this wire channel into a typed [`RpcReplyPort<R>`] to embed in a
    /// message. `R` must be serializable so the reply can cross the node
    /// boundary.
    #[must_use]
    pub fn reply_port<R: serde::Serialize>(self) -> RpcReplyPort<R> {
        RpcReplyPort {
            sink: Reply::Wire {
                tx: self.tx,
                encode: trembita_proto::encode::<R>,
            },
        }
    }
}
