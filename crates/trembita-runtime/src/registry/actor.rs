use super::errors::{ConfigCodecError, MessageDecodeError, MigrationError};
use super::reply::WireReplyPort;

/// A user-defined actor: state built from a `Config`, driven by a serial
/// mailbox of `Message`s.
pub trait UserActor: Send + Sized + 'static {
    /// Immutable configuration used to construct the actor's initial state.
    type Config: Send + 'static;
    /// The message type this actor accepts.
    type Message: Send + 'static;
    /// Error returned by [`start`](UserActor::start) / [`handle`](UserActor::handle).
    type Error: std::error::Error + Send + Sync + 'static;

    /// Whether instances carry migratable state that should be snapshotted and
    /// transferred when their node leaves (cross-node-actors). Defaults to `false`
    /// (stateless — the supervisor simply respawns the same count elsewhere).
    const MIGRATABLE: bool = false;

    /// Build the actor's initial state from its configuration. Called once, on
    /// the actor's task, before any message is handled.
    ///
    /// # Errors
    /// Returns [`Self::Error`] if the actor cannot be initialized; the spawn
    /// fails and no task is left running.
    fn start(config: Self::Config) -> Result<Self, Self::Error>;

    /// Handle a single message. Returned errors are surfaced to the actor's
    /// task (currently logged as a dropped result); the actor keeps running.
    ///
    /// The returned future must be `Send` (it runs on a multi-threaded
    /// executor). Implement it with a plain `async fn handle`.
    ///
    /// # Errors
    /// Returns [`Self::Error`] if the message could not be processed.
    fn handle(
        &mut self,
        msg: Self::Message,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send;

    /// Encode this actor's `Config` for a remote spawn (E9, cross-node-actors
    /// `/actor/spawn`). The default makes the actor **local-spawn-only**:
    /// `spawn_remote` / `scale_cluster` fail with
    /// [`ConfigCodecError::NotSpawnable`]. Override it (typically with
    /// `trembita_proto::encode`) to allow the control plane to place the actor on
    /// other nodes.
    ///
    /// # Errors
    /// Returns [`ConfigCodecError`] if the actor is not remotely spawnable or
    /// the config cannot be encoded.
    fn encode_config(_config: &Self::Config) -> Result<Vec<u8>, ConfigCodecError> {
        Err(ConfigCodecError::NotSpawnable)
    }

    /// Decode a `Config` shipped for a remote spawn (E9). Must round-trip with
    /// [`encode_config`](UserActor::encode_config); the default rejects.
    ///
    /// # Errors
    /// Returns [`ConfigCodecError`] if the actor is not remotely spawnable or
    /// the bytes cannot be decoded.
    fn decode_config(_bytes: &[u8]) -> Result<Self::Config, ConfigCodecError> {
        Err(ConfigCodecError::NotSpawnable)
    }

    /// Capture this actor's migratable state as a byte snapshot, so it can be
    /// transferred to a replacement on another node when this node leaves
    /// (E12, [cross-node-actors](../../../docs/decisions/cross-node-actors.md)). The
    /// default is a **stateless** actor: an empty snapshot, meaning the
    /// supervisor simply respawns a fresh instance elsewhere. Stateful actors
    /// (`MIGRATABLE = true`) override this together with
    /// [`restore_migration`](UserActor::restore_migration).
    ///
    /// Runs on the actor's own task, ordered after any already-queued messages,
    /// so the snapshot reflects everything handled before the migration began.
    ///
    /// # Errors
    /// Returns [`MigrationError`] if the state cannot be captured.
    fn migration_snapshot(&self) -> Result<Vec<u8>, MigrationError> {
        Ok(Vec::new())
    }

    /// Restore migratable state from a snapshot produced by
    /// [`migration_snapshot`](UserActor::migration_snapshot) on the departing
    /// node (E12). Runs once, on the new instance's task, before it handles any
    /// message. The default ignores the (empty) snapshot.
    ///
    /// # Errors
    /// Returns [`MigrationError`] if the snapshot cannot be applied.
    fn restore_migration(&mut self, _snapshot: &[u8]) -> Result<(), MigrationError> {
        Ok(())
    }

    /// Decode a cross-node wire payload into a message for remote delivery
    /// (E8, cross-node-actors `/actor/deliver`). The default leaves the actor
    /// **local-only**: a remote `cast` to it fails with
    /// [`MessageDecodeError::NotAddressable`]. Override it (typically with
    /// `trembita_proto::decode`) to accept messages sent from other nodes.
    ///
    /// This decodes the fire-and-forget half; the request/reply half is
    /// [`decode_ask`](UserActor::decode_ask).
    ///
    /// # Errors
    /// Returns [`MessageDecodeError`] if the actor is not remotely addressable
    /// or the payload cannot be decoded into a [`Message`](UserActor::Message).
    fn decode_message(_payload: &[u8]) -> Result<Self::Message, MessageDecodeError> {
        Err(MessageDecodeError::NotAddressable)
    }

    /// Decode a cross-node **ask** into a message carrying a reply port (E8,
    /// cross-node-actors, cluster-routing `/actor/deliver` with `reply_expected`). Build your ask
    /// message variant, converting the supplied [`WireReplyPort`] into the typed
    /// [`RpcReplyPort<R>`](super::RpcReplyPort) it expects via
    /// [`WireReplyPort::reply_port`]; whatever the handler replies is
    /// `postcard`-encoded back to the caller. The default rejects remote asks
    /// with [`MessageDecodeError::NotAddressable`].
    ///
    /// ```ignore
    /// fn decode_ask(payload: &[u8], reply: WireReplyPort)
    ///     -> Result<Self::Message, MessageDecodeError>
    /// {
    ///     let req: Req = trembita_proto::decode(payload)
    ///         .map_err(|e| MessageDecodeError::Decode(e.to_string()))?;
    ///     Ok(Msg::Ask { req, reply: reply.reply_port::<Resp>() })
    /// }
    /// ```
    ///
    /// # Errors
    /// Returns [`MessageDecodeError`] if the actor does not support remote asks
    /// or the payload cannot be decoded.
    fn decode_ask(
        _payload: &[u8],
        _reply: WireReplyPort,
    ) -> Result<Self::Message, MessageDecodeError> {
        Err(MessageDecodeError::NotAddressable)
    }

    /// Called once after the mailbox closes (stop or scale-in), for cleanup.
    fn stopped(&mut self) -> impl std::future::Future<Output = ()> + Send {
        async {}
    }
}
