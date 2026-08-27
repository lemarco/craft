//! `craft-macros` — derive/attribute macros for the craft framework (backlog
//! Track D).
//!
//! The only public entry point is the [`macro@remote_actor`] attribute, which
//! fills in the boilerplate `postcard` wire codecs on a `UserActor`
//! implementation so an actor can be spawned on and messaged from remote nodes
//! (cross-node-actors). The state-machine "derive" of state-machine is instead served by serde
//! blanket impls in `craft-core` (see backlog D0/D1), so no `StateMachine`
//! derive is exported.

use proc_macro::TokenStream;
use quote::quote;
use std::collections::HashSet;
use syn::punctuated::Punctuated;
use syn::{Ident, ImplItem, ItemImpl, Token, parse_macro_input};

/// Fill in the `postcard` wire codecs on a `UserActor` `impl` so the actor is
/// remotely spawnable and addressable (cross-node-actors, backlog D2).
///
/// A bare `impl craft_actor::UserActor for MyActor { .. }` is **local-only**:
/// the trait's `encode_config` / `decode_config` / `decode_message` default to
/// rejecting (`NotSpawnable` / `NotAddressable`). Rather than hand-writing the
/// `craft_proto::encode`/`decode` glue, annotate the impl:
///
/// ```ignore
/// #[craft_actor::remote_actor]
/// impl craft_actor::UserActor for Counter {
///     type Config = CounterConfig;   // : Serialize + DeserializeOwned
///     type Message = CounterMessage; // : DeserializeOwned
///     type Error = CounterError;
///     fn start(config: Self::Config) -> Result<Self, Self::Error> { /* .. */ }
///     async fn handle(&mut self, msg: Self::Message) -> Result<(), Self::Error> { /* .. */ }
/// }
/// ```
///
/// The attribute appends serde-backed implementations of any of
/// `encode_config`, `decode_config`, and `decode_message` **not already present**
/// in the block (so you can still override one by hand). Because the generated
/// bodies call `craft_proto::encode`/`decode`, the associated `Config`/`Message`
/// types must implement the matching `serde` traits — otherwise the impl fails
/// to compile, which is exactly the "message serde bounds" check of backlog D2.
///
/// Pass `migratable` to also set `const MIGRATABLE = true` (you still implement
/// `migration_snapshot` / `restore_migration` yourself, as those carry actor
/// state, not a mechanical codec):
///
/// ```ignore
/// #[craft_actor::remote_actor(migratable)]
/// impl craft_actor::UserActor for Session { /* .. */ }
/// ```
#[proc_macro_attribute]
pub fn remote_actor(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr with Punctuated::<Ident, Token![,]>::parse_terminated);
    let mut input = parse_macro_input!(item as ItemImpl);

    let mut migratable = false;
    for arg in &args {
        if arg == "migratable" {
            migratable = true;
        } else {
            return syn::Error::new_spanned(
                arg,
                "unknown `remote_actor` option (expected `migratable`)",
            )
            .to_compile_error()
            .into();
        }
    }

    // The attribute only makes sense on `impl UserActor for T`.
    let is_user_actor = input
        .trait_
        .as_ref()
        .and_then(|(_, path, _)| path.segments.last())
        .is_some_and(|seg| seg.ident == "UserActor");
    if !is_user_actor {
        return syn::Error::new_spanned(
            &input.self_ty,
            "`#[remote_actor]` must be applied to an `impl craft_actor::UserActor for T` block",
        )
        .to_compile_error()
        .into();
    }

    // Names already defined in the block, so we never emit a duplicate item
    // (this lets a user override just one codec by hand).
    let defined: HashSet<String> = input
        .items
        .iter()
        .filter_map(|it| match it {
            ImplItem::Fn(f) => Some(f.sig.ident.to_string()),
            ImplItem::Const(c) => Some(c.ident.to_string()),
            _ => None,
        })
        .collect();

    if !defined.contains("encode_config") {
        input.items.push(syn::parse_quote! {
            fn encode_config(
                config: &Self::Config,
            ) -> ::core::result::Result<::std::vec::Vec<u8>, ::craft_actor::ConfigCodecError> {
                ::craft_actor::craft_proto::encode(config)
                    .map_err(|e| ::craft_actor::ConfigCodecError::Codec(
                        ::std::string::ToString::to_string(&e),
                    ))
            }
        });
    }
    if !defined.contains("decode_config") {
        input.items.push(syn::parse_quote! {
            fn decode_config(
                bytes: &[u8],
            ) -> ::core::result::Result<Self::Config, ::craft_actor::ConfigCodecError> {
                ::craft_actor::craft_proto::decode(bytes)
                    .map_err(|e| ::craft_actor::ConfigCodecError::Codec(
                        ::std::string::ToString::to_string(&e),
                    ))
            }
        });
    }
    if !defined.contains("decode_message") {
        input.items.push(syn::parse_quote! {
            fn decode_message(
                payload: &[u8],
            ) -> ::core::result::Result<Self::Message, ::craft_actor::MessageDecodeError> {
                ::craft_actor::craft_proto::decode(payload)
                    .map_err(|e| ::craft_actor::MessageDecodeError::Decode(
                        ::std::string::ToString::to_string(&e),
                    ))
            }
        });
    }
    if migratable && !defined.contains("MIGRATABLE") {
        input.items.push(syn::parse_quote! {
            const MIGRATABLE: bool = true;
        });
    }

    quote!(#input).into()
}
