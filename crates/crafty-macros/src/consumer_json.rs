//! `#[consumer_json("stream", Type)]` expansion helpers.

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{Ident, ItemFn, Path, ReturnType};

use crate::consumer::{result_err_type, to_pascal_case};

pub(crate) fn expand_consumer_json(
    stream: &str,
    payload_ty: &Path,
    input_fn: &ItemFn,
) -> TokenStream2 {
    let fn_name = &input_fn.sig.ident;

    if input_fn.sig.asyncness.is_none() {
        return syn::Error::new_spanned(
            input_fn.sig.fn_token,
            "`#[consumer_json]` must be applied to an `async fn`",
        )
        .to_compile_error();
    }

    let ReturnType::Type(_, return_ty) = &input_fn.sig.output else {
        return syn::Error::new_spanned(
            &input_fn.sig,
            "`#[consumer_json]` handler must return `Result<(), E>`",
        )
        .to_compile_error();
    };

    let Some(err_ty) = result_err_type(return_ty) else {
        return syn::Error::new_spanned(
            return_ty,
            "`#[consumer_json]` handler must return `Result<(), E>`",
        )
        .to_compile_error();
    };

    let wants_ctx = input_fn.sig.inputs.len() > 1;
    let call = if wants_ctx {
        quote! { #fn_name(job, ctx).await }
    } else {
        quote! { #fn_name(job).await }
    };
    let ctx_binding = if wants_ctx {
        quote! { ctx }
    } else {
        quote! { _ctx }
    };

    let consumer_name = Ident::new(
        &format!("{}Consumer", to_pascal_case(&fn_name.to_string())),
        fn_name.span(),
    );
    let payload_ty = payload_ty.clone();

    quote! {
        #input_fn

        #[doc(hidden)]
        #[allow(non_camel_case_types, missing_docs)]
        #[derive(Clone, Copy)]
        pub struct #consumer_name;

        impl ::crafty::JobConsumer for #consumer_name {
            const STREAM: &'static str = #stream;
            const SUBSCRIPTION: Option<&'static str> = None;
            type Error = #err_ty;

            #[allow(clippy::unused_async_trait_impl)]
            async fn handle_job(
                payload: &[u8],
                #ctx_binding: ::crafty::JobContext<'_>,
            ) -> ::core::result::Result<(), Self::Error> {
                let job: #payload_ty = ::serde_json::from_slice(payload).map_err(|e| {
                    let msg = format!("invalid job json: {e}");
                    <#err_ty as ::core::convert::From<::std::string::String>>::from(msg)
                })?;
                #call
            }

            #[allow(clippy::unused_async_trait_impl)]
            async fn handle_topic(
                _payload: &[u8],
                _ctx: ::crafty::TopicContext<'_>,
            ) -> ::core::result::Result<(), Self::Error> {
                panic!("consumer_json does not support topic subscriptions")
            }
        }
    }
}
