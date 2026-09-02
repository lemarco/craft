//! `#[consumer("stream")]` and `#[consumer("topic", subscription = "…")]` expansion helpers.

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{GenericArgument, Ident, ItemFn, PathArguments, ReturnType, Type};

pub(crate) fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
            }
        })
        .collect()
}

fn is_byte_slice(ty: &Type) -> bool {
    match ty {
        Type::Reference(r) => {
            matches!(&*r.elem, Type::Slice(s) if matches!(&*s.elem, Type::Path(p) if p.path.is_ident("u8")))
        }
        _ => false,
    }
}

pub(crate) fn result_err_type(ty: &Type) -> Option<Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };
    let seg = type_path.path.segments.last()?;
    if seg.ident != "Result" {
        return None;
    }
    let PathArguments::AngleBracketed(args) = &seg.arguments else {
        return None;
    };
    let err = args.args.iter().nth(1)?;
    match err {
        GenericArgument::Type(t) => Some(t.clone()),
        _ => None,
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn expand_consumer(
    stream: &str,
    subscription: Option<&str>,
    input_fn: &ItemFn,
) -> TokenStream2 {
    let fn_name = &input_fn.sig.ident;

    if input_fn.sig.asyncness.is_none() {
        return syn::Error::new_spanned(
            input_fn.sig.fn_token,
            "`#[consumer]` must be applied to an `async fn`",
        )
        .to_compile_error();
    }

    let Some(first_arg) = input_fn.sig.inputs.first() else {
        return syn::Error::new_spanned(
            &input_fn.sig,
            "`#[consumer]` handler must take `payload: &[u8]` as its first argument",
        )
        .to_compile_error();
    };

    let syn::FnArg::Typed(pat_type) = first_arg else {
        return syn::Error::new_spanned(
            first_arg,
            "`#[consumer]` handler must take `payload: &[u8]` as its first argument",
        )
        .to_compile_error();
    };

    if !is_byte_slice(&pat_type.ty) {
        return syn::Error::new_spanned(
            &pat_type.ty,
            "`#[consumer]` handler first argument must be `&[u8]`",
        )
        .to_compile_error();
    }

    let ReturnType::Type(_, return_ty) = &input_fn.sig.output else {
        return syn::Error::new_spanned(
            &input_fn.sig,
            "`#[consumer]` handler must return `Result<(), E>`",
        )
        .to_compile_error();
    };

    let Some(err_ty) = result_err_type(return_ty) else {
        return syn::Error::new_spanned(
            return_ty,
            "`#[consumer]` handler must return `Result<(), E>`",
        )
        .to_compile_error();
    };

    let consumer_name = Ident::new(
        &format!("{}Consumer", to_pascal_case(&fn_name.to_string())),
        fn_name.span(),
    );

    let wants_ctx = input_fn.sig.inputs.len() > 1;
    let call = if wants_ctx {
        quote! { #fn_name(payload, ctx).await }
    } else {
        quote! { #fn_name(payload).await }
    };
    let ctx_binding = if wants_ctx {
        quote! { ctx }
    } else {
        quote! { _ctx }
    };

    let subscription_const = if let Some(sub) = subscription {
        quote! { Some(#sub) }
    } else {
        quote! { None }
    };

    let (impl_job, impl_topic) = if subscription.is_some() {
        (
            quote! {
                #[allow(clippy::unused_async_trait_impl)]
                async fn handle_job(
                    _payload: &[u8],
                    _ctx: ::crafty::JobContext<'_>,
                ) -> ::core::result::Result<(), Self::Error> {
                    panic!("topic subscriber invoked as queue consumer")
                }
            },
            quote! {
                #[allow(clippy::unused_async_trait_impl)]
                async fn handle_topic(
                    payload: &[u8],
                    #ctx_binding: ::crafty::TopicContext<'_>,
                ) -> ::core::result::Result<(), Self::Error> {
                    #call
                }
            },
        )
    } else {
        (
            quote! {
                #[allow(clippy::unused_async_trait_impl)]
                async fn handle_job(
                    payload: &[u8],
                    #ctx_binding: ::crafty::JobContext<'_>,
                ) -> ::core::result::Result<(), Self::Error> {
                    #call
                }
            },
            quote! {
                #[allow(clippy::unused_async_trait_impl)]
                async fn handle_topic(
                    _payload: &[u8],
                    _ctx: ::crafty::TopicContext<'_>,
                ) -> ::core::result::Result<(), Self::Error> {
                    panic!("queue consumer invoked as topic subscriber")
                }
            },
        )
    };

    quote! {
        #input_fn

        #[doc(hidden)]
        #[allow(non_camel_case_types, missing_docs)]
        #[derive(Clone, Copy)]
        pub struct #consumer_name;

        impl ::crafty::JobConsumer for #consumer_name {
            const STREAM: &'static str = #stream;
            const SUBSCRIPTION: Option<&'static str> = #subscription_const;
            type Error = #err_ty;

            #impl_job
            #impl_topic
        }
    }
}
