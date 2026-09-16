//! `#[cap_handler(...)]` — `CapRequest` from handler `Result<Reply, CapError>`.

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{
    FnArg, GenericArgument, Ident, ItemFn, LitStr, PatType, PathArguments, ReturnType, Token, Type,
};

use super::cap_request::ident_to_op;

pub(crate) struct CapHandlerArgs {
    group: LitStr,
    op: Option<LitStr>,
    key: Option<LitStr>,
}

impl Parse for CapHandlerArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut group = None;
        let mut op = None;
        let mut key = None;

        while !input.is_empty() {
            let name: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            if name == "group" {
                group = Some(input.parse()?);
            } else if name == "op" {
                op = Some(input.parse()?);
            } else if name == "key" {
                key = Some(input.parse()?);
            } else {
                return Err(syn::Error::new_spanned(
                    name,
                    "unknown option (expected `group`, `op`, `key`)",
                ));
            }
            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }

        Ok(Self {
            group: group.ok_or_else(|| input.error("missing `group = \"…\"`"))?,
            op,
            key,
        })
    }
}

enum HandlerShape {
    /// `(Req, &mut State) -> …` — typical product handler (shim adds `OpCtx`).
    ReqState,
    /// `(Req, OpCtx<'_>, &mut State) -> …` — full signature (no shim).
    Full,
}

fn type_last_ident(ty: &Type) -> Option<String> {
    let Type::Path(p) = ty else {
        return None;
    };
    p.path.segments.last().map(|s| s.ident.to_string())
}

fn is_mut_state_ref(ty: &Type) -> bool {
    matches!(&*ty, Type::Reference(r) if r.mutability.is_some())
}

fn result_ok_type(ty: &Type) -> Option<Type> {
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
    match args.args.first()? {
        GenericArgument::Type(t) => Some(t.clone()),
        _ => None,
    }
}

fn typed_args(input_fn: &ItemFn) -> syn::Result<Vec<Type>> {
    let mut out = Vec::new();
    for arg in &input_fn.sig.inputs {
        let FnArg::Typed(PatType { ty, .. }) = arg else {
            return Err(syn::Error::new_spanned(
                arg,
                "`#[cap_handler]` does not support `self` receivers",
            ));
        };
        out.push(ty.as_ref().clone());
    }
    Ok(out)
}

fn classify_handler(input_fn: &ItemFn) -> syn::Result<(HandlerShape, Type, Option<Type>)> {
    if input_fn.sig.asyncness.is_some() {
        return Err(syn::Error::new_spanned(
            input_fn.sig.fn_token,
            "`#[cap_handler]` is for sync handlers (use `CapOp::for_request_async` without this attribute for now)",
        ));
    }

    let args = typed_args(input_fn)?;
    let req = args.first().cloned().ok_or_else(|| {
        syn::Error::new_spanned(
            &input_fn.sig,
            "`#[cap_handler]` expects `(Req, &mut State)` or `(Req, OpCtx<'_>, &mut State)`",
        )
    })?;

    let shape = match args.len() {
        1 => {
            return Err(syn::Error::new_spanned(
                &input_fn.sig,
                "`#[cap_handler]` expects `(Req, &mut State)` — use `_state` when group state is unused",
            ));
        }
        2 => {
            if is_mut_state_ref(&args[1]) {
                HandlerShape::ReqState
            } else {
                return Err(syn::Error::new_spanned(
                    &args[1],
                    "`#[cap_handler]` with two parameters expects `(Req, &mut State)`",
                ));
            }
        }
        3 => {
            if type_last_ident(&args[1]).as_deref() != Some("OpCtx") {
                return Err(syn::Error::new_spanned(
                    &args[1],
                    "`#[cap_handler]` middle parameter must be `OpCtx<'_>` when using three parameters",
                ));
            }
            if !is_mut_state_ref(&args[2]) {
                return Err(syn::Error::new_spanned(
                    &args[2],
                    "`#[cap_handler]` third parameter must be `&mut State`",
                ));
            }
            HandlerShape::Full
        }
        _ => {
            return Err(syn::Error::new_spanned(
                &input_fn.sig,
                "`#[cap_handler]` expects 1–3 handler parameters",
            ));
        }
    };

    let state_ref_ty = if matches!(shape, HandlerShape::ReqState) {
        Some(args[1].clone())
    } else {
        None
    };

    Ok((shape, req, state_ref_ty))
}

fn reply_type_from_fn(input_fn: &ItemFn) -> syn::Result<Type> {
    let ReturnType::Type(_, ty) = &input_fn.sig.output else {
        return Err(syn::Error::new_spanned(
            &input_fn.sig,
            "`#[cap_handler]` handler must return `Result<Reply, CapError>`",
        ));
    };
    let Some(ok) = result_ok_type(ty) else {
        return Err(syn::Error::new_spanned(
            ty,
            "`#[cap_handler]` handler must return `Result<Reply, CapError>`",
        ));
    };
    let Type::Path(type_path) = &**ty else {
        return Err(syn::Error::new_spanned(
            ty,
            "`#[cap_handler]` handler must return `Result<Reply, CapError>`",
        ));
    };
    let seg = type_path.path.segments.last().ok_or_else(|| {
        syn::Error::new_spanned(
            ty,
            "`#[cap_handler]` handler must return `Result<Reply, CapError>`",
        )
    })?;
    if seg.ident != "Result" {
        return Err(syn::Error::new_spanned(
            ty,
            "`#[cap_handler]` handler must return `Result<Reply, CapError>`",
        ));
    }
    let PathArguments::AngleBracketed(args) = &seg.arguments else {
        return Err(syn::Error::new_spanned(
            ty,
            "`#[cap_handler]` handler must return `Result<Reply, CapError>`",
        ));
    };
    let err = args.args.iter().nth(1).and_then(|a| match a {
        GenericArgument::Type(t) => Some(t),
        _ => None,
    });
    if err.is_none_or(|e| type_last_ident(e).as_deref() != Some("CapError")) {
        return Err(syn::Error::new_spanned(
            ty,
            "`#[cap_handler]` handler must return `Result<Reply, trembita::CapError>`",
        ));
    }
    Ok(ok)
}

fn req_ident(req: &Type) -> syn::Result<Ident> {
    let Type::Path(p) = req else {
        return Err(syn::Error::new_spanned(
            req,
            "`#[cap_handler]` request type must be a struct name (not a generic or tuple)",
        ));
    };
    let seg = p.path.segments.last().ok_or_else(|| {
        syn::Error::new_spanned(req, "`#[cap_handler]` request type must be a struct name")
    })?;
    match &seg.arguments {
        PathArguments::None => Ok(seg.ident.clone()),
        _ => Err(syn::Error::new_spanned(
            req,
            "`#[cap_handler]` request type must be a plain struct name",
        )),
    }
}

fn body_ident(handler: &Ident) -> Ident {
    Ident::new(&format!("{handler}__cap_body"), handler.span())
}

fn state_type_from_ref(state_ref: &Type) -> syn::Result<Type> {
    match state_ref {
        Type::Reference(r) => Ok((*r.elem).clone()),
        other => Err(syn::Error::new_spanned(
            other,
            "`#[cap_handler]` state parameter must be `&mut State`",
        )),
    }
}

fn register_ident(handler: &Ident) -> Ident {
    Ident::new(&format!("{handler}_register"), handler.span())
}

fn arg_idents(input_fn: &ItemFn) -> syn::Result<Punctuated<Ident, Token![,]>> {
    let mut names = Punctuated::new();
    for arg in &input_fn.sig.inputs {
        let FnArg::Typed(PatType { pat, .. }) = arg else {
            continue;
        };
        let syn::Pat::Ident(p) = &**pat else {
            return Err(syn::Error::new_spanned(
                pat,
                "`#[cap_handler]` parameters must be simple identifiers",
            ));
        };
        names.push(p.ident.clone());
    }
    Ok(names)
}

pub(crate) fn expand_cap_handler(args: CapHandlerArgs, input_fn: &ItemFn) -> TokenStream2 {
    let (shape, req, state_ref_ty) = match classify_handler(input_fn) {
        Ok(v) => v,
        Err(e) => return e.to_compile_error(),
    };
    let reply = match reply_type_from_fn(input_fn) {
        Ok(r) => r,
        Err(e) => return e.to_compile_error(),
    };
    let req_ident_name = match req_ident(&req) {
        Ok(i) => i,
        Err(e) => return e.to_compile_error(),
    };

    let group = args.group.value();
    let op = args
        .op
        .as_ref()
        .map(syn::LitStr::value)
        .unwrap_or_else(|| ident_to_op(&req_ident_name));

    let has_key = args.key.is_some();
    let key_impl = match &args.key {
        Some(field) => {
            let field = field.value();
            let field_ident = Ident::new(&field, req_ident_name.span());
            quote! {
                fn cap_key(&self) -> Option<::std::string::String> {
                    Some(::std::string::ToString::to_string(&self.#field_ident))
                }
            }
        }
        None => TokenStream2::new(),
    };

    let handler_name = &input_fn.sig.ident;
    let vis = &input_fn.vis;
    let attrs = &input_fn.attrs;
    let cap_impl = quote! {
        impl ::trembita::CapRequest for #req {
            const GROUP: &'static str = #group;
            const OP: &'static str = #op;
            type Reply = #reply;
            #key_impl
        }
    };

    let register = |state_ty: &Type| register_block(&args, handler_name, &req, state_ty, has_key);

    match shape {
        HandlerShape::Full => {
            let state_ty = match typed_args(input_fn).ok().and_then(|a| a.get(2).cloned()) {
                Some(t) => match state_type_from_ref(&t) {
                    Ok(s) => s,
                    Err(e) => return e.to_compile_error(),
                },
                None => {
                    return syn::Error::new_spanned(input_fn, "missing state parameter")
                        .to_compile_error();
                }
            };
            let reg = register(&state_ty);
            quote! {
                #(#attrs)*
                #input_fn
                #cap_impl
                #reg
            }
        }
        HandlerShape::ReqState => {
            let body_name = body_ident(handler_name);
            let mut body_fn = input_fn.clone();
            body_fn.sig.ident = body_name.clone();
            body_fn.attrs.retain(|a| !a.path().is_ident("cap_handler"));

            let call_args = match arg_idents(input_fn) {
                Ok(v) => v,
                Err(e) => return e.to_compile_error(),
            };
            let FnArg::Typed(first) = input_fn.sig.inputs.first().expect("req") else {
                return syn::Error::new_spanned(input_fn, "missing request parameter")
                    .to_compile_error();
            };
            let FnArg::Typed(second) = input_fn.sig.inputs.iter().nth(1).expect("state") else {
                return syn::Error::new_spanned(input_fn, "missing state parameter")
                    .to_compile_error();
            };
            let output = &input_fn.sig.output;
            let state_ty = match state_ref_ty.as_ref() {
                Some(t) => match state_type_from_ref(t) {
                    Ok(s) => s,
                    Err(e) => return e.to_compile_error(),
                },
                None => {
                    return syn::Error::new_spanned(input_fn, "missing state type")
                        .to_compile_error();
                }
            };
            let reg = register(&state_ty);

            quote! {
                #(#attrs)*
                #[allow(non_snake_case, missing_docs)]
                #body_fn

                #[allow(missing_docs)]
                #vis fn #handler_name(
                    #first,
                    _ctx: ::trembita::OpCtx<'_>,
                    #second,
                ) #output {
                    #body_name(#call_args)
                }

                #cap_impl
                #reg
            }
        }
    }
}

fn register_block(
    args: &CapHandlerArgs,
    handler_name: &Ident,
    req: &Type,
    state_ty: &Type,
    has_key: bool,
) -> TokenStream2 {
    let reg_name = register_ident(handler_name);
    let key_chain = if has_key {
        quote! { .key_cap::<#req>() }
    } else {
        TokenStream2::new()
    };
    quote! {
        #[doc(hidden)]
        #[must_use]
        #[allow(missing_docs)]
        pub fn #reg_name(group: ::trembita::CapGroup<#state_ty>) -> ::trembita::CapGroup<#state_ty> {
            group.op(::trembita::CapOp::for_request(#handler_name) #key_chain)
        }
    }
}
