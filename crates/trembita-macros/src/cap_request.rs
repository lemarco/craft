//! `#[cap_request(...)]` — [`trembita::CapRequest`](trembita::CapRequest) impl for capability DTOs.

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{Ident, ItemStruct, LitStr, Token, Type};

pub(crate) struct CapRequestArgs {
    group: LitStr,
    op: Option<LitStr>,
    reply: Type,
    key: Option<LitStr>,
}

impl Parse for CapRequestArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut group = None;
        let mut op = None;
        let mut reply = None;
        let mut key = None;

        while !input.is_empty() {
            let name: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            if name == "group" {
                group = Some(input.parse()?);
            } else if name == "op" {
                op = Some(input.parse()?);
            } else if name == "reply" {
                reply = Some(input.parse()?);
            } else if name == "key" {
                key = Some(input.parse()?);
            } else {
                return Err(syn::Error::new_spanned(
                    name,
                    "unknown option (expected `group`, `op`, `reply`, `key`)",
                ));
            }
            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }

        Ok(Self {
            group: group.ok_or_else(|| input.error("missing `group = \"…\"`"))?,
            op,
            reply: reply.ok_or_else(|| input.error("missing `reply = …`"))?,
            key,
        })
    }
}

/// PascalCase type name → default op wire name (`CreateAccount` → `create_account`).
pub(crate) fn ident_to_op(ident: &Ident) -> String {
    let s = ident.to_string();
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.extend(c.to_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

pub(crate) fn expand_cap_request(args: CapRequestArgs, input: &ItemStruct) -> TokenStream2 {
    let struct_name = &input.ident;
    let group = args.group.value();
    let op = args
        .op
        .map(|l| l.value())
        .unwrap_or_else(|| ident_to_op(struct_name));
    let reply = args.reply;

    let key_impl = match args.key {
        Some(field) => {
            let field = field.value();
            let field_ident = Ident::new(&field, struct_name.span());
            quote! {
                fn cap_key(&self) -> Option<::std::string::String> {
                    Some(::std::string::ToString::to_string(&self.#field_ident))
                }
            }
        }
        None => TokenStream2::new(),
    };

    quote! {
        #input

        impl ::trembita::CapRequest for #struct_name {
            const GROUP: &'static str = #group;
            const OP: &'static str = #op;
            type Reply = #reply;
            #key_impl
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ident_to_op;
    use syn::parse_str;

    #[test]
    fn ident_to_op_snake() {
        let id: syn::Ident = parse_str("CreateAccount").unwrap();
        assert_eq!(ident_to_op(&id), "create_account");
        let id: syn::Ident = parse_str("Append").unwrap();
        assert_eq!(ident_to_op(&id), "append");
    }
}
