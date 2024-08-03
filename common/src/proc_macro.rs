// extern crate proc_macro;
// use proc_macro::TokenStream;
// use quote::quote;
// use syn::{parse_macro_input, ItemFn};

// #[proc_macro_attribute]
// pub fn async_or_sync(_attr: TokenStream, item: TokenStream) -> TokenStream {
//     let input = parse_macro_input!(item as ItemFn);
//     let name = &input.sig.ident;
//     let block = &input.block;

//     let sync_fn = quote! {
//         fn #name() #block
//     };

//     let async_fn = quote! {
//         async fn #name() #block
//     };

//     let output = quote! {
//         #[cfg(feature = "sync")]
//         #sync_fn

//         #[cfg(not(feature = "sync"))]
//         #async_fn
//     };

//     output.into()
// }
// #[cfg(test)]
// mod tests {
//     use super::*;
//     #[async_or_sync]
//     fn test() {
//         // 这里是函数的逻辑代码
//         println!("This is the function logic.");
//     }
//     #[test]
//     fn test() {
//         #[cfg(feature = "sync")]
//         test()
//     }

//     // #[test]
//     // #[cfg(feature = "sync")]
//     // fn test_async_or_sync() {
//     //     test()
//     // }
// }
