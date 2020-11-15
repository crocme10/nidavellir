// use log::*;
//
// use crate::db::model::ProvideError;
//
// impl From<ProvideError> for tide::Error {
//     /// Convert a ProvideError into a [tide::Error] via [Response::from]
//     ///
//     /// This allows the use of the `?` operator in handler functions
//     fn from(e: ProvideError) -> Self {
//         Response::from(e).into()
//     }
// }
