// This file is @generated by prost-build.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ManifestChangeSet {
    /// A set of changes that are applied atomically.
    #[prost(message, repeated, tag = "1")]
    pub changes: ::prost::alloc::vec::Vec<ManifestChange>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ManifestChange {
    /// Table ID.
    #[prost(uint32, tag = "1")]
    pub id: u32,
    #[prost(enumeration = "manifest_change::Operation", tag = "2")]
    pub op: i32,
    /// Only used for CREATE.
    #[prost(uint32, tag = "3")]
    pub level: u32,
    #[prost(uint64, tag = "4")]
    pub key_id: u64,
    #[prost(enumeration = "EncryptionAlgo", tag = "5")]
    pub encryption_algo: i32,
    /// Only used for CREATE Op.
    #[prost(uint32, tag = "6")]
    pub compression: u32,
}
/// Nested message and enum types in `ManifestChange`.
pub mod manifest_change {
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum Operation {
        Create = 0,
        Delete = 1,
    }
    impl Operation {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Operation::Create => "CREATE",
                Operation::Delete => "DELETE",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "CREATE" => Some(Self::Create),
                "DELETE" => Some(Self::Delete),
                _ => None,
            }
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum EncryptionAlgo {
    Aes = 0,
}
impl EncryptionAlgo {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            EncryptionAlgo::Aes => "aes",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "aes" => Some(Self::Aes),
            _ => None,
        }
    }
}
