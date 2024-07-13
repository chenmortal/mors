use log::error;
use mors_traits::{
    iter::{CacheIter, CacheIterator, IterError, KvCacheIter, KvCacheIterator},
    kv::ValueMeta,
};

use crate::skip_list::{Node, SkipListInner};

pub struct SkipListIter<'a> {
    inner: &'a SkipListInner,
    node: Option<&'a Node>,
    node_back: Option<&'a Node>,
}
impl<'a> SkipListIter<'a> {
    pub(crate) fn new(inner: &'a SkipListInner) -> Self {
        SkipListIter {
            inner,
            node: inner.head().into(),
            node_back: None,
        }
    }
}
impl<'a> CacheIter for SkipListIter<'a> {
    type Item = Node;

    fn item(&self) -> Option<&Self::Item> {
        if let Some(node) = self.node {
            if std::ptr::eq(node, self.inner.head()) {
                return None;
            }
        }
        self.node
    }
}

impl<'a> CacheIterator for SkipListIter<'a> {
    fn next(&mut self) -> Result<bool, IterError> {
        if let Some(now) = self.node {
            if let Ok(new) = now.next(self.inner.arena(), 0) {
                if let Some(back) = self.node_back {
                    if std::ptr::eq(new, back) {
                        return Ok(false);
                    }
                }
                self.node = new.into();
                return Ok(true);
            };
        }
        Ok(false)
    }
}
impl<'a> KvCacheIter<ValueMeta> for SkipListIter<'a> {
    fn key(&self) -> Option<mors_traits::ts::KeyTsBorrow<'_>> {
        if let Some(item) = self.item() {
            match item.get_key(self.inner.arena()) {
                Ok(k) => Some(k.into()),
                Err(e) => {
                    error!("SkipListIter::key() error: {:?}", e);
                    None
                }
            }
        } else {
            None
        }
    }

    fn value(&self) -> Option<ValueMeta> {
        if let Some(item) = self.item() {
            item.get_value(self.inner.arena())
                .map(|v| v.and_then(ValueMeta::decode))
                .unwrap_or_else(|e| {
                    error!("SkipListIter::value() error: {:?}", e);
                    None
                })
        } else {
            None
        }
    }
}
impl KvCacheIterator<ValueMeta> for SkipListIter<'_> {}
