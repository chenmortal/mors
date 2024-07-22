use std::{
    ops::Deref,
    sync::{atomic::AtomicUsize, Arc},
};

use log::info;
use mors_common::closer::Closer;
use tokio::{
    select,
    sync::{
        mpsc::{Receiver, Sender},
        Notify,
    },
};

use crate::{error::MorsVlogError, histogram::Histogram};

type Result<T> = std::result::Result<T, MorsVlogError>;
const MAX_VALUE_THRESHOLD: usize = 1 << 20;
#[derive(Debug, Clone)]
pub(crate) struct VlogThreshold(Arc<VlogThresholdInner>);
#[derive(Debug)]
pub(crate) struct VlogThresholdInner {
    config: VlogThresholdConfig,
    value_threshold: AtomicUsize,
    histogram: Histogram,
    closer: Closer,
    sender: Sender<Vec<usize>>,
    clear_notify: Arc<Notify>,
}
#[derive(Debug, Clone, Copy)]
pub struct VlogThresholdConfig {
    max_value_threshold: usize,
    vlog_percentile: f64,
    value_threshold: usize,
    max_batch_size: usize,
}

impl VlogThresholdConfig {
    pub fn set_vlog_percentile(&mut self, vlog_percentile: f64) {
        self.vlog_percentile = vlog_percentile;
    }

    pub fn set_value_threshold(&mut self, value_threshold: usize) {
        self.value_threshold = value_threshold;
    }
    pub fn set_max_batch_size(&mut self, max_batch_size: usize) {
        self.max_batch_size = max_batch_size;
    }
    #[deny(unused)]
    pub(crate) fn check_threshold_config(
        &mut self,
        // max_batch_size: usize,
    ) -> Result<()> {
        // self.max_value_threshold = MAX_VALUE_THRESHOLD.min(max_batch_size);
        // assert!(self.max_value_threshold >= self.value_threshold)

        self.max_value_threshold = MAX_VALUE_THRESHOLD.min(self.max_batch_size);

        if self.vlog_percentile < 0.0 || self.vlog_percentile > 1.0 {
            return Err(MorsVlogError::ThresholdError(
                "vlog_percentile must be within range of 0.0-1.0".to_string(),
            ));
        }
        if self.value_threshold > MAX_VALUE_THRESHOLD {
            return Err(MorsVlogError::ThresholdError(format!(
                "Invalid ValueThreshold, must be less or equal to {}",
                MAX_VALUE_THRESHOLD
            )));
        }
        if self.value_threshold > self.max_batch_size {
            return Err(MorsVlogError::ThresholdError(format!(
                "ValueThreshold {} greater than max batch size of {}. Either reduce ValueThreshold or increase max_table_size",
                self.value_threshold,
                self.max_batch_size
            )));
        }
        Ok(())
    }

    pub fn value_threshold(&self) -> usize {
        self.value_threshold
    }
}
impl Default for VlogThresholdConfig {
    fn default() -> Self {
        Self {
            max_value_threshold: MAX_VALUE_THRESHOLD,
            vlog_percentile: 0.0,
            value_threshold: MAX_VALUE_THRESHOLD,
            max_batch_size: (15 * (64 << 20)) / 100,
        }
    }
}
impl VlogThresholdInner {
    pub(crate) fn new(
        config: VlogThresholdConfig,
        closer: Closer,
        sender: Sender<Vec<usize>>,
        clear_notify: Arc<Notify>,
    ) -> Self {
        let mut histogram = Histogram::default();
        histogram.measure(config.value_threshold);
        histogram.measure(config.max_value_threshold);

        Self {
            config,
            value_threshold: AtomicUsize::new(config.value_threshold),
            closer,
            sender,
            clear_notify,
            histogram,
        }
    }

    pub(crate) fn sender(&self) -> Sender<Vec<usize>> {
        self.sender.clone()
    }
    pub(crate) fn clear(&self) {
        self.set_value_threshold(self.config.value_threshold);
        self.clear_notify.notify_one();
    }
    pub(crate) fn value_threshold(&self) -> usize {
        self.value_threshold
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn set_value_threshold(&self, value_threshold: usize) {
        self.value_threshold
            .store(value_threshold, std::sync::atomic::Ordering::SeqCst);
    }
}
impl Deref for VlogThreshold {
    type Target = VlogThresholdInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl VlogThreshold {
    pub(crate) fn new(config: VlogThresholdConfig) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::channel::<Vec<usize>>(1000);
        let clear_notify = Arc::new(Notify::new());
        let clear_notified = clear_notify.clone();
        let closer =
            Closer::new("listen for value threshold update".to_string());
        let vlog_threshold = VlogThreshold(Arc::new(VlogThresholdInner::new(
            config,
            closer.clone(),
            sender,
            clear_notify,
        )));
        let vlog_c = vlog_threshold.clone();
        closer.set_joinhandle(tokio::spawn(
            vlog_c.listen_for_value_threshold_update(receiver, clear_notified),
        ));
        vlog_threshold
    }

    pub(crate) async fn listen_for_value_threshold_update(
        self,
        mut receiver: Receiver<Vec<usize>>,
        clear_notified: Arc<Notify>,
    ) {
        loop {
            select! {
                _=self.closer.cancelled()=>{
                    return ;
                }
                Some(v)=receiver.recv()=>{
                    for ele in v {
                        self.histogram.measure(ele);
                    }
                    let p=self.histogram.percentile(self.config.vlog_percentile) as usize;
                    if self.value_threshold() != p{
                        info!("updating value of threshold to: {}",p);
                        self.set_value_threshold(p);
                    }
                }
                _=clear_notified.notified()=>{
                    self.histogram.clear();
                    self.histogram.measure(self.config.value_threshold );
                    self.histogram.measure(self.config.max_value_threshold );
                }
            }
        }
    }
}
