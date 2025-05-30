use std::sync::Arc;

#[derive(Clone, Default)]
pub struct CallbackHistogram {
    callbacks: Arc<boxcar::Vec<Box<dyn Fn(u64) -> () + Send + Sync + 'static>>>,
}

impl std::fmt::Debug for CallbackHistogram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CallbackHistogram").finish()
    }
}

impl CallbackHistogram {
    pub fn register_callback(&self, callback: Box<dyn Fn(u64) -> () + Send + Sync + 'static>) {
        self.callbacks.push(callback);
    }

    pub fn record(&self, value: u64) {
        for (_, callback) in self.callbacks.iter() {
            callback(value);
        }
    }
}

pub struct CallbackHistogramConfig {
    pub buckets: &'static [u64],
}
