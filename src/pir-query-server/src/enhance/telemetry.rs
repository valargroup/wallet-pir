//! Prometheus text snapshots shared by worker and publisher metrics. No query
//! positions, session IDs or request-derived labels.
use prometheus::{core::Collector, Encoder, GaugeVec, Opts, TextEncoder};
use std::collections::BTreeMap;

#[derive(Default)]
pub struct Metrics(BTreeMap<&'static str, GaugeVec>);
impl Metrics {
    pub fn set(&mut self, name: &'static str, labels: &[(&str, &str)], value: impl Into<f64>) {
        self.0
            .entry(name)
            .or_insert_with(|| {
                GaugeVec::new(
                    Opts::new(name, name),
                    &labels.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
                )
                .expect("static metric definition")
            })
            .with_label_values(&labels.iter().map(|(_, v)| *v).collect::<Vec<_>>())
            .set(value.into());
    }
    pub fn number(&mut self, name: &'static str, value: u64) {
        self.set(name, &[], value as f64);
    }
    pub fn finish(self) -> String {
        let families = self
            .0
            .values()
            .flat_map(|v| v.collect())
            .collect::<Vec<_>>();
        let mut bytes = Vec::new();
        TextEncoder::new()
            .encode(&families, &mut bytes)
            .expect("encode metric snapshot");
        String::from_utf8(bytes).expect("metrics are UTF-8")
    }
}
