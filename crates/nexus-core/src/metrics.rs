//! Metrics (task §13): "metrics that must exist because debugging is otherwise
//! blind". Hand-rolled counter map + Prometheus text format, so the native
//! runtime needs no `prometheus` crate and the WASM build stays dependency-flat.
//!
//! Every metric the actor records is listed in [`names`] and is asserted by the
//! acceptance tests, which is what makes this module load-bearing rather than
//! decorative.

use std::collections::BTreeMap;

/// Canonical metric names. Kept in one place so `/metrics` and the tests cannot
/// drift apart.
pub mod names {
    /// Queued jobs per pool (gauge).
    pub const QUEUE_DEPTH: &str = "nexus_queue_depth";
    /// Workers in `ready` state per pool (gauge).
    pub const READY_WORKERS: &str = "nexus_ready_workers";
    /// Connected workers, any engine state (gauge).
    pub const CONNECTED_WORKERS: &str = "nexus_connected_workers";
    /// Workers still loading their engine (gauge).
    pub const LOADING_WORKERS: &str = "nexus_loading_workers";
    /// Live leases (gauge).
    pub const LEASES: &str = "nexus_leases";
    /// Jobs accepted (counter).
    pub const JOBS_ACCEPTED: &str = "nexus_jobs_accepted_total";
    /// Jobs rejected at admission, labelled by reason (counter).
    pub const JOBS_REJECTED: &str = "nexus_jobs_rejected_total";
    /// Jobs reaching a terminal state, labelled by reason (counter).
    pub const JOBS_TERMINAL: &str = "nexus_jobs_terminal_total";
    /// Consumer cancellations issued (counter).
    pub const CANCELS_SENT: &str = "nexus_cancels_sent_total";
    /// Worker `CancelAck`s received (counter).
    pub const CANCEL_ACKS: &str = "nexus_cancel_acks_total";
    /// Lease expiries, labelled `no_output` / `partial` (counter).
    pub const LEASE_EXPIRIES: &str = "nexus_lease_expiries_total";
    /// Frames relayed, labelled by kind (counter).
    pub const FRAMES: &str = "nexus_frames_total";
    /// Duplicate seqs dropped (counter).
    pub const FRAMES_DUPLICATE: &str = "nexus_frames_duplicate_total";
    /// Seq gaps detected (counter).
    pub const SEQ_GAPS: &str = "nexus_seq_gaps_total";
    /// Frames dropped because their connection epoch was stale (counter).
    pub const STALE_FRAMES: &str = "nexus_stale_frames_total";
    /// In-band errors emitted after the status was spent (counter).
    pub const INBAND_ERRORS: &str = "nexus_inband_errors_total";
    /// Idempotent replays that attached to an existing job (counter).
    pub const IDEMPOTENT_ATTACH: &str = "nexus_idempotent_attach_total";
    /// Worker resumes accepted / refused (counter).
    pub const RESUME_ACCEPTED: &str = "nexus_resume_accepted_total";
    /// Resume refusals (counter).
    pub const RESUME_REJECTED: &str = "nexus_resume_rejected_total";
    /// Workers evicted by a newer socket with the same id (counter).
    pub const WORKER_EVCTIONS: &str = "nexus_worker_evictions_total";
    /// Payload bytes spilled to the reference store (counter).
    pub const SPILL_BYTES: &str = "nexus_spill_bytes_total";
    /// Artifact bytes written to disk/object storage (counter).
    pub const ARTIFACT_BYTES: &str = "nexus_artifact_bytes_total";
    /// `t_accept → t_dispatch` in ms (summary).
    pub const DISPATCH_LATENCY: &str = "nexus_dispatch_latency_ms";
    /// `t_accept → first relayed frame` in ms (summary).
    pub const TTFT: &str = "nexus_ttft_ms";
    /// `t_dispatch → t_terminal` in ms (summary).
    pub const JOB_DURATION: &str = "nexus_job_duration_ms";
    /// Output bytes relayed to consumers (counter).
    pub const RELAY_BYTES: &str = "nexus_relay_bytes_total";
    /// Completion tokens reported by workers, when they report usage (counter).
    pub const TOKENS: &str = "nexus_tokens_total";
}

/// Counter/gauge registry. `u64` throughout: every value here is a count or a
/// millisecond sum, so floats would only add display noise.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Metrics {
    counters: BTreeMap<String, u64>,
    gauges: BTreeMap<String, u64>,
    /// Registered help strings, so the rendered text is self-describing.
    help: BTreeMap<String, String>,
}

impl Metrics {
    /// Empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach a `# HELP` line to a metric base name.
    pub fn help(&mut self, base: &str, text: &str) {
        self.help.insert(base.to_string(), text.to_string());
    }

    /// Add `by` to a counter (creates it at `by`).
    pub fn incr(&mut self, name: &str, by: u64) {
        *self.counters.entry(name.to_string()).or_insert(0) += by;
    }

    /// Set a gauge.
    pub fn set(&mut self, name: &str, value: u64) {
        self.gauges.insert(name.to_string(), value);
    }

    /// Increment a labelled counter, e.g. `with_label(JOBS_REJECTED, "reason", "queue_full")`.
    pub fn incr_label(&mut self, base: &str, label: &str, value: &str, by: u64) {
        self.incr(&format!("{base}{{{label}=\"{value}\"}}"), by);
    }

    /// Set a labelled gauge.
    pub fn set_label(&mut self, base: &str, label: &str, value: &str, reading: u64) {
        self.set(&format!("{base}{{{label}=\"{value}\"}}"), reading);
    }

    /// Record one observation into a `_sum` / `_count` pair (a Prometheus
    /// summary without buckets, which is all latency debugging needs here).
    pub fn observe(&mut self, base: &str, value_ms: u64) {
        self.incr(&format!("{base}_sum"), value_ms);
        self.incr(&format!("{base}_count"), 1);
    }

    /// Read a counter or gauge; `0` when never written.
    #[must_use]
    pub fn get(&self, name: &str) -> u64 {
        self.counters.get(name).or(self.gauges.get(name)).copied().unwrap_or(0)
    }

    /// All counter keys and values, sorted (tests assert on this exact set).
    #[must_use]
    pub fn counters(&self) -> &BTreeMap<String, u64> {
        &self.counters
    }

    /// All gauge keys and values, sorted.
    #[must_use]
    pub fn gauges(&self) -> &BTreeMap<String, u64> {
        &self.gauges
    }

    /// Render Prometheus text exposition format 0.0.4.
    ///
    /// Grouped by base metric name so each family gets exactly one `# TYPE`
    /// line; `BTreeMap` iteration keeps the output byte-stable, which the tests
    /// rely on.
    #[must_use]
    pub fn render(&self) -> String {
        use std::fmt::Write as _;
        let mut out = String::new();
        let mut families: BTreeMap<String, Vec<(String, u64, &'static str)>> = BTreeMap::new();

        for (key, value) in &self.gauges {
            families.entry(base_name(key)).or_default().push((key.clone(), *value, "gauge"));
        }
        for (key, value) in &self.counters {
            families.entry(base_name(key)).or_default().push((key.clone(), *value, "counter"));
        }

        for (base, samples) in families {
            if let Some(help_text) = self.help.get(&base) {
                let _ = writeln!(out, "# HELP {base} {help_text}");
            }
            let kind = samples.first().map_or("untyped", |(_, _, kind)| *kind);
            let _ = writeln!(out, "# TYPE {base} {kind}");
            for (key, value, _) in &samples {
                let _ = writeln!(out, "{key} {value}");
            }
        }
        out
    }
}

/// Strip the label set from a metric key to get its family name.
#[must_use]
pub fn base_name(key: &str) -> String {
    match key.split_once('{') {
        Some((base, _)) => base.to_string(),
        None => key.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_gauges_and_observations_are_exact() {
        let mut m = Metrics::new();
        m.incr(names::JOBS_ACCEPTED, 1);
        m.incr(names::JOBS_ACCEPTED, 1);
        m.incr_label(names::JOBS_REJECTED, "reason", "queue_full", 3);
        m.set(names::QUEUE_DEPTH, 7);
        m.observe(names::TTFT, 250);
        m.observe(names::TTFT, 750);

        assert_eq!(m.get(names::JOBS_ACCEPTED), 2);
        assert_eq!(m.get("nexus_jobs_rejected_total{reason=\"queue_full\"}"), 3);
        assert_eq!(m.get(names::QUEUE_DEPTH), 7);
        assert_eq!(m.get("nexus_ttft_ms_sum"), 1_000);
        assert_eq!(m.get("nexus_ttft_ms_count"), 2);
        assert_eq!(m.get("nexus_never_written"), 0);
    }

    #[test]
    fn render_is_stable_grouped_and_typed() {
        let mut m = Metrics::new();
        m.help(names::QUEUE_DEPTH, "Queued jobs per pool");
        m.set(names::QUEUE_DEPTH, 2);
        m.set_label(names::READY_WORKERS, "pool", "llm.chat/qwen38", 1);
        m.incr(names::FRAMES_DUPLICATE, 4);
        assert_eq!(
            m.render(),
            concat!(
                "# TYPE nexus_frames_duplicate_total counter\n",
                "nexus_frames_duplicate_total 4\n",
                "# TYPE nexus_queue_depth gauge\n",
                "nexus_queue_depth 2\n",
                "# TYPE nexus_ready_workers gauge\n",
                "nexus_ready_workers{pool=\"llm.chat/qwen38\"} 1\n"
            ),
            "families are sorted by base name, gauges and counters share the \
             same ordering pass, and HELP appears only when registered"
        );
    }

    #[test]
    fn help_lines_render_only_when_registered() {
        let mut m = Metrics::new();
        m.help(names::QUEUE_DEPTH, "Queued jobs per pool");
        m.set(names::QUEUE_DEPTH, 1);
        assert_eq!(
            m.render(),
            concat!(
                "# HELP nexus_queue_depth Queued jobs per pool\n",
                "# TYPE nexus_queue_depth gauge\n",
                "nexus_queue_depth 1\n"
            )
        );
    }

    #[test]
    fn base_name_splits_labels() {
        assert_eq!(base_name("nexus_jobs_total{reason=\"x\"}"), "nexus_jobs_total");
        assert_eq!(base_name("nexus_jobs_total"), "nexus_jobs_total");
    }
}
