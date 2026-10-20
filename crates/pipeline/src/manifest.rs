//! The completion manifest [`run_bounded_pipeline_s3`](crate::bounded::run_bounded_pipeline_s3)
//! publishes after its output object: reported row count, a logical-content
//! fingerprint and captured input metadata. Output and manifest are separate
//! publications; a record's presence does not verify output bytes or establish
//! an atomic dataset commit. Digest calculation retains one row's hash at a time.
//!
//! Order independence: `accumulate_batch_digest` combines each row's
//! individual hash into a running total with wrapping (mod 2^64) addition,
//! not XOR — XOR cancels on duplicate rows (`x ^ x == 0`), which would let a
//! dataset with dropped duplicate rows hash identically to the original. The
//! resulting digest is an AdHash-style incremental multiset hash
//! (Bellare-Micciancio): a commutative, associative combiner, so the result
//! only depends on the multiset of rows, never on batch boundaries or
//! arrival order. This isn't a security boundary, so a fast non-cryptographic
//! per-row hash (`ahash`, already in the dependency tree via datafusion) is
//! enough — the digest exists to catch accidental content drift, not an
//! adversary.

use crate::batch_contract::CheckedIngestBatch;
use std::hash::{BuildHasher, Hasher};

use crate::PipelineIoError;
use ahash::RandomState;
use arrow::array::Array;
use object_store::ObjectMeta;
use object_store::path::Path as ObjectPath;
use serde::{Deserialize, Serialize};

/// Fixed, arbitrary seeds for [`hash_row`]'s `RandomState`. `ahash`'s default
/// seeds are randomised per process — meant to resist hash-flooding denial of
/// service in hash maps — which would make the digest differ across runs of
/// the *same* content. A content fingerprint needs the opposite: identical
/// content must hash identically across processes using the same pinned
/// implementation and target/CPU-feature configuration.
const DIGEST_SEED_1: u64 = 0x517c_c1b7_2722_0a95;
const DIGEST_SEED_2: u64 = 0x2545_f491_4f6c_dd1d;
const DIGEST_SEED_3: u64 = 0x9e37_79b9_7f4a_7c15;
const DIGEST_SEED_4: u64 = 0xbf58_476d_1ce4_e5b9;

/// Hashes one row's columns into the running digest.
///
/// `id`, `name`, and `note` must come from the same row of the fixed
/// 3-column schema `accumulate_batch_digest` reads (`Int64` id at column 0,
/// `Utf8` name at column 1, nullable `Utf8` note at column 2) — `note`'s
/// `Option` encodes that column's nullability. The checked batch constructor validates names, order, types and nulls before
/// exposing the typed columns to this digest.
fn hash_row(id: i64, name: &str, note: Option<&str>) -> u64 {
    let build_hasher =
        RandomState::with_seeds(DIGEST_SEED_1, DIGEST_SEED_2, DIGEST_SEED_3, DIGEST_SEED_4);
    let mut hasher = build_hasher.build_hasher();
    hasher.write_i64(id);
    hasher.write(name.as_bytes());
    match note {
        Some(note) => {
            hasher.write_u8(1);
            hasher.write(note.as_bytes());
        }
        None => hasher.write_u8(0),
    }
    hasher.finish()
}

/// Folds every row of `batch` into `running` via wrapping addition. Call
/// once per batch, in any order, over however many batches make up the
/// output — the result only depends on the multiset of rows, not on how
/// they were chunked or in what order the batches arrived.
pub(crate) fn accumulate_batch_digest(running: u64, checked: &CheckedIngestBatch) -> u64 {
    let batch = checked.batch();
    let (ids, names, notes) = checked.columns();

    (0..batch.num_rows()).fold(running, |acc, row| {
        let row_hash = hash_row(
            ids.value(row),
            names.value(row),
            (!notes.is_null(row)).then(|| notes.value(row)),
        );
        acc.wrapping_add(row_hash)
    })
}

/// Reported row count, an order-independent fingerprint and input metadata.
///
/// The decoder checks version-specific fields at entry. This public record is
/// mutable, so consuming boundaries must validate it again before relying on
/// those fields. It is not a proof of output contents or atomic publication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletionManifest {
    /// Persisted protocol version, checked by the supported-version decoder.
    pub schema_version: u32,
    /// The input object's key, as given to `run_bounded_pipeline_s3`.
    pub input_key: String,
    /// The input object's size in bytes, at the moment it was read.
    pub input_size: u64,
    /// The input object's ETag, if the store reports one — lets a reader
    /// confirm the input hasn't changed since this manifest was written.
    pub input_e_tag: Option<String>,
    /// The input object's last-modified time, RFC 3339.
    pub input_last_modified: String,
    /// Rows written to the output object.
    pub row_count: usize,
    /// `accumulate_batch_digest`'s final value, as lowercase hex —
    /// identical for the same row multiset regardless of batch or row order.
    pub content_digest: String,
    /// The unique publishing run, required in version 2.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,

    /// The captured input version, when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_version: Option<String>,

    /// The output key bound to this completion record.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_key: Option<String>,

    /// The completed output ETag, when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_e_tag: Option<String>,

    /// The completed output version, when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_version: Option<String>,

    /// The named interpretation of the persisted digest.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub digest_scheme: Option<String>,
}

/// Existing digest interpretation; not a portable or cryptographic hash format.
pub const LEGACY_DIGEST_SCHEME: &str = "legacy-ahash-wrapping-sum-v1";

/// A missing required version-2 completion field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestViolation {
    /// Version 2 requires a nonempty run ID.
    MissingRunId,
    /// Version 2 requires a nonempty output key.
    MissingOutputKey,
    /// Version 2 requires a nonempty output version or ETag.
    MissingOutputIdentity,
    /// A supplied output identity must not be empty.
    EmptyOutputIdentity,
    /// Version 2 requires a digest scheme.
    MissingDigestScheme,
}

impl std::fmt::Display for ManifestViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::MissingRunId => "version 2 requires a run ID",
            Self::MissingOutputKey => "version 2 requires an output key",
            Self::MissingOutputIdentity => "version 2 requires an output version or ETag",
            Self::EmptyOutputIdentity => "output identity must not be empty",
            Self::MissingDigestScheme => "version 2 requires a digest scheme",
        })
    }
}
impl std::error::Error for ManifestViolation {}

// Decode into wire data before returning a supported, validated record.
#[derive(Deserialize)]
struct CompletionManifestWire {
    schema_version: u32,
    input_key: String,
    input_size: u64,
    input_e_tag: Option<String>,
    input_last_modified: String,
    row_count: usize,
    content_digest: String,
    run_id: Option<String>,
    input_version: Option<String>,
    output_key: Option<String>,
    output_e_tag: Option<String>,
    output_version: Option<String>,
    digest_scheme: Option<String>,
}

impl From<CompletionManifestWire> for CompletionManifest {
    fn from(wire: CompletionManifestWire) -> Self {
        Self {
            schema_version: wire.schema_version,
            input_key: wire.input_key,
            input_size: wire.input_size,
            input_e_tag: wire.input_e_tag,
            input_last_modified: wire.input_last_modified,
            row_count: wire.row_count,
            content_digest: wire.content_digest,
            run_id: wire.run_id,
            input_version: wire.input_version,
            output_key: wire.output_key,
            output_e_tag: wire.output_e_tag,
            output_version: wire.output_version,
            digest_scheme: wire.digest_scheme,
        }
    }
}

/// Decodes a persisted completion manifest.
///
/// # Errors
/// Returns [`PipelineIoError::ReadManifest`] for malformed input or
/// [`PipelineIoError::UnsupportedManifestVersion`] for an unknown version.
/// Missing version-specific fields return [`PipelineIoError::InvalidManifest`];
/// unknown digest schemes return [`PipelineIoError::UnsupportedDigestScheme`].
/// Raw Serde decoding does not perform this supported-version check.
pub fn decode_completion_manifest(bytes: &[u8]) -> Result<CompletionManifest, PipelineIoError> {
    let wire: CompletionManifestWire =
        serde_json::from_slice(bytes).map_err(|source| PipelineIoError::ReadManifest { source })?;
    let manifest = CompletionManifest::from(wire);
    manifest.validate()?;
    Ok(manifest)
}

impl CompletionManifest {
    pub(crate) fn validate(&self) -> Result<(), PipelineIoError> {
        match self.schema_version {
            1 => Ok(()),
            2 => {
                let required = |value: &Option<String>, violation| {
                    value
                        .as_deref()
                        .filter(|value| !value.trim().is_empty())
                        .map(|_| ())
                        .ok_or(PipelineIoError::InvalidManifest { violation })
                };
                required(&self.run_id, ManifestViolation::MissingRunId)?;
                required(&self.output_key, ManifestViolation::MissingOutputKey)?;
                if self.output_e_tag.is_none() && self.output_version.is_none() {
                    return Err(PipelineIoError::InvalidManifest {
                        violation: ManifestViolation::MissingOutputIdentity,
                    });
                }
                for identity in [&self.output_e_tag, &self.output_version]
                    .into_iter()
                    .flatten()
                {
                    if identity.trim().is_empty() {
                        return Err(PipelineIoError::InvalidManifest {
                            violation: ManifestViolation::EmptyOutputIdentity,
                        });
                    }
                }
                required(&self.digest_scheme, ManifestViolation::MissingDigestScheme)?;
                if self.digest_scheme.as_deref() != Some(LEGACY_DIGEST_SCHEME) {
                    return Err(PipelineIoError::UnsupportedDigestScheme {
                        scheme: self.digest_scheme.clone().unwrap_or_default(),
                    });
                }
                Ok(())
            }
            version => Err(PipelineIoError::UnsupportedManifestVersion { version }),
        }
    }

    pub(crate) fn new(
        input_key: &ObjectPath,
        input_meta: &ObjectMeta,
        row_count: usize,
        content_digest: u64,
    ) -> Self {
        Self {
            schema_version: 1,
            input_key: input_key.to_string(),
            input_size: input_meta.size,
            input_e_tag: input_meta.e_tag.clone(),
            input_last_modified: input_meta.last_modified.to_rfc3339(),
            row_count,
            content_digest: format!("{content_digest:016x}"),
            run_id: None,
            input_version: None,
            output_key: None,
            output_e_tag: None,
            output_version: None,
            digest_scheme: None,
        }
    }

    /// The manifest's own key for a given output key: a `.manifest.json`
    /// sibling. Its presence alone does not establish that it describes the
    /// current output; readers need the publication protocol and identity checks.
    pub fn key_for(output_key: &ObjectPath) -> ObjectPath {
        ObjectPath::from(format!("{output_key}.manifest.json"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema;
    use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use std::sync::Arc;

    fn batch(rows: &[(i64, &str, Option<&str>)]) -> CheckedIngestBatch {
        let ids: ArrayRef = Arc::new(Int64Array::from(
            rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        ));
        let names: ArrayRef = Arc::new(StringArray::from(
            rows.iter().map(|r| Some(r.1)).collect::<Vec<_>>(),
        ));
        let notes: ArrayRef = Arc::new(StringArray::from(
            rows.iter().map(|r| r.2).collect::<Vec<_>>(),
        ));
        CheckedIngestBatch::try_new(
            RecordBatch::try_new(schema(), vec![ids, names, notes])
                .expect("test batch should build"),
        )
        .expect("valid ingest batch")
    }

    // These values were generated by the baseline executable, not by the
    // candidate constructor. The legacy algorithm is target-dependent.
    #[cfg(all(target_arch = "aarch64", target_os = "macos", target_feature = "aes"))]
    #[test]
    fn digest_matches_independent_pinned_target_baseline() {
        #[derive(Deserialize)]
        struct Observation {
            csv: String,
            batch_size: usize,
            empty_note: bool,
            rows: usize,
            digest: String,
        }
        #[derive(Deserialize)]
        struct Fixture {
            observations: Vec<Observation>,
        }
        let fixture: Fixture = serde_json::from_str(include_str!(
            "../tests/fixtures/manifests/legacy-digest-aarch64-apple-darwin.json"
        ))
        .expect("baseline fixture");
        assert_eq!(fixture.observations.len(), 36);
        let dir = tempfile::tempdir().expect("fixture directory");
        let input = dir.path().join("input.csv");
        for observation in fixture.observations {
            std::fs::write(&input, &observation.csv).expect("input fixture");
            let batches = crate::fixture_to_record_batches(&input, observation.batch_size)
                .expect("baseline CSV");
            let mut digest = 0;
            let mut rows = 0;
            for mut value in batches {
                rows += value.num_rows();
                if observation.empty_note {
                    value = RecordBatch::try_new(
                        value.schema(),
                        vec![
                            Arc::clone(value.column(0)),
                            Arc::clone(value.column(1)),
                            Arc::new(StringArray::from(vec![Some(""); value.num_rows()])),
                        ],
                    )
                    .expect("empty non-null notes");
                }
                let checked = CheckedIngestBatch::try_new(value).expect("valid baseline batch");
                digest = accumulate_batch_digest(digest, &checked);
            }
            assert_eq!(rows, observation.rows);
            assert_eq!(format!("{digest:016x}"), observation.digest);
        }
    }

    #[test]
    fn digest_is_independent_of_batch_boundaries() {
        let rows = [(1, "Ada", None), (2, "Zoë", Some("note"))];
        let whole = accumulate_batch_digest(0, &batch(&rows));

        let split = rows.iter().fold(0u64, |acc, row| {
            accumulate_batch_digest(acc, &batch(&[*row]))
        });

        assert_eq!(whole, split);
    }

    #[test]
    fn digest_is_independent_of_row_order() {
        let forward = batch(&[(1, "Ada", None), (2, "Zoë", Some("note"))]);
        let backward = batch(&[(2, "Zoë", Some("note")), (1, "Ada", None)]);

        assert_eq!(
            accumulate_batch_digest(0, &forward),
            accumulate_batch_digest(0, &backward)
        );
    }

    #[test]
    fn digest_changes_when_content_changes() {
        let a = batch(&[(1, "Ada", None)]);
        let b = batch(&[(1, "Ada", Some("different"))]);

        assert_ne!(
            accumulate_batch_digest(0, &a),
            accumulate_batch_digest(0, &b)
        );
    }

    #[test]
    fn duplicate_rows_do_not_cancel_out() {
        let one = batch(&[(1, "Ada", None)]);
        let two = batch(&[(1, "Ada", None), (1, "Ada", None)]);

        assert_ne!(
            accumulate_batch_digest(0, &one),
            accumulate_batch_digest(0, &two),
            "a combiner where duplicates cancel (e.g. XOR) would wrongly make \
             these equal"
        );
    }

    #[test]
    fn digest_distinguishes_null_empty_and_field_boundaries_in_legacy_examples() {
        let null = accumulate_batch_digest(0, &batch(&[(1, "Ada", None)]));
        let empty = accumulate_batch_digest(0, &batch(&[(1, "Ada", Some(""))]));
        assert_ne!(null, empty);
        assert_ne!(
            accumulate_batch_digest(0, &batch(&[(1, "a", Some("bc"))])),
            accumulate_batch_digest(0, &batch(&[(1, "ab", Some("c"))])),
        );
        let single = accumulate_batch_digest(0, &batch(&[(1, "Ada", None)]));
        assert_eq!(
            accumulate_batch_digest(0, &batch(&[(1, "Ada", None), (1, "Ada", None)])),
            single.wrapping_add(single)
        );
    }

    #[test]
    fn manifest_key_is_a_sibling_of_the_output_key() {
        let output = ObjectPath::from("prefix/output.parquet");
        assert_eq!(
            CompletionManifest::key_for(&output).as_ref(),
            "prefix/output.parquet.manifest.json"
        );
    }
}

#[cfg(test)]
mod version_regressions {
    use super::*;
    const V1: &[u8] = include_bytes!("../tests/fixtures/manifests/v1.json");

    #[test]
    fn manifest_version_one_fixture_preserves_persisted_values() {
        let manifest = decode_completion_manifest(V1).expect("legacy manifest");
        assert_eq!(manifest.schema_version, 1);
        assert_eq!(manifest.input_key, "input.csv");
        assert_eq!(manifest.input_size, 27);
        assert_eq!(manifest.input_e_tag.as_deref(), Some("captured-etag"));
        assert_eq!(manifest.row_count, 2);
        assert_eq!(manifest.content_digest, "0123456789abcdef");
        let stored: serde_json::Value = serde_json::from_slice(V1).expect("fixture JSON");
        assert_eq!(serde_json::to_value(manifest).expect("serialize"), stored);
        let baseline = CompletionManifest::new(
            &ObjectPath::from("input.csv"),
            &ObjectMeta {
                location: ObjectPath::from("input.csv"),
                last_modified: chrono::DateTime::parse_from_rfc3339("2026-10-20T00:00:00Z")
                    .expect("timestamp")
                    .to_utc(),
                size: 27,
                e_tag: Some("captured-etag".to_owned()),
                version: None,
            },
            2,
            0x0123_4567_89ab_cdef,
        );
        assert_eq!(
            serde_json::to_value(baseline).expect("legacy constructor"),
            stored
        );
    }

    #[test]
    fn manifest_version_rejects_unknown_versions() {
        for version in [0, 3, 99, u32::MAX] {
            let mut value: serde_json::Value = serde_json::from_slice(V1).expect("fixture");
            value["schema_version"] = version.into();
            assert!(matches!(
                decode_completion_manifest(&serde_json::to_vec(&value).expect("JSON")),
                Err(PipelineIoError::UnsupportedManifestVersion { version: actual }) if actual == version
            ));
        }
    }

    #[test]
    fn manifest_version_requires_well_formed_version_and_required_fields() {
        for invalid in [serde_json::Value::Null, (-1).into(), "1".into(), 1.5.into()] {
            let mut value: serde_json::Value = serde_json::from_slice(V1).expect("fixture");
            value["schema_version"] = invalid;
            assert!(matches!(
                decode_completion_manifest(&serde_json::to_vec(&value).expect("JSON")),
                Err(PipelineIoError::ReadManifest { .. })
            ));
        }
        for field in [
            "schema_version",
            "input_key",
            "input_size",
            "row_count",
            "content_digest",
        ] {
            let mut value: serde_json::Value = serde_json::from_slice(V1).expect("fixture");
            value.as_object_mut().expect("object").remove(field);
            assert!(matches!(
                decode_completion_manifest(&serde_json::to_vec(&value).expect("JSON")),
                Err(PipelineIoError::ReadManifest { .. })
            ));
        }
        assert!(matches!(
            decode_completion_manifest(b"{"),
            Err(PipelineIoError::ReadManifest { .. })
        ));
    }
    #[test]
    fn manifest_version_publisher_refuses_unsupported_version() {
        let mut manifest = decode_completion_manifest(V1).expect("legacy manifest");
        manifest.schema_version = 99;
        let store = object_store::memory::InMemory::new();
        assert!(matches!(
            crate::object_store_io::put_manifest(
                &store,
                &manifest,
                &ObjectPath::from("output.manifest.json")
            ),
            Err(PipelineIoError::UnsupportedManifestVersion { version: 99 })
        ));
    }
    #[test]
    fn manifest_version_two_fixture_is_supported() {
        let bytes = include_bytes!("../tests/fixtures/manifests/v2.json");
        let manifest = decode_completion_manifest(bytes).expect("version two manifest");
        assert_eq!(manifest.schema_version, 2);
        assert_eq!(manifest.content_digest, "0123456789abcdef");
    }
    #[test]
    fn manifest_version_two_requires_protocol_fields() {
        let original: serde_json::Value =
            serde_json::from_slice(include_bytes!("../tests/fixtures/manifests/v2.json"))
                .expect("fixture");
        for (fields, violation) in [
            (vec!["run_id"], ManifestViolation::MissingRunId),
            (vec!["output_key"], ManifestViolation::MissingOutputKey),
            (
                vec!["output_e_tag", "output_version"],
                ManifestViolation::MissingOutputIdentity,
            ),
            (
                vec!["digest_scheme"],
                ManifestViolation::MissingDigestScheme,
            ),
        ] {
            let mut value = original.clone();
            for field in fields {
                value.as_object_mut().expect("object").remove(field);
            }
            assert!(
                matches!(decode_completion_manifest(&serde_json::to_vec(&value).expect("JSON")),
                Err(PipelineIoError::InvalidManifest { violation: actual }) if actual == violation)
            );
        }
        for field in ["output_e_tag", "output_version"] {
            let mut value = original.clone();
            value[field] = " ".into();
            assert!(matches!(
                decode_completion_manifest(&serde_json::to_vec(&value).expect("JSON")),
                Err(PipelineIoError::InvalidManifest {
                    violation: ManifestViolation::EmptyOutputIdentity
                })
            ));
        }
        for (field, violation) in [
            ("run_id", ManifestViolation::MissingRunId),
            ("output_key", ManifestViolation::MissingOutputKey),
            ("digest_scheme", ManifestViolation::MissingDigestScheme),
        ] {
            let mut value = original.clone();
            value[field] = " ".into();
            assert!(
                matches!(decode_completion_manifest(&serde_json::to_vec(&value).expect("JSON")),
                Err(PipelineIoError::InvalidManifest { violation: actual }) if actual == violation)
            );
        }
    }

    #[test]
    fn manifest_version_two_accepts_each_identity_and_preserves_wire_values() {
        let original: serde_json::Value =
            serde_json::from_slice(include_bytes!("../tests/fixtures/manifests/v2.json"))
                .expect("fixture");
        for removed in [None, Some("output_e_tag"), Some("output_version")] {
            let mut value = original.clone();
            if let Some(field) = removed {
                value.as_object_mut().expect("object").remove(field);
            }
            let manifest = decode_completion_manifest(&serde_json::to_vec(&value).expect("JSON"))
                .expect("one identity is sufficient");
            assert_eq!(manifest.input_version.as_deref(), Some("input-v1"));
            assert_eq!(manifest.run_id.as_deref(), Some("run-fixture"));
            assert_eq!(serde_json::to_value(&manifest).expect("serialize"), value);
            assert!(matches!(
                crate::object_store_io::put_manifest(
                    &object_store::memory::InMemory::new(),
                    &manifest,
                    &ObjectPath::from("manifest.json")
                ),
                Err(PipelineIoError::UnsupportedManifestWriteVersion { version: 2 })
            ));
        }
    }

    #[test]
    fn manifest_version_two_rejects_unknown_digest_scheme() {
        let mut value: serde_json::Value =
            serde_json::from_slice(include_bytes!("../tests/fixtures/manifests/v2.json"))
                .expect("fixture");
        value["digest_scheme"] = "future-hash".into();
        assert!(
            matches!(decode_completion_manifest(&serde_json::to_vec(&value).expect("JSON")),
            Err(PipelineIoError::UnsupportedDigestScheme { scheme }) if scheme == "future-hash")
        );
    }
}
