//! Compare bounded CSV construction paths without changing production ingestion.
//!
//! `ingest_comparison <rows|direct> <input.csv> <output.parquet> <batch-size>`
//! Run the built binary under an external profiler; compilation is outside the measurement.

use std::error::Error;
use std::io::Read;
use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow::array::{Int64Array, Int64Builder, RecordBatch, StringArray, StringBuilder};
use pipeline::{PipelineIoError, schema};

#[derive(Clone, Copy)]
enum Construction {
    Rows,
    Direct,
}

fn batches(
    input: impl Read,
    size: NonZeroUsize,
    construction: Construction,
    mut consume: impl FnMut(RecordBatch) -> Result<bool, PipelineIoError>,
) -> Result<(), PipelineIoError> {
    match construction {
        Construction::Rows => row_batches(input, size, consume),
        Construction::Direct => {
            let mut reader = csv::Reader::from_reader(input);
            let mut record = csv::StringRecord::new();
            let mut row = 0;
            loop {
                let mut ids = Int64Builder::with_capacity(size.get());
                let mut names = StringBuilder::new();
                let mut notes = StringBuilder::new();
                let mut count = 0;
                while count < size.get() {
                    if !reader
                        .read_record(&mut record)
                        .map_err(|source| PipelineIoError::ReadRecord { source })?
                    {
                        break;
                    }
                    ids.append_value(parse_id(row, &record)?);
                    names.append_value(record.get(1).unwrap_or_default());
                    match record.get(2).unwrap_or_default() {
                        "" => notes.append_null(),
                        note => notes.append_value(note),
                    }
                    row += 1;
                    count += 1;
                }
                if count == 0 {
                    return Ok(());
                }
                let batch = RecordBatch::try_new(
                    schema(),
                    vec![
                        Arc::new(ids.finish()),
                        Arc::new(names.finish()),
                        Arc::new(notes.finish()),
                    ],
                )
                .map_err(|source| PipelineIoError::BuildBatch { source })?;
                if !consume(batch)? {
                    return Ok(());
                }
            }
        }
    }
}

fn parse_id(row: usize, record: &csv::StringRecord) -> Result<i64, PipelineIoError> {
    let value = record.get(0).unwrap_or_default();
    value.parse().map_err(|_| PipelineIoError::InvalidId {
        row,
        value: value.to_owned(),
    })
}

// The row control intentionally preserves the producer's per-record allocations and
// owned string intermediates. It does not collect the entire file as fixture helpers do.
fn row_batches(
    input: impl Read,
    size: NonZeroUsize,
    mut consume: impl FnMut(RecordBatch) -> Result<bool, PipelineIoError>,
) -> Result<(), PipelineIoError> {
    struct Row {
        id: i64,
        name: String,
        note: Option<String>,
    }
    let mut reader = csv::Reader::from_reader(input);
    let mut rows = Vec::with_capacity(size.get());
    let mut flush = |rows: &[Row]| {
        let ids: Int64Array = rows.iter().map(|r| r.id).collect();
        let names: StringArray = rows.iter().map(|r| Some(r.name.as_str())).collect();
        let notes: StringArray = rows.iter().map(|r| r.note.as_deref()).collect();
        let batch = RecordBatch::try_new(
            schema(),
            vec![Arc::new(ids), Arc::new(names), Arc::new(notes)],
        )
        .map_err(|source| PipelineIoError::BuildBatch { source })?;
        consume(batch)
    };
    for (row, record) in reader.records().enumerate() {
        let record = record.map_err(|source| PipelineIoError::ReadRecord { source })?;
        rows.push(Row {
            id: parse_id(row, &record)?,
            name: record.get(1).unwrap_or_default().to_owned(),
            note: match record.get(2).unwrap_or_default() {
                "" => None,
                note => Some(note.to_owned()),
            },
        });
        if rows.len() == size.get() {
            if !flush(&rows)? {
                return Ok(());
            }
            rows.clear();
        }
    }
    if !rows.is_empty() {
        flush(&rows)?;
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use std::fs::File;

    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 4 {
        return Err("usage: ingest_comparison <rows|direct> <input.csv> <output.parquet> <positive-batch-size>".into());
    }
    let construction = match args[0].as_str() {
        "rows" => Construction::Rows,
        "direct" => Construction::Direct,
        _ => return Err("construction must be rows or direct".into()),
    };
    let size: NonZeroUsize = args[3].parse()?;
    let input = File::open(&args[1])?;
    let output = File::create(&args[2])?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(output, schema(), Some(properties))?;
    let mut rows = 0;
    batches(input, size, construction, |batch| {
        rows += batch.num_rows();
        writer
            .write(&batch)
            .map_err(|source| PipelineIoError::WriteParquet { source })?;
        Ok(true)
    })?;
    writer.into_inner()?.sync_all()?;
    println!("rows={rows}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(
        input: &[u8],
        size: usize,
        mode: Construction,
    ) -> Result<Vec<RecordBatch>, PipelineIoError> {
        let mut result = Vec::new();
        batches(
            input,
            NonZeroUsize::new(size).expect("positive"),
            mode,
            |batch| {
                result.push(batch);
                Ok(true)
            },
        )?;
        Ok(result)
    }

    #[test]
    fn direct_ingest_values_nulls_and_boundaries() {
        let input = "id,name,note\n-2,Zoë,\n3,\"a,b\",\"line one\nline two\"\n4,,tail\n";
        let expected = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(vec![-2, 3, 4])),
                Arc::new(StringArray::from(vec!["Zoë", "a,b", ""])),
                Arc::new(StringArray::from(vec![
                    None,
                    Some("line one\nline two"),
                    Some("tail"),
                ])),
            ],
        )
        .expect("independent expected batch");
        for size in [1, 7, 1000] {
            for mode in [Construction::Rows, Construction::Direct] {
                let actual = collect(input.as_bytes(), size, mode).expect("valid CSV");
                assert!(
                    actual
                        .iter()
                        .all(|b| b.num_rows() <= size && b.schema() == schema())
                );
                let actual =
                    arrow::compute::concat_batches(&schema(), &actual).expect("concatenate");
                assert_eq!(actual, expected);
                assert_eq!(actual.column(2).null_count(), 1);
            }
        }
    }

    #[test]
    fn direct_ingest_preserves_error_row_and_value() {
        for size in [1, 7, 1000] {
            for mode in [Construction::Rows, Construction::Direct] {
                let err = collect(b"id,name,note\n1,a,\nnot-id,b,\n", size, mode)
                    .expect_err("invalid id");
                assert!(
                    matches!(err, PipelineIoError::InvalidId { row: 1, value } if value == "not-id")
                );
            }
        }
    }

    #[test]
    fn direct_ingest_preserves_csv_errors() {
        for input in [
            b"id,name,note\n1,a\n".as_slice(),
            b"id,name,note\n1,\xff,n\n".as_slice(),
        ] {
            for mode in [Construction::Rows, Construction::Direct] {
                assert!(matches!(
                    collect(input, 7, mode),
                    Err(PipelineIoError::ReadRecord { .. })
                ));
            }
        }
    }

    #[test]
    fn direct_ingest_empty_and_partial_batches() {
        for mode in [Construction::Rows, Construction::Direct] {
            assert!(
                collect(b"id,name,note\n", 7, mode)
                    .expect("empty")
                    .is_empty()
            );
            let input = format!(
                "id,name,note\n{}",
                (0..15).map(|i| format!("{i},n,\n")).collect::<String>()
            );
            let result = collect(input.as_bytes(), 7, mode).expect("partial final batch");
            assert_eq!(
                result.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
                [7, 7, 1]
            );
        }
    }

    #[test]
    #[ignore = "requires generated comparison files; reads full outputs outside timing"]
    fn direct_ingest_measured_outputs_match_reference() {
        let root = std::path::PathBuf::from(
            std::env::var("DATACRATE_INGEST_EVIDENCE_DIR").expect("evidence directory"),
        );
        for workload in ["short", "wide"] {
            let expected = pipeline::fixture_to_record_batch(&root.join(format!("{workload}.csv")))
                .expect("independent row reference");
            for mode in ["rows", "direct"] {
                let actual =
                    pipeline::read_parquet(&root.join(format!("{workload}-{mode}.parquet")))
                        .expect("read measured output");
                assert_eq!(actual, expected, "{workload}/{mode}");
            }
        }
    }

    #[test]
    fn direct_ingest_consumer_closure_stops_before_invalid_tail() {
        for mode in [Construction::Rows, Construction::Direct] {
            let mut calls = 0;
            batches(
                b"id,name,note\n1,a,\ninvalid,b,\n".as_slice(),
                NonZeroUsize::new(1).expect("positive"),
                mode,
                |_| {
                    calls += 1;
                    Ok(false)
                },
            )
            .expect("closed consumer stops reading");
            assert_eq!(calls, 1);
        }
    }
}
