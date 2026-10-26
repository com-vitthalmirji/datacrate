//! Legacy callers must reach patched code while preserving real compressed I/O.
#![forbid(unsafe_code)]

use std::{
    error::Error,
    fs::File,
    io::{Cursor, Read, Write},
};

use arrow::{
    array::{ArrayRef, Int64Array},
    compute::concat_batches,
    error::ArrowError,
    record_batch::RecordBatch,
};
use arrow_ipc::{
    CompressionType,
    reader::FileReader,
    writer::{FileWriter, IpcWriteOptions},
};
use parquet::{
    arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
    basic::{Compression, ZstdLevel},
};

type TestResult = Result<(), Box<dyn Error>>;

fn batch() -> Result<RecordBatch, ArrowError> {
    RecordBatch::try_from_iter([(
        "value",
        std::sync::Arc::new(Int64Array::from(vec![42_i64; 4096])) as ArrayRef,
    )])
}

#[test]
fn legacy_bulk_and_dictionary_interfaces_preserve_the_payload() -> TestResult {
    let payload = b"orders amount amount amount; independent expected bytes";
    let compressed = zstd_compat::bulk::compress(payload, 3)?;
    assert_eq!(
        zstd_compat::bulk::decompress(&compressed, payload.len())?,
        payload
    );

    let dictionary = b"orders amount source target contract records";
    let mut encoder = zstd_compat::bulk::Compressor::with_dictionary(3, dictionary)?;
    let mut decoder = zstd_compat::bulk::Decompressor::with_dictionary(dictionary)?;
    assert_eq!(
        decoder.decompress(&encoder.compress(payload)?, payload.len())?,
        payload
    );
    Ok(())
}

#[test]
fn abandoned_decode_does_not_leak_into_a_reused_context() -> TestResult {
    let private = zstd_compat::encode_all(Cursor::new(vec![b'a'; 512 * 1024]), 1)?;
    let public = b"only this second request may be returned";
    let encoded = zstd_compat::encode_all(Cursor::new(public), 1)?;
    let mut context = zstd_compat::zstd_safe::DCtx::create();
    {
        let mut first =
            zstd_compat::stream::read::Decoder::with_context(Cursor::new(private), &mut context);
        first.read_exact(&mut [0_u8; 1])?;
    }
    let mut second =
        zstd_compat::stream::read::Decoder::with_context(Cursor::new(encoded), &mut context);
    let mut actual = Vec::new();
    second.read_to_end(&mut actual)?;
    assert_eq!(actual, public);
    Ok(())
}

#[test]
fn abandoned_encode_does_not_leak_into_a_reused_context() -> TestResult {
    let mut context = zstd_compat::zstd_safe::CCtx::create();
    {
        let mut first = zstd_compat::stream::read::Encoder::with_context(
            Cursor::new(vec![b'a'; 512 * 1024]),
            &mut context,
        );
        first.read_exact(&mut [0_u8; 1])?;
    }
    let expected = b"the new encoder starts a separate frame";
    let mut second =
        zstd_compat::stream::read::Encoder::with_context(Cursor::new(expected), &mut context);
    let mut encoded = Vec::new();
    second.read_to_end(&mut encoded)?;
    assert_eq!(zstd_compat::decode_all(Cursor::new(encoded))?, expected);
    Ok(())
}

#[test]
fn invalid_legacy_frame_never_reaches_the_output_writer() -> TestResult {
    // Zstd v0.7 magic followed by a reserved frame-header bit.
    let frame = [0x27, 0xb5, 0x2f, 0xfd, 0x08, 0x00];
    let mut output = Vec::new();
    {
        let mut decoder = zstd_compat::stream::write::Decoder::new(&mut output)?.auto_flush();
        assert!(decoder.write_all(&frame).is_err());
        // Exercise the error cleanup path as well as the initial rejection.
        let _ = decoder.flush();
    }
    assert!(output.is_empty());
    Ok(())
}

#[test]
fn parquet_zstd_output_preserves_every_value() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("orders.parquet");
    let expected = batch()?;
    pipeline::write_parquet(&expected, &path, Compression::ZSTD(ZstdLevel::try_new(3)?))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    assert!(builder.metadata().row_groups().iter().all(|group| {
        group
            .columns()
            .iter()
            .all(|column| matches!(column.compression(), Compression::ZSTD(_)))
    }));
    let decoded = builder.build()?.collect::<Result<Vec<_>, _>>()?;
    assert_eq!(concat_batches(&expected.schema(), &decoded)?, expected);
    Ok(())
}

#[test]
fn arrow_ipc_zstd_output_preserves_every_value() -> TestResult {
    let expected = batch()?;
    let options = IpcWriteOptions::default().try_with_compression(Some(CompressionType::ZSTD))?;
    let mut encoded = Vec::new();
    {
        let mut writer =
            FileWriter::try_new_with_options(&mut encoded, &expected.schema(), options)?;
        writer.write(&expected)?;
        writer.finish()?;
    }
    // Ensure this repetitive fixture actually exercised compression, not raw buffer fallback.
    assert!(encoded.len() < expected.num_rows() * std::mem::size_of::<i64>());
    let decoded =
        FileReader::try_new(Cursor::new(encoded), None)?.collect::<Result<Vec<_>, _>>()?;
    assert_eq!(concat_batches(&expected.schema(), &decoded)?, expected);
    Ok(())
}
