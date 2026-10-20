//! Generate the physical schema used to configure Arrow builders or a writer.
use contracts::Contract;
#[allow(dead_code)]
#[derive(Contract)]
struct Ingest {
    id: i64,
    name: String,
    note: Option<String>,
}
fn main() -> Result<(), contracts::arrow::MappingError> {
    println!("{:?}", contracts::arrow::schema::<Ingest>()?);
    Ok(())
}
