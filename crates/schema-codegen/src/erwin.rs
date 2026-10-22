//! Parses a simplified `Entity`/`Attribute` XML subset into the same
//! `arrow_schema::Schema` shape the Parquet path produces. Erwin's own
//! Mart/XMI export formats vary across tool versions and are not fully
//! supported here — this targets the minimal shape a steward can produce by
//! hand or script:
//! `<Entities><Entity name="..."><Attribute name="..." type="..."
//! nullable="true|false"/></Entity></Entities>`. This is a deliberately
//! simplified subset, named as such, not a claim of full Erwin-format
//! compatibility.
#![cfg(feature = "erwin")]

use crate::SchemaCodegenError;
use crate::text_types::declared_type_to_arrow;
use arrow_schema::{Field, Schema};

/// Parses `xml` and returns the Arrow schema for the `Entity` named
/// `entity_name`. An `Attribute` with no `nullable` attribute is treated as
/// nullable, matching Parquet's own default for a field with no explicit
/// marker.
///
/// # Errors
/// Returns [`SchemaCodegenError::ParseErwinXml`] if `xml` is not well-formed,
/// [`SchemaCodegenError::EntityNotFound`] if no `Entity` named `entity_name`
/// exists, or [`SchemaCodegenError::UnsupportedDeclaredType`] if an
/// attribute's `type` is missing or has no Rust mapping.
pub fn parse_erwin_xml(xml: &str, entity_name: &str) -> Result<Schema, SchemaCodegenError> {
    let document = roxmltree::Document::parse(xml)
        .map_err(|source| SchemaCodegenError::ParseErwinXml { source })?;

    let entity = document
        .descendants()
        .find(|node| node.has_tag_name("Entity") && node.attribute("name") == Some(entity_name))
        .ok_or_else(|| SchemaCodegenError::EntityNotFound {
            entity: entity_name.to_string(),
        })?;

    entity
        .children()
        .filter(|node| node.has_tag_name("Attribute"))
        .map(|attribute| {
            let name = attribute.attribute("name").unwrap_or_default().to_string();
            let declared = attribute.attribute("type").unwrap_or("<none>");
            let data_type = declared_type_to_arrow(declared).ok_or_else(|| {
                SchemaCodegenError::UnsupportedDeclaredType {
                    field: name.clone(),
                    declared_type: declared.to_string(),
                }
            })?;
            let nullable = attribute
                .attribute("nullable")
                .map(|value| value == "true")
                .unwrap_or(true);
            Ok(Field::new(name, data_type, nullable))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Schema::new)
}
