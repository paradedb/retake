// Copyright (c) 2023-2025 Retake, Inc.
//
// This file is part of ParadeDB - Postgres for Search and Analytics
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use crate::schema::SearchFieldId;
use tantivy::schema::OwnedValue;
use tantivy::TantivyDocument;

use super::{SearchField, SearchIndexSchema};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchDocument {
    pub doc: TantivyDocument,
}

impl SearchDocument {
    #[inline(always)]
    pub fn insert(&mut self, SearchFieldId(key): SearchFieldId, value: OwnedValue) {
        self.doc.add_field_value(key, &value)
    }

    #[inline(always)]
    pub fn insert_nested(
        &mut self,
        schema: &SearchIndexSchema,
        search_field: &SearchField,
        value: OwnedValue,
    ) -> Vec<SearchDocument> {
        let field_id = search_field.id.0;
        self.doc
            .add_nested_object(
                &schema.schema,
                field_id,
                serde_json::to_value(value).expect("nested value must be valid json"),
                &search_field.config.clone().into(),
            )
            .expect("must be able to produce child docs")
            .into_iter()
            .map(|doc| doc.into())
            .collect()
    }
}

impl From<SearchDocument> for TantivyDocument {
    fn from(value: SearchDocument) -> Self {
        value.doc
    }
}

impl From<TantivyDocument> for SearchDocument {
    fn from(val: TantivyDocument) -> Self {
        SearchDocument { doc: val }
    }
}
