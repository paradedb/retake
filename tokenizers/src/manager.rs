// Copyright (c) 2023-2024 Retake, Inc.
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

use std::fmt::Write;

#[cfg(feature = "icu")]
use crate::icu::ICUTokenizer;
use crate::{
    cjk::ChineseTokenizer,
    code::CodeTokenizer,
    lindera::{LinderaChineseTokenizer, LinderaJapaneseTokenizer, LinderaKoreanTokenizer},
    DEFAULT_REMOVE_TOKEN_LENGTH,
};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::json;
use strum::AsRefStr;
use tantivy::tokenizer::{
    AsciiFoldingFilter, Language, LowerCaser, NgramTokenizer, RawTokenizer, RegexTokenizer,
    RemoveLongFilter, SimpleTokenizer, Stemmer, StopWordFilter, TextAnalyzer, WhitespaceTokenizer,
};

#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq, Eq)]
pub struct SearchTokenizerFilters {
    remove_long: Option<usize>,
    lowercase: Option<bool>,
    stemmer: Option<Language>,
    stopwords_language: Option<Language>,
    stopwords: Option<Vec<String>>,
}

impl SearchTokenizerFilters {
    fn from_json_value(value: &serde_json::Value) -> Result<Self, anyhow::Error> {
        let mut filters = SearchTokenizerFilters::default();

        if let Some(remove_long) = value.get("remove_long") {
            filters.remove_long = Some(remove_long.as_u64().ok_or_else(|| {
                anyhow::anyhow!(
                    "a 'remove_long' value passed to the pg_search tokenizer configuration \
                     must be of type u64, found: {remove_long:#?}"
                )
            })? as usize);
        }
        if let Some(lowercase) = value.get("lowercase") {
            filters.lowercase = Some(lowercase.as_bool().ok_or_else(|| {
                anyhow::anyhow!(
                    "a 'lowercase' value passed to the pg_search tokenizer configuration \
                     must be of type bool, found: {lowercase:#?}"
                )
            })?);
        };
        if let Some(stemmer) = value.get("stemmer") {
            filters.stemmer = Some(serde_json::from_value(stemmer.clone()).map_err(|_| {
                anyhow::anyhow!("stemmer tokenizer requires a valid 'stemmer' field")
            })?);
        }
        if let Some(stopwords_language) = value.get("stopwords_language") {
            filters.stopwords_language = Some(
                serde_json::from_value(stopwords_language.clone()).map_err(|_| {
                    anyhow::anyhow!(
                        "stopwords_language tokenizer requires a valid 'stopwords_language' field"
                    )
                })?,
            );
        }
        if let Some(stopwords) = value.get("stopwords") {
            filters.stopwords = Some(serde_json::from_value(stopwords.clone()).map_err(|_| {
                anyhow::anyhow!("stopwords tokenizer requires a valid 'stopwords' field")
            })?);
        }

        Ok(filters)
    }

    fn to_json_value(&self, enclosing: &mut serde_json::Value) {
        let enclosing = enclosing.as_object_mut().expect("object value");
        if let Some(value) = self.remove_long {
            let v = serde_json::Value::Number(value.into());
            enclosing.insert("remove_long".to_string(), v);
        }
        if let Some(value) = self.lowercase {
            let v = serde_json::Value::Bool(value);
            enclosing.insert("lowercase".to_string(), v);
        }

        if let Some(stopwords) = self.stopwords.as_ref() {
            let v = serde_json::Value::Array(
                stopwords
                    .iter()
                    .map(|s| serde_json::Value::String(s.clone()))
                    .collect(),
            );
            enclosing.insert("stopwords".to_string(), v);
        }
    }

    fn name_suffix(&self) -> String {
        let mut buffer = String::new();
        let mut is_empty = true;

        fn sep(is_empty: bool) -> &'static str {
            if is_empty {
                ""
            } else {
                ","
            }
        }

        if let Some(value) = self.remove_long {
            write!(buffer, "{}remove_long={value}", sep(is_empty))
                .expect("Writing to String buffer should never fail");
            is_empty = false;
        }
        if let Some(value) = self.lowercase {
            write!(buffer, "{}lowercase={value}", sep(is_empty))
                .expect("Writing to String buffer should never fail");
            is_empty = false;
        }
        if let Some(value) = self.stemmer {
            write!(buffer, "{}stemmer={value:?}", sep(is_empty)).unwrap();
            is_empty = false;
        }
        if let Some(value) = self.stopwords_language.as_ref() {
            write!(buffer, "{}stopwords_language={value:?}", sep(is_empty)).unwrap();
            is_empty = false;
        }

        if let Some(value) = self.stopwords.as_ref() {
            write!(buffer, "{}stopwords={value:?}", sep(is_empty)).unwrap();
            is_empty = false;
        }

        if is_empty {
            "".into()
        } else {
            format!("[{buffer}]")
        }
    }

    fn remove_long_filter(&self) -> Option<RemoveLongFilter> {
        let limit = self.remove_long.unwrap_or(DEFAULT_REMOVE_TOKEN_LENGTH);
        Some(RemoveLongFilter::limit(limit))
    }

    fn lower_caser(&self) -> Option<LowerCaser> {
        match self.lowercase {
            Some(false) => None, // Only disable if explicitly requested.
            _ => Some(LowerCaser),
        }
    }

    fn stemmer(&self) -> Option<Stemmer> {
        self.stemmer.map(Stemmer::new)
    }

    fn stopwords_language(&self) -> Option<StopWordFilter> {
        match self.stopwords_language {
            Some(language) => StopWordFilter::new(language),
            None => None,
        }
    }

    fn stopwords(&self) -> Option<StopWordFilter> {
        self.stopwords
            .as_ref()
            .map(|stop_words| StopWordFilter::remove(stop_words.clone()))
    }
}

// Serde will pick a SearchTokenizer variant based on the value of the
// "type" key, which needs to match one of the variant names below.
// The "type" field will not be present on the deserialized value.
//
// Ensure that new variants are added to the `to_json_value` and
// `from_json_value` methods. We don't use serde_json to ser/de the
// SearchTokenizer, because our bincode serialization format is incompatible
// with the "tagged" format we use in our public API.
#[derive(
    Serialize, Deserialize, Clone, Debug, PartialEq, Eq, strum_macros::VariantNames, AsRefStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum SearchTokenizer {
    Default(SearchTokenizerFilters),
    Raw(SearchTokenizerFilters),
    EnStem(SearchTokenizerFilters),
    Stem {
        language: Language,
        filters: SearchTokenizerFilters,
    },
    Lowercase(SearchTokenizerFilters),
    WhiteSpace(SearchTokenizerFilters),
    RegexTokenizer {
        pattern: String,
        filters: SearchTokenizerFilters,
    },
    ChineseCompatible(SearchTokenizerFilters),
    SourceCode(SearchTokenizerFilters),
    Ngram {
        min_gram: usize,
        max_gram: usize,
        prefix_only: bool,
        filters: SearchTokenizerFilters,
    },
    ChineseLindera(SearchTokenizerFilters),
    JapaneseLindera(SearchTokenizerFilters),
    KoreanLindera(SearchTokenizerFilters),
    #[cfg(feature = "icu")]
    #[strum(serialize = "icu")]
    ICUTokenizer(SearchTokenizerFilters),
}

impl Default for SearchTokenizer {
    fn default() -> Self {
        Self::Default(SearchTokenizerFilters::default())
    }
}

impl SearchTokenizer {
    pub fn to_json_value(&self) -> serde_json::Value {
        let mut json = match self {
            SearchTokenizer::Default(_filters) => json!({ "type": "default" }),
            SearchTokenizer::Raw(_filters) => json!({ "type": "raw" }),
            SearchTokenizer::EnStem(_filters) => json!({ "type": "en_stem" }),
            SearchTokenizer::Stem {
                language,
                filters: _,
            } => json!({ "type": "stem", "language": language }),
            SearchTokenizer::Lowercase(_filters) => json!({ "type": "lowercase" }),
            SearchTokenizer::WhiteSpace(_filters) => json!({ "type": "whitespace" }),
            SearchTokenizer::RegexTokenizer {
                pattern,
                filters: _,
            } => {
                json!({ "type": "regex", "pattern": pattern })
            }
            SearchTokenizer::ChineseCompatible(_filters) => json!({ "type": "chinese_compatible" }),
            SearchTokenizer::SourceCode(_filters) => json!({ "type": "source_code" }),
            SearchTokenizer::Ngram {
                min_gram,
                max_gram,
                prefix_only,
                filters: _,
            } => json!({
                "type": "ngram",
                "min_gram": min_gram,
                "max_gram": max_gram,
                "prefix_only": prefix_only,
            }),
            SearchTokenizer::ChineseLindera(_filters) => json!({ "type": "chinese_lindera" }),
            SearchTokenizer::JapaneseLindera(_filters) => json!({ "type": "japanese_lindera" }),
            SearchTokenizer::KoreanLindera(_filters) => json!({ "type": "korean_lindera" }),
            #[cfg(feature = "icu")]
            SearchTokenizer::ICUTokenizer(_filters) => json!({ "type": "icu" }),
        };

        // Serialize filters to the enclosing json object.
        self.filters().to_json_value(&mut json);

        json
    }

    pub fn from_json_value(value: &serde_json::Value) -> Result<Self, anyhow::Error> {
        // We use the `type` field of a JSON object to distinguish the tokenizer variant.
        // Deserialized in this "tagged enum" fashion is not supported by bincode, which
        // we use elsewhere for serialization, so we manually parse the JSON object here.

        let tokenizer_type = value["type"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("a 'type' must be passed in pg_search tokenizer configuration, not found in: {value:#?}"))?;

        let filters = SearchTokenizerFilters::from_json_value(value)?;

        match tokenizer_type {
            "default" => Ok(SearchTokenizer::Default(filters)),
            "raw" => Ok(SearchTokenizer::Raw(filters)),
            "en_stem" => Ok(SearchTokenizer::EnStem(filters)),
            "stem" => {
                let language: Language = serde_json::from_value(value["language"].clone())
                    .map_err(|_| {
                        anyhow::anyhow!("stem tokenizer requires a valid 'language' field")
                    })?;
                Ok(SearchTokenizer::Stem { language, filters })
            }
            "lowercase" => Ok(SearchTokenizer::Lowercase(filters)),
            "whitespace" => Ok(SearchTokenizer::WhiteSpace(filters)),
            "regex" => {
                let pattern: String =
                    serde_json::from_value(value["pattern"].clone()).map_err(|_| {
                        anyhow::anyhow!("regex tokenizer requires a string 'pattern' field")
                    })?;
                Ok(SearchTokenizer::RegexTokenizer { pattern, filters })
            }
            "chinese_compatible" => Ok(SearchTokenizer::ChineseCompatible(filters)),
            "source_code" => Ok(SearchTokenizer::SourceCode(filters)),
            "ngram" => {
                let min_gram: usize =
                    serde_json::from_value(value["min_gram"].clone()).map_err(|_| {
                        anyhow::anyhow!("ngram tokenizer requires an integer 'min_gram' field")
                    })?;
                let max_gram: usize =
                    serde_json::from_value(value["max_gram"].clone()).map_err(|_| {
                        anyhow::anyhow!("ngram tokenizer requires an integer 'max_gram' field")
                    })?;
                let prefix_only: bool = serde_json::from_value(value["prefix_only"].clone())
                    .map_err(|_| {
                        anyhow::anyhow!("ngram tokenizer requires a boolean 'prefix_only' field")
                    })?;
                Ok(SearchTokenizer::Ngram {
                    min_gram,
                    max_gram,
                    prefix_only,
                    filters,
                })
            }
            "chinese_lindera" => Ok(SearchTokenizer::ChineseLindera(filters)),
            "japanese_lindera" => Ok(SearchTokenizer::JapaneseLindera(filters)),
            "korean_lindera" => Ok(SearchTokenizer::KoreanLindera(filters)),
            #[cfg(feature = "icu")]
            "icu" => Ok(SearchTokenizer::ICUTokenizer(filters)),
            _ => Err(anyhow::anyhow!(
                "unknown tokenizer type: {}",
                tokenizer_type
            )),
        }
    }

    pub fn to_tantivy_tokenizer(&self) -> Option<tantivy::tokenizer::TextAnalyzer> {
        match self {
            SearchTokenizer::Default(filters) => Some(
                TextAnalyzer::builder(SimpleTokenizer::default())
                    .filter(filters.remove_long_filter())
                    .filter(filters.lower_caser())
                    .filter(filters.stemmer())
                    .filter(filters.stopwords_language())
                    .filter(filters.stopwords())
                    .build(),
            ),
            SearchTokenizer::Raw(filters) => Some(
                TextAnalyzer::builder(RawTokenizer::default())
                    .filter(filters.remove_long_filter())
                    .filter(filters.lower_caser())
                    .filter(filters.stemmer())
                    .filter(filters.stopwords_language())
                    .filter(filters.stopwords())
                    .build(),
            ),
            // Deprecated, use `raw` with `lowercase` filter instead
            SearchTokenizer::Lowercase(filters) => Some(
                TextAnalyzer::builder(RawTokenizer::default())
                    .filter(filters.remove_long_filter())
                    .filter(filters.lower_caser())
                    .filter(filters.stemmer())
                    .filter(filters.stopwords_language())
                    .filter(filters.stopwords())
                    .build(),
            ),
            SearchTokenizer::WhiteSpace(filters) => Some(
                TextAnalyzer::builder(WhitespaceTokenizer::default())
                    .filter(filters.remove_long_filter())
                    .filter(filters.lower_caser())
                    .filter(filters.stemmer())
                    .filter(filters.stopwords_language())
                    .filter(filters.stopwords())
                    .build(),
            ),
            SearchTokenizer::RegexTokenizer { pattern, filters } => Some(
                TextAnalyzer::builder(RegexTokenizer::new(pattern.as_str()).unwrap())
                    .filter(filters.remove_long_filter())
                    .filter(filters.lower_caser())
                    .filter(filters.stemmer())
                    .filter(filters.stopwords_language())
                    .filter(filters.stopwords())
                    .build(),
            ),
            SearchTokenizer::Ngram {
                min_gram,
                max_gram,
                prefix_only,
                filters,
            } => Some(
                TextAnalyzer::builder(
                    NgramTokenizer::new(*min_gram, *max_gram, *prefix_only)
                        .expect("Ngram parameters should be valid parameters for NgramTokenizer"),
                )
                .filter(filters.remove_long_filter())
                .filter(filters.lower_caser())
                .filter(filters.stemmer())
                .filter(filters.stopwords_language())
                .filter(filters.stopwords())
                .build(),
            ),
            SearchTokenizer::ChineseCompatible(filters) => Some(
                TextAnalyzer::builder(ChineseTokenizer)
                    .filter(filters.remove_long_filter())
                    .filter(filters.lower_caser())
                    .filter(filters.stemmer())
                    .filter(filters.stopwords_language())
                    .filter(filters.stopwords())
                    .build(),
            ),
            SearchTokenizer::SourceCode(filters) => Some(
                TextAnalyzer::builder(CodeTokenizer::default())
                    .filter(filters.remove_long_filter())
                    .filter(filters.lower_caser())
                    .filter(AsciiFoldingFilter)
                    .filter(filters.stemmer())
                    .filter(filters.stopwords_language())
                    .filter(filters.stopwords())
                    .build(),
            ),
            SearchTokenizer::ChineseLindera(filters) => Some(
                TextAnalyzer::builder(LinderaChineseTokenizer::default())
                    .filter(filters.remove_long_filter())
                    .filter(filters.lower_caser())
                    .filter(filters.stemmer())
                    .filter(filters.stopwords_language())
                    .filter(filters.stopwords())
                    .build(),
            ),
            SearchTokenizer::JapaneseLindera(filters) => Some(
                TextAnalyzer::builder(LinderaJapaneseTokenizer::default())
                    .filter(filters.remove_long_filter())
                    .filter(filters.lower_caser())
                    .filter(filters.stemmer())
                    .filter(filters.stopwords_language())
                    .filter(filters.stopwords())
                    .build(),
            ),
            SearchTokenizer::KoreanLindera(filters) => Some(
                TextAnalyzer::builder(LinderaKoreanTokenizer::default())
                    .filter(filters.remove_long_filter())
                    .filter(filters.lower_caser())
                    .filter(filters.stemmer())
                    .filter(filters.stopwords_language())
                    .filter(filters.stopwords())
                    .build(),
            ),
            // Deprecated, use `stemmer` filter instead
            SearchTokenizer::EnStem(filters) => Some(
                TextAnalyzer::builder(SimpleTokenizer::default())
                    .filter(filters.remove_long_filter())
                    .filter(filters.lower_caser())
                    .filter(Stemmer::new(Language::English))
                    .filter(filters.stopwords_language())
                    .filter(filters.stopwords())
                    .build(),
            ),
            // Deprecated, use `stemmer` filter instead
            SearchTokenizer::Stem { language, filters } => Some(
                TextAnalyzer::builder(SimpleTokenizer::default())
                    .filter(filters.remove_long_filter())
                    .filter(filters.lower_caser())
                    .filter(Stemmer::new(*language))
                    .filter(filters.stopwords_language())
                    .filter(filters.stopwords())
                    .build(),
            ),
            #[cfg(feature = "icu")]
            SearchTokenizer::ICUTokenizer(filters) => Some(
                TextAnalyzer::builder(ICUTokenizer)
                    .filter(filters.remove_long_filter())
                    .filter(filters.lower_caser())
                    .filter(filters.stemmer())
                    .filter(filters.stopwords_language())
                    .filter(filters.stopwords())
                    .build(),
            ),
        }
    }

    fn filters(&self) -> &SearchTokenizerFilters {
        match self {
            SearchTokenizer::Default(filters) => filters,
            SearchTokenizer::Raw(filters) => filters,
            SearchTokenizer::EnStem(filters) => filters,
            SearchTokenizer::Stem { filters, .. } => filters,
            SearchTokenizer::Lowercase(filters) => filters,
            SearchTokenizer::WhiteSpace(filters) => filters,
            SearchTokenizer::RegexTokenizer { filters, .. } => filters,
            SearchTokenizer::ChineseCompatible(filters) => filters,
            SearchTokenizer::SourceCode(filters) => filters,
            SearchTokenizer::Ngram { filters, .. } => filters,
            SearchTokenizer::ChineseLindera(filters) => filters,
            SearchTokenizer::JapaneseLindera(filters) => filters,
            SearchTokenizer::KoreanLindera(filters) => filters,
            #[cfg(feature = "icu")]
            SearchTokenizer::ICUTokenizer(filters) => filters,
        }
    }
}

pub fn language_to_str(lang: &Language) -> &str {
    match lang {
        Language::Arabic => "Arabic",
        Language::Danish => "Danish",
        Language::Dutch => "Dutch",
        Language::English => "English",
        Language::Finnish => "Finnish",
        Language::French => "French",
        Language::German => "German",
        Language::Greek => "Greek",
        Language::Hungarian => "Hungarian",
        Language::Italian => "Italian",
        Language::Norwegian => "Norwegian",
        Language::Portuguese => "Portuguese",
        Language::Romanian => "Romanian",
        Language::Russian => "Russian",
        Language::Spanish => "Spanish",
        Language::Swedish => "Swedish",
        Language::Tamil => "Tamil",
        Language::Turkish => "Turkish",
    }
}

impl SearchTokenizer {
    pub fn name(&self) -> String {
        let filters_suffix = self.filters().name_suffix();
        match self {
            SearchTokenizer::Default(_filters) => format!("default{filters_suffix}"),
            SearchTokenizer::Raw(_filters) => format!("raw{filters_suffix}"),
            SearchTokenizer::EnStem(_filters) => format!("en_stem{filters_suffix}"),
            SearchTokenizer::Stem {
                language,
                filters: _,
            } => {
                let language_suffix = language_to_str(language);
                format!("stem_{language_suffix}{filters_suffix}")
            }
            SearchTokenizer::Lowercase(_filters) => format!("lowercase{filters_suffix}"),
            SearchTokenizer::WhiteSpace(_filters) => format!("whitespace{filters_suffix}"),
            SearchTokenizer::RegexTokenizer { .. } => format!("regex{filters_suffix}"),
            SearchTokenizer::ChineseCompatible(_filters) => {
                format!("chinese_compatible{filters_suffix}")
            }
            SearchTokenizer::SourceCode(_filters) => format!("source_code{filters_suffix}"),
            SearchTokenizer::Ngram {
                min_gram,
                max_gram,
                prefix_only,
                filters: _,
            } => format!("ngram_mingram:{min_gram}_maxgram:{max_gram}_prefixonly:{prefix_only}{filters_suffix}"),
            SearchTokenizer::ChineseLindera(_filters) => format!("chinese_lindera{filters_suffix}"),
            SearchTokenizer::JapaneseLindera(_filters) => {
                format!("japanese_lindera{filters_suffix}")
            }
            SearchTokenizer::KoreanLindera(_filters) => format!("korean_lindera{filters_suffix}"),
            #[cfg(feature = "icu")]
            SearchTokenizer::ICUTokenizer(_filters) => format!("icu{filters_suffix}"),
        }
    }
}

// Normalizers for fast fields
#[derive(Default, Copy, Clone, Deserialize, Serialize, Debug, PartialEq, Eq)]
pub enum SearchNormalizer {
    #[serde(rename = "raw")]
    #[default]
    Raw,
    #[serde(rename = "lowercase")]
    Lowercase,
}

impl SearchNormalizer {
    pub fn name(&self) -> &str {
        match self {
            SearchNormalizer::Raw => "raw",
            SearchNormalizer::Lowercase => "lowercase",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    #[rstest]
    fn test_search_tokenizer() {
        let tokenizer = SearchTokenizer::default();
        assert_eq!(tokenizer.name(), "default".to_string());

        let tokenizer = SearchTokenizer::EnStem(SearchTokenizerFilters {
            remove_long: Some(999),
            lowercase: Some(true),
            stemmer: None,
            stopwords_language: None,
            stopwords: None,
        });
        assert_eq!(
            tokenizer.name(),
            "en_stem[remove_long=999,lowercase=true]".to_string()
        );

        let json = r#"{
            "type": "ngram",
            "min_gram": 20,
            "max_gram": 60,
            "prefix_only": true,
            "remove_long": 123,
            "lowercase": false
        }"#;

        let tokenizer =
            SearchTokenizer::from_json_value(&serde_json::from_str(json).unwrap()).unwrap();

        assert_eq!(
            tokenizer,
            SearchTokenizer::Ngram {
                min_gram: 20,
                max_gram: 60,
                prefix_only: true,
                filters: SearchTokenizerFilters {
                    remove_long: Some(123),
                    lowercase: Some(false),
                    stemmer: None,
                    stopwords_language: None,
                    stopwords: None,
                }
            }
        );
    }

    #[rstest]
    fn test_regexizer() {
        let json = r#"{
            "type": "regex",
            "pattern": "a+b*",
            "remove_long": 100
        }"#;
        let tokenizer = SearchTokenizer::RegexTokenizer {
            pattern: "a+b*".to_string(),
            filters: SearchTokenizerFilters {
                remove_long: Some(100),
                lowercase: None,
                stemmer: None,
                stopwords_language: None,
                stopwords: None,
            },
        };

        assert_eq!(
            tokenizer,
            SearchTokenizer::from_json_value(&serde_json::from_str(json).unwrap()).unwrap()
        );
    }

    #[rstest]
    fn test_search_normalizer() {
        assert_eq!(SearchNormalizer::Lowercase.name(), "lowercase");
        assert_ne!(SearchNormalizer::Raw, SearchNormalizer::Lowercase);
    }
}
