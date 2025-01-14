DROP FUNCTION IF EXISTS tokenizer(name text, remove_long pg_catalog.int4, lowercase bool, min_gram pg_catalog.int4, max_gram pg_catalog.int4, prefix_only bool, language text, pattern text, stemmer text);
CREATE OR REPLACE FUNCTION tokenizer(
    name text,
    remove_long pg_catalog.int4 DEFAULT '255',
    lowercase bool DEFAULT '(('t')::pg_catalog.bool)',
    min_gram pg_catalog.int4 DEFAULT NULL,
    max_gram pg_catalog.int4 DEFAULT NULL,
    prefix_only bool DEFAULT NULL,
    language text DEFAULT NULL,
    pattern text DEFAULT NULL,
    stemmer TEXT DEFAULT NULL,
    stopwords_language text DEFAULT NULL,
    stopwords text[] DEFAULT NULL
) RETURNS jsonb
AS 'MODULE_PATHNAME', 'tokenizer_wrapper' IMMUTABLE LANGUAGE c PARALLEL SAFE;
