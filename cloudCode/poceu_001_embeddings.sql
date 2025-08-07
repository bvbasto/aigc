



CREATE OR REPLACE MODEL `bs-fdld-ai`.`rag_dataset_eu`.`embedding_multilingual_02`
REMOTE WITH CONNECTION `eu.eu_demo` OPTIONS(endpoint="text-multilingual-embedding-002")  -- "text-embedding-005";

CREATE OR REPLACE MODEL `bs-fdld-ai`.`rag_dataset_eu`.`text-embedding-005`
REMOTE WITH CONNECTION `eu.eu_demo` OPTIONS(endpoint="text-embedding-005")  -- "text-embedding-005";

CREATE OR REPLACE MODEL `bs-fdld-ai`.`rag_dataset_eu`.`gemini_25_pro` REMOTE
WITH CONNECTION `eu.eu_demo` OPTIONS(endpoint="gemini-2.5-pro");


--

/*
-- if direct chunk creation from bq
-- Create a new processor in Document AI with the type LAYOUT_PARSER_PROCESSOR

CREATE OR REPLACE MODEL `docai_demo.layout_parser` 
REMOTE WITH CONNECTION `us.demo_conn`
OPTIONS(remote_service_type="CLOUD_AI_DOCUMENT_V1", document_processor="{processor_id}")


CREATE or REPLACE TABLE docai_demo.demo_result AS (
  SELECT * FROM ML.PROCESS_DOCUMENT(
  MODEL docai_demo.layout_parser,
  TABLE docai_demo.object_table,
  PROCESS_OPTIONS => (JSON '{"layout_config": {"chunking_config": {"chunk_size": 250}}}')
  )
);

CREATE OR REPLACE TABLE docai_demo.demo_result_parsed AS (
SELECT
  uri,
  JSON_EXTRACT_SCALAR(json , '$.chunkId') AS id,
  JSON_EXTRACT_SCALAR(json , '$.content') AS content,
  JSON_EXTRACT_SCALAR(json , '$.pageFooters[0].text') AS page_footers_text,
  JSON_EXTRACT_SCALAR(json , '$.pageSpan.pageStart') AS page_span_start,
  JSON_EXTRACT_SCALAR(json , '$.pageSpan.pageEnd') AS page_span_end
FROM docai_demo.demo_result, UNNEST(JSON_EXTRACT_ARRAY(ml_process_document_result.chunkedDocument.chunks, '$')) json
);

*/


CREATE OR REPLACE TABLE `bs-fdld-ai`.`rag_dataset_eu`.`testFomCode_embeddings` AS
SELECT * FROM ML.GENERATE_EMBEDDING(
  MODEL `bs-fdld-ai`.`rag_dataset_eu`.`embedding_multilingual_02`,
  (select *, BlocoContent content from  `bs-fdld-ai`.`rag_dataset_eu`.`testFomCode`)
);




--select * from  `bs-fdld-ai`.`rag_dataset`.`testFomCode_embeddings`

SELECT  query.query, base.docname, base.BlocoID, base.BlocoMetadata, base.BlocoContent,base.BlocoType,base.docPage_ini , distance
    FROM
      VECTOR_SEARCH( TABLE `bs-fdld-ai`.`rag_dataset_eu`.`testFomCode_embeddings`,
        'ml_generate_embedding_result',
        (
        SELECT
          ml_generate_embedding_result,
          content AS query
        FROM
          ML.GENERATE_EMBEDDING( MODEL `bs-fdld-ai`.`rag_dataset_eu`.`embedding_multilingual_02`,
           -- ( SELECT 'Tendo seguro faz 3 anos, qual o agravamento se tiver tido 2 sinistros nos ultimos 5 anos mas nenhum nos ultimos 2 anos?' AS content)
           ( SELECT 'Como pode ser resolvido o contrato de seguro de saude?' AS content)
          ) 
        ),
        top_k => 10,
        OPTIONS => '{"fraction_lists_to_search": 0.01}') 
ORDER BY distance DESC;





SELECT
 -- ml_generate_text_llm_result AS generated,
 *
FROM
  ML.GENERATE_TEXT( MODEL `bs-fdld-ai`.`rag_dataset_eu`.`gemini_25_pro`,
    (
    SELECT
    CONCAT( 'Tendo seguro faz 3 anos, qual o agravamento se tiver tido 2 sinistros nos ultimos 5 anos mas nenhum nos ultimos 2 anos?',
    STRING_AGG(FORMAT("context: %s and reference: %s", base.BlocoContent, CAST(base.docPage_ini AS STRING) ), ',\n')) AS prompt,
    FROM
      VECTOR_SEARCH( TABLE `bs-fdld-ai`.`rag_dataset_eu`.`testFomCode_embeddings`,
        'ml_generate_embedding_result',
        (
        SELECT
          ml_generate_embedding_result,
          content AS query
        FROM
          ML.GENERATE_EMBEDDING( MODEL `bs-fdld-ai`.`rag_dataset_eu`.`embedding_multilingual_02`,
           -- ( SELECT 'Tendo seguro faz 3 anos, qual o agravamento se tiver tido 2 sinistros nos ultimos 5 anos mas nenhum nos ultimos 2 anos?' AS content)
           ( SELECT 'Como pode ser resolvido o contrato de seguro de saude?' AS content)
          ) 
        ),
        top_k => 10,
        OPTIONS => '{"fraction_lists_to_search": 0.01}') 
      ),
      STRUCT(35648 AS max_output_tokens, TRUE AS flatten_json_output)
  );








CREATE OR REPLACE VIEW `bs-fdld-ai`.`rag_dataset_eu`.`testFomCode_embeddings_view` AS
SELECT
  cast((t.docPage_ini * 1000 + t.BlocoID) as string) AS id,
  t.BlocoContent AS content, -- This will be the main text for your LLM context
  t.ml_generate_embedding_result AS embedding, -- Your existing embedding vector column
  JSON_OBJECT(
    'Docname', t.Docname,
    'BlocoID_PageIni', CONCAT(CAST(t.BlocoID AS STRING), '_', CAST(t.docPage_ini AS STRING)),
    'BlocoType', t.BlocoType,
    'BlocoMetadata', t.BlocoMetadata,
    'BlocoContent', SUBSTR(t.BlocoContent, 1, 250) -- Optional: add an excerpt if full content is too long for metadata
  ) AS metadata
FROM
  `bs-fdld-ai`.`rag_dataset_eu`.`testFomCode_embeddings` AS t
;


CREATE OR REPLACE TABLE `bs-fdld-ai`.`rag_dataset_eu`.`testFomCode_embeddings_materialized` AS
SELECT * from `bs-fdld-ai`.`rag_dataset_eu`.`testFomCode_embeddings_view`




SELECT * FROM `bs-fdld-ai`.`rag_dataset_eu`.`testFomCode_embeddings_view` LIMIT 10;


SELECT COUNT(*) FROM `bs-fdld-ai`.`rag_dataset_eu`.`testFomCode_embeddings_view` WHERE embedding IS NULL OR ARRAY_LENGTH(embedding) = 0;
SELECT COUNT(*) FROM `bs-fdld-ai`.`rag_dataset_eu`.`testFomCode_embeddings_view` WHERE content IS NULL OR content = '';
