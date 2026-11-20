-- Lakeflow Declarative Pipeline — BRONZE
-- STREAMING TABLE + STREAM read_files = Auto Loader declarativo (checkpoint e schemaLocation são gerenciados pelo pipeline)

CREATE OR REFRESH STREAMING TABLE bronze_vendas
COMMENT 'Vendas cruas (CSV) via Auto Loader'
AS SELECT *, _metadata.file_path AS arquivo_origem
FROM STREAM read_files(
  '/Volumes/cert_prep/landing/raw/vendas',
  format => 'csv',
  header => true,
  schemaHints => 'id_venda BIGINT, preco_unitario DECIMAL(10,2)'
);

CREATE OR REFRESH STREAMING TABLE bronze_clientes_cdc
COMMENT 'Feed CDC de clientes (JSON)'
AS SELECT *, _metadata.file_modification_time AS arquivo_ts
FROM STREAM read_files('/Volumes/cert_prep/landing/raw/clientes', format => 'json');
