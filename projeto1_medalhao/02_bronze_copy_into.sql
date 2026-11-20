-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 — Bronze com COPY INTO
-- MAGIC **COPY INTO** = comando SQL *batch*, **idempotente**: guarda no log da tabela quais arquivos já carregou e os pula nas próximas execuções.
-- MAGIC Bom para milhares de arquivos; para milhões/streaming contínuo → Auto Loader (notebook 03).
-- MAGIC
-- MAGIC Rode os cenários na ordem do README (A no passo 1, B no passo 3, C no 4, D no 5).

-- COMMAND ----------

USE CATALOG cert_prep;

-- COMMAND ----------

-- MAGIC %md ## Cenário A — primeira carga e idempotência (Passo 1)

-- COMMAND ----------

-- A1: tabela Delta vazia SEM schema (placeholder). O schema vem da 1ª carga via mergeSchema.
CREATE TABLE IF NOT EXISTS bronze.vendas_copy;

-- COMMAND ----------

-- A2: primeira carga
-- 👀 OBSERVE: num_affected_rows / num_inserted_rows
COPY INTO bronze.vendas_copy
FROM '/Volumes/cert_prep/landing/raw/vendas'
FILEFORMAT = CSV
PATTERN = 'vendas_lote1_*.csv'
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------

-- A3: rode EXATAMENTE o mesmo comando de novo
-- 👀 OBSERVE: quantas linhas entraram agora? Por quê?
COPY INTO bronze.vendas_copy
FROM '/Volumes/cert_prep/landing/raw/vendas'
FILEFORMAT = CSV
PATTERN = 'vendas_lote1_*.csv'
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------

-- A4: conferência
-- 👀 OBSERVE: total de linhas, tipos inferidos de cada coluna, e a operação COPY INTO no histórico (operationMetrics)
SELECT count(*) AS linhas, count(DISTINCT id_venda) AS ids FROM bronze.vendas_copy;

-- COMMAND ----------

DESCRIBE TABLE bronze.vendas_copy;

-- COMMAND ----------

DESCRIBE HISTORY bronze.vendas_copy;

-- COMMAND ----------

-- MAGIC %md ## Cenário B — evolução de schema (Passo 3, depois do Lote 2)

-- COMMAND ----------

-- B1: carga do lote 2 SEM mergeSchema
-- 👀 OBSERVE: deu erro? Ou carregou e a coluna `canal` sumiu?
COPY INTO bronze.vendas_copy
FROM '/Volumes/cert_prep/landing/raw/vendas'
FILEFORMAT = CSV
PATTERN = 'vendas_lote2_*.csv'
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true');

-- COMMAND ----------

-- B2: agora COM mergeSchema
-- 👀 OBSERVE: se B1 carregou, quantas linhas entram aqui? (dica: idempotência vale mesmo trocando opções)
COPY INTO bronze.vendas_copy
FROM '/Volumes/cert_prep/landing/raw/vendas'
FILEFORMAT = CSV
PATTERN = 'vendas_lote2_*.csv'
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------

-- B3: 👀 OBSERVE: a coluna canal existe? Está preenchida para os ids 301–400?
SELECT canal, count(*) FROM bronze.vendas_copy GROUP BY canal;

-- COMMAND ----------

-- MAGIC %md ## Cenário C — arquivo modificado com o mesmo nome (Passo 4)

-- COMMAND ----------

-- C1: recarga normal do lote 1 (o arquivo 002 foi reescrito)
-- 👀 OBSERVE: o arquivo modificado foi recarregado?
COPY INTO bronze.vendas_copy
FROM '/Volumes/cert_prep/landing/raw/vendas'
FILEFORMAT = CSV
PATTERN = 'vendas_lote1_*.csv'
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------

-- C2: força a recarga só do arquivo 002
COPY INTO bronze.vendas_copy
FROM '/Volumes/cert_prep/landing/raw/vendas'
FILEFORMAT = CSV
PATTERN = 'vendas_lote1_002.csv'
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true', 'force' = 'true');

-- COMMAND ----------

-- C3: 👀 OBSERVE: quantos ids aparecem 2x? Que preço cada versão tem? (isso é trabalho da Silver resolver)
SELECT id_venda, count(*) AS vezes, collect_list(preco_unitario) AS precos
FROM bronze.vendas_copy
GROUP BY id_venda HAVING count(*) > 1
ORDER BY id_venda LIMIT 10;

-- COMMAND ----------

-- MAGIC %md ## Cenário D — registros ruins (Passo 5, depois do Lote 3)

-- COMMAND ----------

-- D1: VALIDATE só verifica, NÃO carrega nada
-- 👀 OBSERVE: o que é reportado? A tabela mudou de tamanho?
COPY INTO bronze.vendas_copy
FROM '/Volumes/cert_prep/landing/raw/vendas'
FILEFORMAT = CSV
VALIDATE ALL                       -- a ordem importa: VALIDATE vem logo após FILEFORMAT
PATTERN = 'vendas_lote3_*.csv'
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true');

-- COMMAND ----------

-- D2: carga com coluna de resgate (dados que não cabem no schema vão para _rescued_data em vez de sumir)
COPY INTO bronze.vendas_copy
FROM '/Volumes/cert_prep/landing/raw/vendas'
FILEFORMAT = CSV
PATTERN = 'vendas_lote3_*.csv'
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'rescuedDataColumn' = '_rescued_data')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------

-- D3: 👀 OBSERVE: o que aconteceu com cada linha ruim (402–407 e a duplicata 401)?
SELECT * FROM bronze.vendas_copy WHERE id_venda >= 401 ORDER BY id_venda;
