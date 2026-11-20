-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 — Silver: limpeza, deduplicação, quarentena e MERGE
-- MAGIC Fonte: `bronze.vendas_al` (Auto Loader). Regras de negócio:
-- MAGIC `_rescued_data IS NULL`, `id_cliente IS NOT NULL`, `quantidade > 0`, `data_venda IS NOT NULL`.
-- MAGIC Quem viola vai para **quarentena** (não se perde dado).

-- COMMAND ----------

USE CATALOG cert_prep;

-- COMMAND ----------

-- MAGIC %md ## S1 — CTAS com deduplicação (carga inicial)

-- COMMAND ----------

-- 👀 OBSERVE: quantas linhas? (bronze tinha 408)
CREATE OR REPLACE TABLE silver.vendas AS
SELECT id_venda, data_venda, id_cliente, id_produto, quantidade, preco_unitario, canal,
       quantidade * preco_unitario AS valor_total, ingerido_em
FROM bronze.vendas_al
WHERE _rescued_data IS NULL AND id_cliente IS NOT NULL AND quantidade > 0 AND data_venda IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY id_venda ORDER BY ingerido_em DESC) = 1;

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.vendas_quarentena AS
SELECT *,
       CASE WHEN _rescued_data IS NOT NULL THEN 'schema'
            WHEN id_cliente IS NULL THEN 'cliente_nulo'
            WHEN coalesce(quantidade, 0) <= 0 THEN 'quantidade_invalida'
            WHEN data_venda IS NULL THEN 'data_nula' END AS motivo
FROM bronze.vendas_al
WHERE _rescued_data IS NOT NULL OR id_cliente IS NULL OR coalesce(quantidade, 0) <= 0 OR data_venda IS NULL;

-- COMMAND ----------

-- 👀 OBSERVE: contagem silver, contagem por motivo na quarentena
SELECT 'silver' AS t, count(*) FROM silver.vendas
UNION ALL SELECT concat('quarentena:', motivo), count(*) FROM silver.vendas_quarentena GROUP BY motivo;

-- COMMAND ----------

-- MAGIC %md ## S2 — MERGE incremental (upsert)
-- MAGIC Simula um novo lote de correções chegando: preço do id 1 muda, id 9999 é novo, e o id 5 vem **duplicado** na fonte.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW novos AS
SELECT * FROM VALUES
  (1,    DATE'2026-01-10', 7, 'P001', 2, 999.99D, 'site'),
  (5,    DATE'2026-01-11', 8, 'P002', 1, 10.00D,  'app'),
  (5,    DATE'2026-01-11', 8, 'P002', 1, 11.00D,  'app'),
  (9999, DATE'2026-03-31', 9, 'P003', 3, 20.00D,  'loja')
AS t(id_venda, data_venda, id_cliente, id_produto, quantidade, preco_unitario, canal);

-- COMMAND ----------

-- S2a 👀 OBSERVE: o MERGE falha. Leia o erro: por quê?
MERGE INTO silver.vendas AS t
USING novos AS s
ON t.id_venda = s.id_venda
WHEN MATCHED THEN UPDATE SET
  t.preco_unitario = s.preco_unitario, t.quantidade = s.quantidade,
  t.valor_total = s.quantidade * s.preco_unitario, t.ingerido_em = current_timestamp()
WHEN NOT MATCHED THEN INSERT
  (id_venda, data_venda, id_cliente, id_produto, quantidade, preco_unitario, canal, valor_total, ingerido_em)
  VALUES (s.id_venda, s.data_venda, s.id_cliente, s.id_produto, s.quantidade, s.preco_unitario, s.canal,
          s.quantidade * s.preco_unitario, current_timestamp());

-- COMMAND ----------

-- S2b: deduplica a FONTE antes do MERGE
-- 👀 OBSERVE: num_updated_rows, num_inserted_rows
MERGE INTO silver.vendas AS t
USING (SELECT * FROM novos QUALIFY row_number() OVER (PARTITION BY id_venda ORDER BY preco_unitario DESC) = 1) AS s
ON t.id_venda = s.id_venda
WHEN MATCHED AND t.preco_unitario <> s.preco_unitario THEN UPDATE SET
  t.preco_unitario = s.preco_unitario, t.quantidade = s.quantidade,
  t.valor_total = s.quantidade * s.preco_unitario, t.ingerido_em = current_timestamp()
WHEN NOT MATCHED THEN INSERT
  (id_venda, data_venda, id_cliente, id_produto, quantidade, preco_unitario, canal, valor_total, ingerido_em)
  VALUES (s.id_venda, s.data_venda, s.id_cliente, s.id_produto, s.quantidade, s.preco_unitario, s.canal,
          s.quantidade * s.preco_unitario, current_timestamp());

-- COMMAND ----------

-- S2c: rode o S2b de novo. 👀 OBSERVE: quantas linhas afetadas agora? (MERGE bem escrito = idempotente)

-- COMMAND ----------

-- MAGIC %md ## S3 — Time travel e RESTORE

-- COMMAND ----------

-- 👀 OBSERVE: uma versão por operação (CREATE OR REPLACE, MERGE...)
DESCRIBE HISTORY silver.vendas;

-- COMMAND ----------

-- preço do id 1 antes e depois do MERGE (troque 0 pela versão anterior ao MERGE se precisar)
SELECT 'v0' AS versao, preco_unitario FROM silver.vendas VERSION AS OF 0 WHERE id_venda = 1
UNION ALL
SELECT 'atual', preco_unitario FROM silver.vendas WHERE id_venda = 1;

-- COMMAND ----------

-- desfaz o MERGE. 👀 OBSERVE: RESTORE cria uma versão NOVA (não apaga o histórico)
RESTORE TABLE silver.vendas TO VERSION AS OF 0;

-- COMMAND ----------

DESCRIBE HISTORY silver.vendas;

-- COMMAND ----------

-- MAGIC %md ## S4 — Formas de escrever (pergunta clássica)
-- MAGIC | Comando | Schema | Dados | Histórico |
-- MAGIC |---|---|---|---|
-- MAGIC | `CREATE OR REPLACE TABLE ... AS` | substitui | substitui | **mantido** (nova versão) |
-- MAGIC | `INSERT OVERWRITE` | precisa ser compatível | substitui | mantido |
-- MAGIC | `INSERT INTO` | precisa ser compatível | acrescenta (duplica se rodar 2x) | mantido |
-- MAGIC | `MERGE INTO` | precisa ser compatível | upsert/delete | mantido |
-- MAGIC | `DROP` + `CREATE` | novo | novo | **perdido** |
