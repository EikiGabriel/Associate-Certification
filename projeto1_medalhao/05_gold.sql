-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05 — Gold: view × tabela (CTAS) × materialized view
-- MAGIC Mesma agregação em 3 formas. O objetivo é ver **quando cada uma enxerga um dado novo** da Silver.
-- MAGIC
-- MAGIC | Objeto | Guarda dados? | Atualiza quando? |
-- MAGIC |---|---|---|
-- MAGIC | VIEW | não (só a query) | a cada consulta |
-- MAGIC | TABLE (CTAS) | sim | só se você reescrever |
-- MAGIC | MATERIALIZED VIEW | sim (pré-computado) | no `REFRESH` (manual ou agendado), incremental quando possível |

-- COMMAND ----------

USE CATALOG cert_prep;

-- COMMAND ----------

-- G1: view
CREATE OR REPLACE VIEW gold.v_vendas_mensais AS
SELECT date_trunc('MONTH', data_venda) AS mes, count(*) AS vendas, round(sum(valor_total), 2) AS receita
FROM silver.vendas GROUP BY 1;

-- COMMAND ----------

-- G2: tabela física (snapshot)
CREATE OR REPLACE TABLE gold.t_vendas_mensais AS
SELECT date_trunc('MONTH', data_venda) AS mes, count(*) AS vendas, round(sum(valor_total), 2) AS receita
FROM silver.vendas GROUP BY 1;

-- COMMAND ----------

-- G3: materialized view (roda em serverless; pode levar ~1 min na criação)
CREATE OR REPLACE MATERIALIZED VIEW gold.mv_vendas_mensais AS
SELECT date_trunc('MONTH', data_venda) AS mes, count(*) AS vendas, round(sum(valor_total), 2) AS receita
FROM silver.vendas GROUP BY 1;

-- COMMAND ----------

-- G4: 👀 OBSERVE: as 3 devem bater
SELECT 'view' AS obj, * FROM gold.v_vendas_mensais
UNION ALL SELECT 'tabela', * FROM gold.t_vendas_mensais
UNION ALL SELECT 'mv', * FROM gold.mv_vendas_mensais
ORDER BY mes, obj;

-- COMMAND ----------

-- G5: chega uma venda grande em janeiro na Silver
INSERT INTO silver.vendas (id_venda, data_venda, id_cliente, id_produto, quantidade, preco_unitario, canal, valor_total, ingerido_em)
VALUES (88888, DATE'2026-01-15', 1, 'P001', 1, 10000, 'loja', 10000, current_timestamp());

-- COMMAND ----------

-- G6: 👀 OBSERVE: qual das 3 já mostra a receita de janeiro +10000?
SELECT 'view' AS obj, * FROM gold.v_vendas_mensais WHERE mes = '2026-01-01'
UNION ALL SELECT 'tabela', * FROM gold.t_vendas_mensais WHERE mes = '2026-01-01'
UNION ALL SELECT 'mv', * FROM gold.mv_vendas_mensais WHERE mes = '2026-01-01';

-- COMMAND ----------

-- G7: atualiza a MV e repita o G6. 👀 OBSERVE: e a tabela?
REFRESH MATERIALIZED VIEW gold.mv_vendas_mensais;

-- COMMAND ----------

-- G8: 👀 OBSERVE: Type de cada objeto e, na MV, informações de refresh/pipeline
DESCRIBE EXTENDED gold.mv_vendas_mensais;

-- COMMAND ----------

-- MAGIC %md ## G9 — tipos de view (pergunta clássica)
-- MAGIC | | Persistida no catálogo? | Visível para | Dura até |
-- MAGIC |---|---|---|---|
-- MAGIC | `VIEW` | sim | quem tem permissão | `DROP` |
-- MAGIC | `TEMP VIEW` | não | só a sessão atual (notebook) | fim da sessão |
-- MAGIC | `GLOBAL TEMP VIEW` | não (`global_temp`) | todas as sessões do mesmo cluster | reinício do cluster (não existe no serverless) |
