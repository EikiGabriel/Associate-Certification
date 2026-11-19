-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 — Setup (Unity Catalog)
-- MAGIC Cria catálogo, schemas da arquitetura medalhão e o **volume** onde os arquivos brutos chegam.
-- MAGIC
-- MAGIC **Ponto de prova:** hierarquia de 3 níveis `catalog.schema.objeto`; *volumes* guardam arquivos não tabulares
-- MAGIC (`/Volumes/<catalog>/<schema>/<volume>/...`); tabelas **managed** × **external**.

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS cert_prep;
USE CATALOG cert_prep;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS landing COMMENT 'Arquivos brutos (volumes)';
CREATE SCHEMA IF NOT EXISTS bronze  COMMENT 'Dados crus, append-only';
CREATE SCHEMA IF NOT EXISTS silver  COMMENT 'Limpo, tipado, deduplicado';
CREATE SCHEMA IF NOT EXISTS gold    COMMENT 'Agregado para consumo';

-- COMMAND ----------

-- Volume managed: o Unity Catalog decide onde os arquivos ficam fisicamente
CREATE VOLUME IF NOT EXISTS landing.raw COMMENT 'Lotes de vendas e clientes';

-- COMMAND ----------

-- 👀 OBSERVE: tipo do volume (MANAGED) e o storage_location
DESCRIBE VOLUME landing.raw;

-- COMMAND ----------

SHOW SCHEMAS IN cert_prep;
