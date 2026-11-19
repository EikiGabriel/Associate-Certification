# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Bronze com Auto Loader
# MAGIC **Auto Loader** = `spark.readStream.format("cloudFiles")`. Descobre arquivos novos de forma incremental, guarda o
# MAGIC estado no **checkpoint** (exactly-once) e o schema inferido em **schemaLocation**.
# MAGIC
# MAGIC No serverless use `trigger(availableNow=True)`: processa tudo que está pendente e para (padrão "batch incremental").
# MAGIC
# MAGIC | | COPY INTO | Auto Loader |
# MAGIC |---|---|---|
# MAGIC | Interface | SQL batch | Structured Streaming |
# MAGIC | Estado | log da tabela Delta | checkpoint (RocksDB) |
# MAGIC | Escala | milhares de arquivos | milhões de arquivos |
# MAGIC | Schema novo | `mergeSchema` | `cloudFiles.schemaEvolutionMode` |

# COMMAND ----------

# Config
CATALOG = "cert_prep"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.landing.checkpoints")

ORIGEM = f"/Volumes/{CATALOG}/landing/raw/vendas"
CHK = f"/Volumes/{CATALOG}/landing/checkpoints/vendas_al"
TABELA = f"{CATALOG}.bronze.vendas_al"

from pyspark.sql import functions as F

def carregar():
    df = (spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("header", "true")
          .option("cloudFiles.inferColumnTypes", "true")       # sem isso, CSV/JSON viram tudo STRING
          .option("cloudFiles.schemaLocation", f"{CHK}/_schemas")
          .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # padrão
          .option("cloudFiles.schemaHints", "id_venda BIGINT, preco_unitario DECIMAL(10,2)")
          .load(ORIGEM)
          .withColumn("arquivo_origem", F.col("_metadata.file_path"))
          .withColumn("ingerido_em", F.current_timestamp()))
    q = (df.writeStream
         .option("checkpointLocation", CHK)
         .option("mergeSchema", "true")
         .trigger(availableNow=True)
         .toTable(TABELA))
    q.awaitTermination()
    print(q.lastProgress["numInputRows"] if q.lastProgress else 0, "linhas nesta execução")

def resumo():
    return spark.sql(f"""
        SELECT regexp_extract(arquivo_origem, '[^/]+$', 0) AS arquivo, count(*) AS linhas,
               count(_rescued_data) AS linhas_com_resgate
        FROM {TABELA} GROUP BY 1 ORDER BY 1""")

# COMMAND ----------

# MAGIC %md ## Cenário A — primeira carga (Passo 2)

# COMMAND ----------

# A1 👀 OBSERVE: linhas carregadas; rode a célula 2x — o que muda na 2ª vez?
carregar()

# COMMAND ----------

# A2 👀 OBSERVE: a coluna _rescued_data foi criada sozinha? Tipos de id_venda e preco_unitario (schemaHints)
display(resumo())
spark.table(TABELA).printSchema()

# COMMAND ----------

# A3 👀 OBSERVE: o que o Auto Loader guardou no checkpoint
display(dbutils.fs.ls(CHK))
display(dbutils.fs.ls(f"{CHK}/_schemas"))

# COMMAND ----------

# MAGIC %md ## Cenário B — coluna nova (Passo 3, depois do Lote 2)

# COMMAND ----------

# B1 👀 OBSERVE: a execução FALHA (UnknownFieldException). Leia a mensagem.
carregar()

# COMMAND ----------

# B2 rode de novo, sem mudar nada. 👀 OBSERVE: agora passa? Por quê? Coluna canal existe?
carregar()
display(resumo())

# COMMAND ----------

# MAGIC %md ## Cenário C — registros ruins (Passo 5, depois do Lote modificado e do Lote 3)

# COMMAND ----------

# C1 👀 OBSERVE: o arquivo vendas_lote1_002.csv (modificado) foi reprocessado? (opção cloudFiles.allowOverwrites)
carregar()
display(resumo())

# COMMAND ----------

# C2 👀 OBSERVE: o que foi parar em _rescued_data para cada linha ruim? Compare com o cenário D do notebook 02.
display(spark.sql(f"SELECT id_venda, quantidade, data_venda, id_cliente, _rescued_data FROM {TABELA} WHERE id_venda >= 401 ORDER BY id_venda"))

# COMMAND ----------

# MAGIC %md ## Reset (só se quiser recomeçar do zero)

# COMMAND ----------

# spark.sql(f"DROP TABLE IF EXISTS {TABELA}")
# dbutils.fs.rm(CHK, recurse=True)
