# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Gerador de lotes sintéticos
# MAGIC Cada lote simula a chegada de arquivos novos no volume. Rode **só a célula do lote pedido** no passo atual.
# MAGIC
# MAGIC | Lote | Conteúdo | Problema proposital |
# MAGIC |---|---|---|
# MAGIC | 1 | 3 arquivos CSV de vendas + clientes JSON (CDC) | nenhum |
# MAGIC | 2 | 2 CSV com coluna nova `canal` + CDC de clientes | **evolução de schema** |
# MAGIC | modificado | reescreve `vendas_lote1_002.csv` com preços diferentes | **arquivo com mesmo nome** |
# MAGIC | 3 | 1 CSV com tipos errados, nulos, duplicatas, negativos | **registros ruins** |

# COMMAND ----------

# Config
import random, json
from datetime import date, timedelta

CATALOG = "cert_prep"
BASE = f"/Volumes/{CATALOG}/landing/raw"
VENDAS = f"{BASE}/vendas"
CLIENTES = f"{BASE}/clientes"

for p in (VENDAS, CLIENTES):
    dbutils.fs.mkdirs(p)

HEADER = "id_venda,data_venda,id_cliente,id_produto,quantidade,preco_unitario"

def linha_venda(id_venda, rnd, canal=None):
    d = date(2026, 1, 1) + timedelta(days=rnd.randint(0, 89))
    campos = [id_venda, d.isoformat(), rnd.randint(1, 50), f"P{rnd.randint(1, 20):03d}",
              rnd.randint(1, 5), f"{rnd.uniform(5, 500):.2f}"]
    if canal is not None:
        campos.append(canal)
    return ",".join(str(c) for c in campos)

def escrever(caminho, linhas):
    dbutils.fs.put(caminho, "\n".join(linhas) + "\n", overwrite=True)
    print(f"✔ {caminho} ({len(linhas) - 1} linhas de dados)")

def escrever_cdc(caminho, eventos):
    dbutils.fs.put(caminho, "\n".join(json.dumps(e) for e in eventos) + "\n", overwrite=True)
    print(f"✔ {caminho} ({len(eventos)} eventos)")

def listar():
    for p in (VENDAS, CLIENTES):
        for f in dbutils.fs.ls(p):
            print(f.path, f.size, f.modificationTime)

# COMMAND ----------

# MAGIC %md ## Lote 1

# COMMAND ----------

rnd = random.Random(1)
id_venda = 1
for n in range(1, 4):
    linhas = [HEADER]
    for _ in range(100):
        linhas.append(linha_venda(id_venda, rnd)); id_venda += 1
    escrever(f"{VENDAS}/vendas_lote1_{n:03d}.csv", linhas)

eventos = [{"id_cliente": i, "nome": f"Cliente {i}", "cidade": rnd.choice(["SP", "RJ", "BH", "POA"]),
            "operacao": "INSERT", "seq": 1} for i in range(1, 51)]
escrever_cdc(f"{CLIENTES}/clientes_lote1.json", eventos)
listar()

# COMMAND ----------

# MAGIC %md ## Lote 2 — coluna nova `canal`

# COMMAND ----------

rnd = random.Random(2)
id_venda = 301
for n in range(1, 3):
    linhas = [HEADER + ",canal"]
    for _ in range(50):
        linhas.append(linha_venda(id_venda, rnd, rnd.choice(["loja", "site", "app"]))); id_venda += 1
    escrever(f"{VENDAS}/vendas_lote2_{n:03d}.csv", linhas)

eventos = (
    [{"id_cliente": i, "nome": f"Cliente {i}", "cidade": "Curitiba", "operacao": "UPDATE", "seq": 2} for i in range(1, 6)]
    + [{"id_cliente": i, "nome": None, "cidade": None, "operacao": "DELETE", "seq": 2} for i in (49, 50)]
    + [{"id_cliente": 51, "nome": "Cliente 51", "cidade": "SP", "operacao": "INSERT", "seq": 1}]
    # evento fora de ordem: seq menor chega depois -> AUTO CDC deve ignorar
    + [{"id_cliente": 1, "nome": "Cliente 1", "cidade": "SP-antigo", "operacao": "UPDATE", "seq": 1}]
)
escrever_cdc(f"{CLIENTES}/clientes_lote2.json", eventos)
listar()

# COMMAND ----------

# MAGIC %md ## Lote modificado — mesmo nome de arquivo, conteúdo novo

# COMMAND ----------

rnd = random.Random(99)
linhas = [HEADER] + [linha_venda(i, rnd) for i in range(101, 201)]  # mesmos ids do arquivo 002, preços novos
escrever(f"{VENDAS}/vendas_lote1_002.csv", linhas)
listar()  # 👀 OBSERVE: modificationTime do arquivo 002 mudou

# COMMAND ----------

# MAGIC %md ## Lote 3 — registros ruins

# COMMAND ----------

linhas = [HEADER,
    "401,2026-03-01,10,P001,2,19.90",          # ok
    "402,2026-03-01,11,P002,abc,25.00",        # quantidade não numérica
    "403,01/03/2026,12,P003,1,30.00",          # data em formato errado
    "404,2026-03-02,,P004,1,40.00",            # id_cliente nulo
    "405,2026-03-02,13,P005,-3,50.00",         # quantidade negativa
    "401,2026-03-01,10,P001,2,19.90",          # duplicata exata
    "406,2026-03-03,14,P006,1",                # coluna faltando
    "407,2026-03-03,15,P007,1,12.50,extra",    # coluna sobrando
]
escrever(f"{VENDAS}/vendas_lote3_001.csv", linhas)
listar()
