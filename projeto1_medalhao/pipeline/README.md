# Passo 8 — Lakeflow Declarative Pipeline

## Criar o pipeline
1. **Jobs & Pipelines → Create → ETL pipeline**.
2. Nome: `vendas_medalhao`. Catálogo padrão: `cert_prep`. Schema padrão: crie `lakeflow`.
3. Código-fonte: adicione os 3 arquivos desta pasta (`01_bronze.sql`, `02_silver.sql`, `03_gold.sql`) do Git folder.
4. **Run pipeline** (não full refresh).

## O que observar
| Onde | 👀 OBSERVE | Ponto de prova |
|---|---|---|
| Grafo | ordem das tabelas — você não declarou dependências | o pipeline infere o DAG pelas referências |
| `silver_vendas` → Data quality | registros descartados por constraint; `preco_ok` com violação mas sem drop | warn × DROP ROW × FAIL UPDATE |
| `silver_vendas` | o id 401 duplicado sobreviveu? | streaming table é append; expectation não deduplica |
| `silver_clientes_scd1` | clientes 49 e 50; cidade do cliente 1 | SCD1 aplica DELETE; SEQUENCE BY ordena eventos fora de ordem |
| `silver_clientes_scd2` | linhas do cliente 1 e `__START_AT`/`__END_AT`; 49 e 50 | SCD2 guarda histórico; DELETE fecha a versão |
| 2ª execução sem mudar nada | linhas processadas por tabela | ST processa só o novo; MV pode recomputar |

## Experimentos depois
- Troque `quantidade_ok` para `ON VIOLATION FAIL UPDATE` e rode: o que acontece com o update?
- Rode **Full refresh** em `bronze_vendas`: o que acontece com o estado (checkpoint)?
