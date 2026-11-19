# Projeto 1 — Pipeline medalhão de vendas

Objetivo: dominar **ingestão incremental** (COPY INTO × Auto Loader), **evolução de schema**, **registros ruins**, e a camada **Silver/Gold** com **Lakeflow Declarative Pipelines**.

Catálogo usado: `cert_prep` (se a Free Edition não deixar criar catálogo, troque por `workspace` em todos os arquivos).

| Passo | Notebook / ação | O que observar | Ponto de prova |
|---|---|---|---|
| 1 | `00_setup` (Run all) → `01_gerar_lotes` (Config + **Lote 1**) → `02_bronze_copy_into` células **A1–A4** | contagens, `num_affected_rows`, reexecução = 0 linhas | COPY INTO é **idempotente** (rastreia arquivos já carregados) |
| 2 | `03_bronze_autoloader` cenário **A** | `_rescued_data`, `_metadata.file_path`, pasta `_schemas` | Auto Loader: `cloudFiles`, `schemaLocation`, `checkpointLocation`, `availableNow` |
| 3 | `01` **Lote 2** (coluna nova `canal`) → `02` cenário **B** → `03` cenário **B** | erro/ausência da coluna sem `mergeSchema`; stream que falha e volta | `mergeSchema` (COPY INTO) × `schemaEvolutionMode = addNewColumns` (Auto Loader) |
| 4 | `01` **Lote modificado** → `02` cenário **C** | arquivo mesmo nome não é recarregado; `force = true` duplica | idempotência por arquivo; `force` |
| 5 | `01` **Lote 3** (registros ruins) → `02` cenário **D** → `03` cenário **C** | `VALIDATE`, `_rescued_data`, linhas com tipo errado | `rescuedDataColumn`, `badRecordsPath`, `VALIDATE` |
| 6 | `04_silver.sql` (em breve) | dedup, tipagem, `MERGE INTO` | MERGE, window functions, CTAS × INSERT OVERWRITE |
| 7 | `05_gold.sql` (em breve) | agregações, views × MVs | views, materialized views |
| 8 | `pipeline/` (em breve) | streaming table × MV, expectations, `AUTO CDC INTO` SCD1/SCD2 | Lakeflow Declarative Pipelines |
| 9 | 5 questões estilo prova | — | revisão |

Ao fim de cada passo, mande os resultados das células `👀 OBSERVE`.
