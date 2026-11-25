# Projeto 1 — Resultados e métricas

Pipeline medalhão de vendas no Databricks Free Edition (serverless), catálogo `cert_prep`.
Período: 19–25/11/2026. **Status: concluído.**

## Resumo
- Ingestão incremental praticada nas duas formas: **COPY INTO** (SQL batch) e **Auto Loader** (streaming com `availableNow`).
- Testados de propósito: evolução de schema, arquivo modificado com o mesmo nome, registros ruins e duplicatas.
- Silver com deduplicação, quarentena e MERGE idempotente. Gold comparando view, tabela e materialized view.
- Mesmo medalhão reconstruído como **Lakeflow Declarative Pipeline**, com expectations e AUTO CDC (SCD1 e SCD2).
- **Questões do Passo 9: 4/5 (80%).** O simulado anterior foi 73,3%, com Ingestão em 67%.

## Métricas por passo

### Passo 1 — COPY INTO, primeira carga
| Medida | Valor | Conclusão |
|---|---|---|
| A2: linhas carregadas | **300** | 3 arquivos × 100 linhas |
| A3: mesma carga de novo | **0** | idempotente: os arquivos ficam registrados no log Delta |
| Total / ids distintos | 300 / 300 | nenhuma duplicata |

### Passo 2 — Auto Loader, primeira carga
| Medida | Valor | Conclusão |
|---|---|---|
| Linhas por arquivo (lote 1) | 100 / 100 / 100 | 2ª execução não duplicou nada (estado no checkpoint) |
| Tipos | `id_venda` long, `preco_unitario` decimal(10,2) (vieram dos `schemaHints`); `data_venda` date, `quantidade` int (vieram do `inferColumnTypes`) | sem `inferColumnTypes`, CSV vira tudo string |
| `_rescued_data` | criada automaticamente | o Auto Loader cria a coluna quando infere o schema |
| Checkpoint | `offsets/`, `commits/`, `sources/`, `_schemas/`, `metadata` | `sources` = arquivos já vistos (RocksDB) |

### Passo 3 — Evolução de schema (coluna `canal`)
| Medida | Valor | Conclusão |
|---|---|---|
| COPY INTO sem `mergeSchema` | **erro** | os arquivos não ficaram marcados como carregados |
| COPY INTO com `mergeSchema` | **100** linhas | a coluna foi adicionada |
| `canal`: null / loja / site / app | 300 / 39 / 33 / 28 | linhas antigas ficam NULL na coluna nova |
| Auto Loader B1 / B2 | falhou / passou | `addNewColumns`: falha de propósito e segue ao reiniciar |

### Passos 4–5 — Arquivo modificado e registros ruins
| Medida | Valor | Conclusão |
|---|---|---|
| Auto Loader: `vendas_lote1_002.csv` depois de modificado | continuou com **100** linhas | `allowOverwrites = false` (padrão) |
| COPY INTO `VALIDATE` | primeiro erro de sintaxe, depois `Failed to merge fields 'data_venda'` | `VALIDATE` vem logo após `FILEFORMAT`; `inferSchema` do lote sujo gerou STRING |
| COPY INTO D2 | reescrito com `SELECT try_cast(...)` | tipagem explícita; valor inválido vira NULL |
| Auto Loader, lote 3 | 8 linhas, **2** com `_rescued_data` (402 `abc`, 403 `01/03/2026`) | resgata só problemas de schema |
| 404 (cliente nulo), 405 (`-3`), 401 (duplicata), 406/407 (colunas a menos/a mais) | entraram normalmente | regra de negócio é papel da Silver |

### Passo 6 — Silver
| Medida | Valor | Conclusão |
|---|---|---|
| `silver.vendas` (CTAS v0) | **403** linhas | 408 da bronze − 4 em quarentena − 1 duplicata |
| MERGE com fonte duplicada | `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` | é preciso deduplicar a fonte |
| MERGE deduplicado (v1) | 2 atualizadas, 1 inserida, **1 deletion vector**, 0 arquivos removidos | deletion vectors evitam reescrever o arquivo |
| MERGE repetido (v2) | 0 linhas alteradas, **mas criou versão nova** | toda escrita gera commit |

### Passo 7 — Gold (janeiro, depois de inserir R$ 10.000 na Silver)
| Objeto | Vendas | Receita | Conclusão |
|---|---|---|---|
| VIEW | 130 | 107.931,88 | atualizada na hora |
| MATERIALIZED VIEW (depois do `REFRESH`) | 130 | 107.931,88 | atualizada no refresh |
| TABLE (CTAS) | 129 | 97.931,88 | snapshot, não muda |

### Passo 8 — Lakeflow Declarative Pipeline
| Expectation | Ação | Falhas |
|---|---|---|
| `quantidade_ok` | DROP | 2 (0,5%) |
| `cliente_ok` | DROP | 1 (0,2%) |
| `preco_ok` | ALLOW (warn) | 1 (0,2%) — linha 406 mantida |
| `schema_ok` | DROP | **0** — ver achado abaixo |

**Achado:** com o lote 3 já presente na 1ª execução, `read_files` inferiu `data_venda` e `quantidade` como **STRING**. Nada foi resgatado e a linha 403 (`01/03/2026`) chegou à Silver como texto. Correção: `schemaHints` com `data_venda DATE` e `quantidade INT`, seguido de full refresh.

| AUTO CDC | Resultado |
|---|---|
| SCD1 | cliente 1 = **Curitiba** (maior `seq`); clientes 49 e 50 **removidos** |
| SCD2, cliente 1 | 3 versões na ordem da sequência: SP → SP-antigo → Curitiba (atual, `__END_AT` null) |
| SCD2, cliente 49 | versão **fechada** (`__END_AT` preenchido), não removida |

### Passo 9 — Questões
**4/5.** Erro: escolher COPY INTO para "milhões de arquivos contínuos com schema mudando", quando o certo é Auto Loader.

## Erros encontrados no caminho (todos viram questão)
1. `VALIDATE` no fim do COPY INTO → erro de sintaxe. Ele vem logo após `FILEFORMAT`.
2. `inferSchema` em um lote sujo → conflito de tipo com a tabela. Solução: `try_cast` explícito.
3. Expectation testando uma coluna fora do `SELECT` → `UNRESOLVED_COLUMN`.
4. Inferência de schema no pipeline com dado sujo → coluna STRING e expectation inútil. Solução: `schemaHints`.
5. Arquivos `.sql` duplicados na pasta de código-fonte → tabela definida duas vezes.

## Pendências (opcionais)
- Full refresh do pipeline com os novos `schemaHints` (esperado: `schema_ok` descartar 2 linhas).
- COPY INTO C1–C3 e D3 sem resultado registrado.
- Confirmar no histórico que o `RESTORE` criou a versão 3.
