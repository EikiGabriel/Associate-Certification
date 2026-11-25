# Resumo para a prova — Databricks Data Engineer Associate

Foco: o que **cai em questão** e como reconhecer a alternativa certa. Baseado no que foi praticado no Projeto 1.

---

## 1. COPY INTO

**Definição de prova:** comando **SQL batch, idempotente**, que carrega arquivos de um local para uma tabela Delta.

| Fato | Detalhe |
|---|---|
| Idempotente | rodar 2x não duplica: os arquivos já carregados ficam registrados **no log da tabela Delta** |
| Controle por **arquivo** | o mesmo dado em um arquivo com **outro nome** entra de novo |
| Arquivo modificado com o mesmo nome | **não** é recarregado |
| Forçar recarga | `COPY_OPTIONS ('force' = 'true')` → **duplica** linhas (não substitui) |
| Tabela sem schema | `CREATE TABLE t;` (vazia) + `COPY_OPTIONS ('mergeSchema' = 'true')` na 1ª carga |
| Coluna nova sem `mergeSchema` | **erro**; os arquivos **não** ficam marcados como carregados |
| Coluna nova com `mergeSchema` | a coluna é adicionada; as linhas antigas ficam **NULL** nela |
| Escala ideal | **milhares** de arquivos (para milhões → Auto Loader) |

**Ordem das cláusulas** (a prova cobra a sintaxe):
```sql
COPY INTO tabela
FROM 'caminho'                       -- ou FROM (SELECT ... FROM 'caminho') para transformar/tipar
FILEFORMAT = CSV
VALIDATE ALL                         -- ou VALIDATE 10 ROWS: só valida, NÃO grava
PATTERN = '*.csv'                    -- ou FILES = ('a.csv','b.csv')
FORMAT_OPTIONS ('header'='true', 'inferSchema'='true')
COPY_OPTIONS ('mergeSchema'='true', 'force'='false');
```

**Pegadinhas**
- `VALIDATE` vem **logo após `FILEFORMAT`**. Ele **não carrega dados**: só verifica o schema e se as linhas podem ser lidas.
- `inferSchema` olha **só os arquivos da carga atual**. Um arquivo sujo muda o tipo inferido e gera o erro `Failed to merge fields`.
- Solução: tipar explicitamente com `FROM (SELECT try_cast(...) ...)`. O `try_cast` devolve **NULL** quando o valor é inválido e não falha.
- No COPY INTO, `_rescued_data` só existe se você pedir com `rescuedDataColumn`.

---

## 2. Auto Loader

**Definição de prova:** `spark.readStream.format("cloudFiles")`. Faz ingestão **incremental** de arquivos novos com garantia **exactly-once**.

| Opção | Para que serve |
|---|---|
| `cloudFiles.format` | formato do arquivo (csv, json, parquet…) |
| `checkpointLocation` (no `writeStream`) | guarda **quais arquivos já foram processados** (RocksDB) |
| `cloudFiles.schemaLocation` | guarda o **schema inferido** e suas versões |
| `cloudFiles.inferColumnTypes = true` | sem ele, **CSV e JSON viram tudo STRING** |
| `cloudFiles.schemaHints` | força o tipo de colunas específicas (sobrepõe a inferência) |
| `cloudFiles.schemaEvolutionMode` | o que fazer com uma coluna nova (tabela abaixo) |
| `cloudFiles.allowOverwrites` | padrão `false`: arquivo modificado com o mesmo nome **não** é reprocessado |
| `trigger(availableNow=True)` | processa o pendente e **para** (batch incremental; obrigatório no serverless) |
| `_metadata.file_path` | nome do arquivo de origem de cada linha |

**`schemaEvolutionMode`** (cai muito):

| Modo | Quando chega uma coluna nova |
|---|---|
| `addNewColumns` **(padrão)** | o stream **falha** (`UnknownFieldException`), grava o schema novo e **passa na próxima execução** |
| `rescue` | **nunca falha**; a coluna nova vai para `_rescued_data` |
| `failOnNewColumns` | falha até alguém atualizar o schema manualmente |
| `none` | ignora a coluna nova (é o padrão quando você passa `.schema(...)`) |

**Por que `addNewColumns` falha de propósito:** um stream roda com schema **fixo**. A única forma de trocar o schema é reiniciar o stream. Em produção, um **Job com retry** faz esse reinício sozinho.

**`_rescued_data`**
- O Auto Loader cria a coluna **automaticamente** quando infere o schema.
- Captura **só problemas de schema**: tipo errado (`"abc"` em uma coluna int) ou coluna desconhecida. O dado original não se perde.
- **Não** captura regra de negócio: nulo, valor negativo e duplicata entram normalmente.
- Linha com colunas a menos ou a mais: no modo PERMISSIVE, a coluna que falta vira NULL e a que sobra é descartada, sem resgate.

**Pegadinhas**
- Apagar o checkpoint faz o Auto Loader **reprocessar tudo** e duplicar a tabela.
- Cada stream precisa do **seu próprio** checkpoint.
- O Auto Loader **não deduplica**.

---

## 3. COPY INTO × Auto Loader (questão clássica)

| | COPY INTO | Auto Loader |
|---|---|---|
| Interface | SQL batch | Structured Streaming |
| Onde guarda o que já carregou | log da tabela Delta | checkpoint (RocksDB) |
| Escala | milhares de arquivos | **milhões** de arquivos |
| Schema novo | `mergeSchema` | `schemaEvolutionMode` |
| Resgate de dados | precisa de `rescuedDataColumn` | `_rescued_data` automático |
| Schema persistido | não (infere a cada carga) | sim (`schemaLocation`) |

**Como reconhecer na questão:** "milhões de arquivos", "contínuo", "near real-time" ou "evolução de schema automática" → **Auto Loader**. "Poucos arquivos", "SQL simples" ou "carga ad hoc idempotente" → **COPY INTO**.

---

## 4. Medalhão

| Camada | Regra |
|---|---|
| **Bronze** | dado cru, **append-only**, **não se filtra nada**. Guarda arquivo de origem e horário de ingestão |
| **Silver** | tipagem, **deduplicação**, regras de negócio, quarentena |
| **Gold** | agregações para consumo (BI, ML) |

- A regra de negócio (nulo, negativo, duplicata) vai para a **Silver**, nunca para a Bronze.
- A quarentena guarda o registro rejeitado junto com o **motivo**, para que nenhum dado se perca.

---

## 5. MERGE INTO

```sql
MERGE INTO destino t
USING (SELECT * FROM fonte QUALIFY row_number() OVER (PARTITION BY id ORDER BY ts DESC) = 1) s
ON t.id = s.id
WHEN MATCHED AND t.valor <> s.valor THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
WHEN NOT MATCHED BY SOURCE THEN DELETE;   -- opcional
```

| Fato | Detalhe |
|---|---|
| Fonte com chave duplicada que **casa** com o destino | **erro** `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` (não dá para saber qual UPDATE aplicar) |
| Fonte com chave duplicada que **não casa** | **sem erro**: insere as duas linhas (duplicata silenciosa) |
| Solução | **deduplicar a fonte** antes do MERGE |
| `WHEN MATCHED AND <condição>` | só atualiza o que mudou, e o MERGE fica **idempotente** |
| MERGE com 0 alterações | **mesmo assim cria uma versão nova** no histórico |
| Chave primária no Delta | **informativa**: não impede duplicata; quem garante unicidade é o pipeline |

---

## 6. Delta Lake: histórico, time travel e RESTORE

```sql
DESCRIBE HISTORY t;                          -- versão, operação, operationMetrics
SELECT * FROM t VERSION AS OF 3;
SELECT * FROM t TIMESTAMP AS OF '2026-11-20';
RESTORE TABLE t TO VERSION AS OF 0;
```

- **Toda escrita gera uma versão**, inclusive um MERGE que não altera nenhuma linha.
- `RESTORE` **cria uma versão nova** com o conteúdo antigo. Ele **não apaga** o histórico.
- `operationMetrics` é onde se audita o que cada operação fez (`numTargetRowsUpdated`, `numTargetRowsInserted`, `numOutputRows`…).
- **Deletion vectors**: no UPDATE, DELETE ou MERGE, o Delta **marca** as linhas como apagadas em vez de reescrever o arquivo Parquet. Isso aparece como `numTargetDeletionVectorsAdded` com `numTargetFilesRemoved = 0`.
- Isolamento padrão: **WriteSerializable**.
- `VACUUM` remove arquivos antigos e **limita o time travel** (retenção padrão de 7 dias).

---

## 7. Formas de escrever em uma tabela

| Comando | Schema | Dados | Histórico |
|---|---|---|---|
| `CREATE OR REPLACE TABLE ... AS` | substitui | substitui | **mantido** |
| `INSERT OVERWRITE` | precisa ser compatível | substitui | mantido |
| `INSERT INTO` | precisa ser compatível | acrescenta (**duplica** se rodar 2x) | mantido |
| `MERGE INTO` | precisa ser compatível | upsert/delete | mantido |
| `DROP` + `CREATE` | novo | novo | **perdido** |

**Pegadinha:** se a pergunta for "substituir os dados preservando o histórico/time travel", a resposta é `CREATE OR REPLACE` ou `INSERT OVERWRITE`, **nunca** `DROP` + `CREATE`.

---

## 8. View × tabela × materialized view

| Objeto | Guarda dados? | Vê dado novo da fonte |
|---|---|---|
| `VIEW` | não (só a query) | **na hora** (recalcula a cada consulta) |
| `TABLE` (CTAS) | sim (snapshot) | **nunca**, até você reescrever |
| `MATERIALIZED VIEW` | sim (pré-computado) | **no `REFRESH`** (manual ou agendado), incremental quando possível |

**Quando usar cada uma**
- Precisa de dado sempre atual e a consulta é barata → **VIEW**.
- Consulta pesada com muitos leitores (dashboard) → **MATERIALIZED VIEW**.
- Precisa de um snapshot controlado (fechamento mensal) → **TABLE**.

| Tipo de view | Persistida? | Visível para | Dura até |
|---|---|---|---|
| `VIEW` | sim (catálogo) | quem tem permissão | `DROP` |
| `TEMP VIEW` | não | só a sessão (notebook) | fim da sessão |
| `GLOBAL TEMP VIEW` | não (`global_temp`) | sessões do mesmo cluster | reinício do cluster |

---

## 9. Unity Catalog (o que já apareceu)

- Namespace de 3 níveis: `catalogo.schema.objeto`.
- **Volume** guarda arquivos não tabulares. O caminho é `/Volumes/<catalogo>/<schema>/<volume>/...`.
- **Managed**: o UC controla o armazenamento e `DROP` apaga os dados. **External**: `DROP` remove só o metadado.

---

## 10. Funções e tipos que confundem

| Função | Retorna |
|---|---|
| `date_trunc('MONTH', d)` | **timestamp** |
| `trunc(d, 'MM')` | **date** |
| `try_cast(x AS INT)` | NULL se for inválido (não falha) |
| `cast(x AS INT)` | erro se for inválido (com ANSI ligado) |
| `sum(decimal)` | decimal com **precisão maior** (ex.: `decimal(32,2)`) |
| `QUALIFY` | filtra pelo resultado de uma window function (ex.: dedup com `row_number() = 1`) |

---

## 11. Lakeflow Declarative Pipelines (= Delta Live Tables / DLT)

| Conceito | Resumo |
|---|---|
| `STREAMING TABLE` | append incremental; cada linha da fonte é processada **uma vez** |
| `MATERIALIZED VIEW` | resultado recalculado (incremental quando possível); reflete UPDATE e DELETE da fonte |
| `STREAM read_files(...)` | Auto Loader declarativo (checkpoint gerenciado pelo pipeline) |
| Expectation sem `ON VIOLATION` | **warn**: mantém a linha e registra a métrica |
| `ON VIOLATION DROP ROW` | descarta a linha |
| `ON VIOLATION FAIL UPDATE` | **derruba o update** inteiro |
| `AUTO CDC INTO` | aplica CDC: `KEYS`, `SEQUENCE BY`, `APPLY AS DELETE WHEN`, `STORED AS SCD TYPE 1/2` |
| SCD1 | só o estado atual; DELETE remove a linha |
| SCD2 | histórico com `__START_AT` e `__END_AT`; DELETE **fecha** a versão |
| DAG | inferido pelas referências entre tabelas; você não declara a ordem |
| Full refresh | apaga o estado e reprocessa tudo |

**Validado na prática (Passo 8)**
- Expectations só enxergam **colunas do SELECT** do dataset (erro `UNRESOLVED_COLUMN` se a coluna testada não estiver na saída).
- Aba **Data quality**: `DROP` mostra registros descartados; sem `ON VIOLATION` aparece como **ALLOW** (linha mantida, violação contada).
- `SEQUENCE BY` ordena pelo valor de sequência, **não pela ordem de chegada**: evento atrasado é encaixado na posição certa do histórico.
- SCD2: versão atual = `__END_AT IS NULL`; `__START_AT`/`__END_AT` têm o **tipo da coluna de sequência** (aqui, struct).
- DELETE: SCD1 **remove** a linha; SCD2 **fecha** a versão (preenche `__END_AT`).
- Empate de sequência na mesma chave é ambíguo → desempatar com `SEQUENCE BY STRUCT(col1, col2)`.
- Sintaxe antiga ainda em questões: `APPLY CHANGES INTO` (= `AUTO CDC INTO`), `LIVE.tabela`, `CREATE LIVE TABLE` / `CREATE STREAMING LIVE TABLE`.
- Pasta de código-fonte do pipeline: todo `.sql`/`.py` dentro (inclusive subpastas) entra no pipeline; a ordem dos arquivos não importa, o **DAG vem das referências**.
- Schema inferido na 1ª execução depende dos arquivos presentes; fixe tipos críticos com `schemaHints`.
