# Banco de questões — Data Engineer Associate

Estilo da prova. Tente responder antes de abrir **Resposta**.
Projeto 1: Ingestão, Delta Lake, Silver/Gold e Lakeflow Declarative Pipelines.

---

## Ingestão: COPY INTO

**1.** O mesmo `COPY INTO` é executado duas vezes sobre a mesma pasta, sem arquivos novos. O que acontece na 2ª execução?
A) Duplica todas as linhas  B) Falha  C) Não carrega nenhuma linha  D) Substitui as linhas
<details><summary>Resposta</summary>

**C.** O COPY INTO é idempotente: registra no log da tabela Delta os arquivos já carregados e os ignora depois.
</details>

**2.** Um engenheiro precisa recarregar um arquivo que o COPY INTO já processou. O que ele deve usar, e qual é a consequência?
A) `VALIDATE ALL`; substitui as linhas  B) `COPY_OPTIONS ('force'='true')`; as linhas são **duplicadas**  C) `mergeSchema`; substitui  D) Não é possível
<details><summary>Resposta</summary>

**B.** `force = true` ignora o controle de idempotência e acrescenta as linhas de novo. Não há substituição.
</details>

**3.** O que `COPY INTO ... VALIDATE ALL` faz?
A) Carrega só as linhas válidas  B) Valida o schema e o parse **sem gravar dados**  C) Carrega tudo e reporta os erros  D) Move os arquivos inválidos para quarentena
<details><summary>Resposta</summary>

**B.** Só verifica. Também existe `VALIDATE n ROWS`. Na sintaxe, ele vem logo após `FILEFORMAT`.
</details>

**4.** Um lote novo traz uma coluna que não existe na tabela. O `COPY INTO` é executado **sem** `mergeSchema`. O que acontece?
A) A coluna é adicionada  B) **Erro**, e os arquivos não ficam marcados como carregados  C) A coluna é ignorada  D) A coluna vai para `_rescued_data`
<details><summary>Resposta</summary>

**B.** Com `COPY_OPTIONS ('mergeSchema'='true')` a coluna é adicionada, e as linhas antigas ficam NULL nela.
</details>

**5.** Qual é o padrão para criar uma tabela de destino do COPY INTO sem definir o schema?
<details><summary>Resposta</summary>

`CREATE TABLE IF NOT EXISTS t;` (vazia, sem colunas) e depois um `COPY INTO` com `COPY_OPTIONS ('mergeSchema'='true')`. A 1ª carga define o schema.
</details>

---

## Ingestão: Auto Loader

**6.** Arquivos JSON chegam continuamente, na casa de milhões por dia, e o schema muda com frequência. Qual é a melhor opção?
A) COPY INTO agendado  B) **Auto Loader**  C) CTAS de `json.\`path\``  D) `INSERT INTO` diário
<details><summary>Resposta</summary>

**B.** As pistas "milhões de arquivos", "contínuo" e "schema muda" apontam para Auto Loader. COPY INTO é para milhares de arquivos e cargas batch.
</details>

**7.** Onde o Auto Loader guarda quais arquivos já foram processados?
A) No log Delta da tabela de destino  B) **No `checkpointLocation`**  C) No `schemaLocation`  D) No metastore
<details><summary>Resposta</summary>

**B.** No checkpoint, em um RocksDB. O `schemaLocation` guarda o schema inferido. Se o checkpoint for apagado, o Auto Loader reprocessa tudo e duplica a tabela.
</details>

**8.** Com a configuração padrão, o Auto Loader encontra uma coluna nova. O que acontece?
A) Ignora  B) **Falha, atualiza o schema e processa a coluna na próxima execução**  C) Manda a coluna para `_rescued_data`  D) Reprocessa tudo
<details><summary>Resposta</summary>

**B.** O padrão é `schemaEvolutionMode = addNewColumns`. O stream falha porque o schema de um stream em execução é fixo. Um Job com retry reinicia o stream automaticamente.
</details>

**9.** Qual `schemaEvolutionMode` **nunca falha** e guarda as colunas novas em `_rescued_data`?
<details><summary>Resposta</summary>

**`rescue`.** Os outros: `failOnNewColumns` falha até alguém mudar o schema manualmente; `none` ignora a coluna nova e é o padrão quando o schema é informado.
</details>

**10.** O Auto Loader lê CSV e todas as colunas chegam como STRING. Qual opção resolve?
<details><summary>Resposta</summary>

`cloudFiles.inferColumnTypes = true`. Para tipos específicos, use `cloudFiles.schemaHints`, que sobrepõe a inferência.
</details>

**11.** Quais desses registros vão para `_rescued_data`? (I) `"abc"` em uma coluna INT; (II) `id_cliente` nulo; (III) quantidade `-3`; (IV) linha duplicada.
<details><summary>Resposta</summary>

**Só o I.** `_rescued_data` captura problemas de **schema** (tipo errado ou coluna desconhecida). Nulo, valor negativo e duplicata são regra de negócio, tratada na Silver ou com expectations.
</details>

**12.** Qual trigger faz o Auto Loader processar tudo o que está pendente e parar?
<details><summary>Resposta</summary>

`trigger(availableNow=True)`. É o padrão "batch incremental" e o modo usado no serverless.
</details>

**13.** Um arquivo já processado pelo Auto Loader é sobrescrito com o mesmo nome. Ele é reprocessado?
<details><summary>Resposta</summary>

**Não**, por padrão (`cloudFiles.allowOverwrites = false`).
</details>

---

## Delta Lake

**14.** Um `MERGE` falha com `multiple source rows matched ... same target row`. Qual é a causa e a correção?
<details><summary>Resposta</summary>

A fonte tem **chave duplicada** que casa com uma mesma linha do destino, e o MERGE não sabe qual UPDATE aplicar. A correção é **deduplicar a fonte** antes, por exemplo com `QUALIFY row_number() OVER (PARTITION BY id ORDER BY ts DESC) = 1`. Atenção: duplicatas que **não** casam com o destino são inseridas sem erro.
</details>

**15.** Como tornar um MERGE idempotente, para que reexecutá-lo não altere nada?
<details><summary>Resposta</summary>

Use `WHEN MATCHED AND t.col <> s.col THEN UPDATE`, para só atualizar o que mudou. Mesmo com 0 linhas alteradas, o MERGE **cria uma versão** nova no histórico.
</details>

**16.** Depois de `RESTORE TABLE t TO VERSION AS OF 4` (a tabela estava na versão 5), o que é verdade?
A) A versão 5 é apagada  B) **Uma nova versão é criada com o conteúdo da versão 4, e a 5 continua acessível por time travel**  C) A tabela fica só leitura  D) Exige VACUUM
<details><summary>Resposta</summary>

**B.** O RESTORE nunca apaga o histórico.
</details>

**17.** Você precisa substituir todo o conteúdo de uma tabela **mantendo o time travel**. Quais comandos servem?
<details><summary>Resposta</summary>

`CREATE OR REPLACE TABLE ... AS SELECT` ou `INSERT OVERWRITE`. `DROP` + `CREATE` **perde** o histórico. `INSERT INTO` acrescenta e duplica se for executado duas vezes.
</details>

**18.** Depois de um UPDATE, o `DESCRIBE HISTORY` mostra `numTargetDeletionVectorsAdded: 1` e `numTargetFilesRemoved: 0`. O que isso significa?
<details><summary>Resposta</summary>

**Deletion vectors**: o Delta marcou as linhas antigas como apagadas sem reescrever o arquivo Parquet. A escrita fica mais barata.
</details>

**19.** O que o `VACUUM` faz com o time travel?
<details><summary>Resposta</summary>

Remove arquivos que não são mais referenciados e são mais antigos que a retenção (padrão de **7 dias**). Versões que dependiam desses arquivos deixam de ser consultáveis.
</details>

**20.** Qual é a diferença entre `DROP TABLE` em uma tabela **managed** e em uma **external**?
<details><summary>Resposta</summary>

Na managed, o metadado e os **dados** são apagados. Na external, só o metadado; os arquivos continuam no storage.
</details>

---

## Views e Gold

**21.** Um dado novo é inserido na tabela de origem. Quem o mostra imediatamente: a VIEW, a tabela criada por CTAS ou a MATERIALIZED VIEW?
<details><summary>Resposta</summary>

A **VIEW**, porque recalcula a cada consulta. A MV só mostra depois do `REFRESH`. A tabela CTAS nunca mostra, a não ser que seja reescrita.
</details>

**22.** Um dashboard muito acessado consulta uma agregação pesada que pode ter alguns minutos de atraso. Qual objeto usar?
<details><summary>Resposta</summary>

**MATERIALIZED VIEW**: o resultado fica pré-computado e o refresh é incremental quando possível. Uma VIEW recalcularia tudo a cada acesso.
</details>

**23.** Qual é o escopo de uma `TEMP VIEW` e de uma `GLOBAL TEMP VIEW`?
<details><summary>Resposta</summary>

`TEMP VIEW` existe só na sessão (no notebook) que a criou. `GLOBAL TEMP VIEW` é visível para todas as sessões do mesmo cluster, no schema `global_temp`, até o cluster reiniciar.
</details>

**24.** Qual é o tipo de retorno de `date_trunc('MONTH', data)`?
<details><summary>Resposta</summary>

**TIMESTAMP.** Para obter DATE, use `trunc(data, 'MM')`.
</details>

---

## Lakeflow Declarative Pipelines (DLT)

**25.** `CONSTRAINT c EXPECT (id IS NOT NULL)`, sem `ON VIOLATION`. O que acontece com as linhas inválidas?
<details><summary>Resposta</summary>

**São mantidas**, e a violação é registrada nas métricas de data quality (aparece como ALLOW ou warn).
</details>

**26.** Qual é o efeito de `ON VIOLATION DROP ROW` e de `ON VIOLATION FAIL UPDATE`?
<details><summary>Resposta</summary>

`DROP ROW` descarta a linha e conta a violação. `FAIL UPDATE` interrompe a atualização do pipeline na primeira violação.
</details>

**27.** Uma expectation testa `_rescued_data IS NULL` e o pipeline dá `UNRESOLVED_COLUMN`. Por quê?
<details><summary>Resposta</summary>

As expectations são avaliadas sobre as **colunas do SELECT** do dataset. A coluna testada precisa estar na saída.
</details>

**28.** STREAMING TABLE ou MATERIALIZED VIEW: qual processa cada linha da fonte uma única vez (append), e qual reflete UPDATE e DELETE da fonte?
<details><summary>Resposta</summary>

A STREAMING TABLE é append incremental, e cada linha é processada uma vez. A MATERIALIZED VIEW é recalculada (incrementalmente quando possível) e reflete updates e deletes.
</details>

**29.** Em um `AUTO CDC INTO ... STORED AS SCD TYPE 2`, como encontrar a versão atual de cada chave? E o que acontece com um DELETE?
<details><summary>Resposta</summary>

A versão atual é a que tem `__END_AT IS NULL`. O DELETE **fecha** a versão, preenchendo `__END_AT`. No SCD1, o DELETE **remove** a linha.
</details>

**30.** Um evento com `seq = 1` chega **depois** de um evento com `seq = 2` da mesma chave. O que o `AUTO CDC` faz?
<details><summary>Resposta</summary>

Ordena pelo `SEQUENCE BY`, não pela chegada. No SCD1, o estado final continua sendo o do `seq = 2`. No SCD2, o evento atrasado é encaixado na posição certa do histórico.
</details>

**31.** `APPLY CHANGES INTO` e `AUTO CDC INTO`: qual é a diferença?
<details><summary>Resposta</summary>

**Nenhuma**, é o mesmo comando. `APPLY CHANGES` é o nome antigo, da época do DLT. Outros nomes antigos: `LIVE.tabela` e `CREATE LIVE TABLE`.
</details>

**32.** Você muda o schema (ou os `schemaHints`) de uma STREAMING TABLE. O que é necessário para aplicar a mudança aos dados já processados?
<details><summary>Resposta</summary>

Um **full refresh**. A streaming table mantém o checkpoint e o schema, e uma atualização normal continua do estado antigo.
</details>

**33.** Em que ordem o pipeline executa os arquivos `.sql` da pasta de código-fonte?
<details><summary>Resposta</summary>

A ordem dos arquivos não importa. O pipeline monta o **DAG pelas referências** entre as tabelas.
</details>
