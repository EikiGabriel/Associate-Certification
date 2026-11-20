-- Lakeflow Declarative Pipeline — SILVER
-- Expectations: sem ON VIOLATION = só registra (warn); DROP ROW = descarta; FAIL UPDATE = derruba o update

CREATE OR REFRESH STREAMING TABLE silver_vendas (
  CONSTRAINT schema_ok     EXPECT (_rescued_data IS NULL)     ON VIOLATION DROP ROW,
  CONSTRAINT cliente_ok    EXPECT (id_cliente IS NOT NULL)    ON VIOLATION DROP ROW,
  CONSTRAINT quantidade_ok EXPECT (quantidade > 0)            ON VIOLATION DROP ROW,
  CONSTRAINT preco_ok      EXPECT (preco_unitario IS NOT NULL)            -- só warn (linha 406)
)
COMMENT 'Vendas válidas'
AS SELECT id_venda, data_venda, id_cliente, id_produto, quantidade, preco_unitario, canal,
          quantidade * preco_unitario AS valor_total
FROM STREAM(bronze_vendas);

-- CDC de clientes: SCD tipo 1 (só o estado atual)
CREATE OR REFRESH STREAMING TABLE silver_clientes_scd1;

CREATE FLOW fluxo_clientes_scd1 AS AUTO CDC INTO silver_clientes_scd1
FROM STREAM(bronze_clientes_cdc)
KEYS (id_cliente)
APPLY AS DELETE WHEN operacao = 'DELETE'
SEQUENCE BY STRUCT(seq, arquivo_ts)          -- desempate: dois eventos com seq = 1 para o cliente 1
COLUMNS * EXCEPT (operacao, seq, arquivo_ts, _rescued_data)
STORED AS SCD TYPE 1;

-- CDC de clientes: SCD tipo 2 (histórico com __START_AT / __END_AT)
CREATE OR REFRESH STREAMING TABLE silver_clientes_scd2;

CREATE FLOW fluxo_clientes_scd2 AS AUTO CDC INTO silver_clientes_scd2
FROM STREAM(bronze_clientes_cdc)
KEYS (id_cliente)
APPLY AS DELETE WHEN operacao = 'DELETE'
SEQUENCE BY STRUCT(seq, arquivo_ts)
COLUMNS * EXCEPT (operacao, seq, arquivo_ts, _rescued_data)
STORED AS SCD TYPE 2;
