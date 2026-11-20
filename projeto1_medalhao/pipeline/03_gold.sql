-- Lakeflow Declarative Pipeline — GOLD
-- MATERIALIZED VIEW: recalculada (incremental quando possível); reflete UPDATE/DELETE da fonte

CREATE OR REFRESH MATERIALIZED VIEW gold_receita_por_cidade
COMMENT 'Receita por cidade atual do cliente'
AS SELECT c.cidade, count(*) AS vendas, round(sum(v.valor_total), 2) AS receita
FROM silver_vendas v
JOIN silver_clientes_scd1 c USING (id_cliente)
GROUP BY c.cidade;
