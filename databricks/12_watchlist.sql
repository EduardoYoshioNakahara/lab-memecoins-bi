-- Databricks notebook source
-- Par principal por token e por execução, nas últimas 12 horas; recencia 1 = execução mais recente
CREATE OR REPLACE TEMP VIEW principal AS
SELECT run_id, extracted_at, token_address, liquidity_usd, pair_created_at,
       ROW_NUMBER() OVER (PARTITION BY token_address ORDER BY extracted_at DESC) AS recencia
FROM (
  SELECT s.*, ROW_NUMBER() OVER (PARTITION BY run_id, token_address
                                 ORDER BY liquidity_usd DESC NULLS LAST, pair_created_at) AS rn
  FROM workspace.bronze_memecoins.dex_pair_snapshot AS s
  WHERE extracted_at >= current_timestamp() - INTERVAL 12 HOURS
) WHERE rn = 1;

-- COMMAND ----------

-- Entrada: liquidez >= 10 mil e par com ate 72 h
MERGE INTO workspace.ops_memecoins.watchlist AS w
USING (SELECT token_address, extracted_at FROM principal
       WHERE recencia = 1 AND liquidity_usd >= 10000
         AND pair_created_at >= extracted_at - INTERVAL 72 HOURS) AS c
ON w.token_address = c.token_address
WHEN NOT MATCHED THEN INSERT (token_address, status, discovered_at, status_changed_at, retired_reason)
  VALUES (c.token_address, 'tracked', c.extracted_at, c.extracted_at, NULL);

-- COMMAND ----------

-- Saida: idade, rug (queda >= 90% entre dois snapshots) e morte (6 snapshots < 1 mil)
MERGE INTO workspace.ops_memecoins.watchlist AS w
USING (
  SELECT w.token_address,
    CASE
      WHEN w.discovered_at < current_timestamp() - INTERVAL 14 DAYS THEN 'aged_out'
      WHEN a.liquidity_usd <= b.liquidity_usd * 0.1 THEN 'rug'
      WHEN d.n_baixa = 6 THEN 'dead'
    END AS motivo
  FROM workspace.ops_memecoins.watchlist AS w
  LEFT JOIN principal AS a ON a.token_address = w.token_address AND a.recencia = 1
  LEFT JOIN principal AS b ON b.token_address = w.token_address AND b.recencia = 2
  LEFT JOIN (SELECT token_address, COUNT_IF(liquidity_usd < 1000) AS n_baixa
             FROM principal WHERE recencia <= 6 GROUP BY token_address) AS d
    ON d.token_address = w.token_address
  WHERE w.status = 'tracked'
) AS r
ON w.token_address = r.token_address AND r.motivo IS NOT NULL
WHEN MATCHED THEN UPDATE SET status = 'retired', retired_reason = r.motivo,
                             status_changed_at = current_timestamp();

-- COMMAND ----------

SELECT status, retired_reason, COUNT(*) AS tokens
FROM workspace.ops_memecoins.watchlist GROUP BY ALL ORDER BY 1, 2;