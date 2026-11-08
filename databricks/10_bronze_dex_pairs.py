# Databricks notebook source
dbutils.widgets.text("janela_horas", "6")

# COMMAND ----------

from pyspark.sql import functions as F

LANDING = "/Volumes/workspace/raw_memecoins/landing"
TARGET = "workspace.bronze_memecoins.dex_pair_snapshot"
janela = int(dbutils.widgets.get("janela_horas"))
if janela <= 0:
    raise ValueError("janela_horas deve ser maior que zero")

ENVELOPE = "run_id STRING, extracted_at STRING, source STRING, endpoint STRING, request STRING, payload_json STRING"
# Só os campos usados: campo novo na API é ignorado, campo que some vira NULL
PAIRS = """ARRAY<STRUCT<
  chainId: STRING, dexId: STRING, pairAddress: STRING,
  baseToken: STRUCT<address: STRING, name: STRING, symbol: STRING>,
  quoteToken: STRUCT<symbol: STRING>,
  priceUsd: STRING,
  txns: STRUCT<m5: STRUCT<buys: BIGINT, sells: BIGINT>, h1: STRUCT<buys: BIGINT, sells: BIGINT>,
               h24: STRUCT<buys: BIGINT, sells: BIGINT>>,
  volume: STRUCT<m5: DOUBLE, h1: DOUBLE, h6: DOUBLE, h24: DOUBLE>,
  priceChange: STRUCT<h1: DOUBLE, h6: DOUBLE, h24: DOUBLE>,
  liquidity: STRUCT<usd: DOUBLE>,
  fdv: DOUBLE, marketCap: DOUBLE, pairCreatedAt: BIGINT>>"""

raw = (spark.read.schema(ENVELOPE).json(f"{LANDING}/source=dexscreener/endpoint=tokens")
       # poda pela pasta de carga, depois o corte exato em horas
       .where(F.col("inserted_date") >= F.date_sub(F.current_date(), janela // 24 + 1))
       .withColumn("extracted_at", F.to_timestamp("extracted_at"))
       .where(F.col("extracted_at") >= F.expr(f"current_timestamp() - INTERVAL {janela} HOURS")))

pares = (raw.select("run_id", "extracted_at", F.explode(F.from_json("payload_json", PAIRS)).alias("p"))
    .where(F.col("p.chainId") == "solana")
    .select(
        "run_id", "extracted_at",
        F.col("p.pairAddress").alias("pair_address"),
        F.col("p.baseToken.address").alias("token_address"),
        F.col("p.baseToken.name").alias("token_name"),
        F.col("p.baseToken.symbol").alias("token_symbol"),
        F.col("p.quoteToken.symbol").alias("quote_symbol"),
        F.col("p.dexId").alias("dex_id"),
        F.col("p.priceUsd").cast("decimal(38,18)").alias("price_usd"),   # texto -> decimal, nunca double
        F.col("p.liquidity.usd").alias("liquidity_usd"),
        F.col("p.fdv").alias("fdv_usd"),
        F.col("p.marketCap").alias("market_cap_usd"),
        F.col("p.volume.m5").alias("volume_m5_usd"),
        F.col("p.volume.h1").alias("volume_h1_usd"),
        F.col("p.volume.h6").alias("volume_h6_usd"),
        F.col("p.volume.h24").alias("volume_h24_usd"),
        F.col("p.txns.m5.buys").alias("buys_m5"), F.col("p.txns.m5.sells").alias("sells_m5"),
        F.col("p.txns.h1.buys").alias("buys_h1"), F.col("p.txns.h1.sells").alias("sells_h1"),
        F.col("p.txns.h24.buys").alias("buys_h24"), F.col("p.txns.h24.sells").alias("sells_h24"),
        F.col("p.priceChange.h1").alias("price_change_h1_pct"),
        F.col("p.priceChange.h6").alias("price_change_h6_pct"),
        F.col("p.priceChange.h24").alias("price_change_h24_pct"),
        F.expr("timestamp_millis(p.pairCreatedAt)").alias("pair_created_at"))   # API manda milissegundos
    .dropDuplicates(["run_id", "pair_address"]))

sem_chave = pares.where("pair_address IS NULL OR token_address IS NULL").count()
if sem_chave:
    print(f"ATENÇÃO: {sem_chave} pares sem endereço descartados")   # visível, nunca calado

# COMMAND ----------

pares.where("pair_address IS NOT NULL AND token_address IS NOT NULL").createOrReplaceTempView("novos")
spark.sql(f"""
MERGE INTO {TARGET} AS t
USING novos AS s
ON t.run_id = s.run_id AND t.pair_address = s.pair_address
WHEN NOT MATCHED THEN INSERT *
""")
display(spark.sql(f"""SELECT run_id, COUNT(*) AS pares, COUNT(DISTINCT token_address) AS tokens
                      FROM {TARGET} GROUP BY run_id ORDER BY run_id DESC LIMIT 12"""))