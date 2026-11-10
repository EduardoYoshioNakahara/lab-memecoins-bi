CREATE SCHEMA IF NOT EXISTS workspace.raw_memecoins;
CREATE VOLUME IF NOT EXISTS workspace.raw_memecoins.landing
  COMMENT 'JSON bruto com envelope: source=/endpoint=/inserted_date=/inserted_hour= (UTC)';
CREATE SCHEMA IF NOT EXISTS workspace.ops_memecoins;
CREATE SCHEMA IF NOT EXISTS workspace.bronze_memecoins;
CREATE SCHEMA IF NOT EXISTS workspace.silver_memecoins_context;
CREATE SCHEMA IF NOT EXISTS workspace.gold_memecoins_context;
CREATE SCHEMA IF NOT EXISTS workspace.visualization_memecoins;

CREATE TABLE IF NOT EXISTS workspace.ops_memecoins.watchlist (
  token_address     STRING    NOT NULL,
  status            STRING    NOT NULL COMMENT 'tracked | retired',
  discovered_at     TIMESTAMP NOT NULL,
  status_changed_at TIMESTAMP NOT NULL,
  retired_reason    STRING    COMMENT 'aged_out | rug | crash | dead'
) COMMENT 'Universo acompanhado: quem a extração consulta a cada hora';

CREATE TABLE IF NOT EXISTS workspace.ops_memecoins.run_log (
  run_id       STRING    NOT NULL,
  extracted_at TIMESTAMP NOT NULL,
  source       STRING    NOT NULL,
  endpoint     STRING    NOT NULL,
  seq          INT,
  status       STRING    NOT NULL COMMENT 'ok | failed',
  error        STRING
) COMMENT 'Uma linha por chamada de API: prova de que a coleta rodou e do que falhou';

CREATE TABLE IF NOT EXISTS workspace.bronze_memecoins.dex_pair_snapshot (
  run_id               STRING    NOT NULL,
  extracted_at         TIMESTAMP NOT NULL,
  pair_address         STRING    NOT NULL,
  token_address        STRING    NOT NULL,
  token_name           STRING,
  token_symbol         STRING,
  quote_symbol         STRING,
  dex_id               STRING,
  price_usd            DECIMAL(38,18),
  liquidity_usd        DOUBLE,
  fdv_usd              DOUBLE,
  market_cap_usd       DOUBLE,
  volume_m5_usd        DOUBLE,
  volume_h1_usd        DOUBLE,
  volume_h6_usd        DOUBLE,
  volume_h24_usd       DOUBLE,
  buys_m5              BIGINT,
  sells_m5             BIGINT,
  buys_h1              BIGINT,
  sells_h1             BIGINT,
  buys_h24             BIGINT,
  sells_h24            BIGINT,
  price_change_h1_pct  DOUBLE,
  price_change_h6_pct  DOUBLE,
  price_change_h24_pct DOUBLE,
  pair_created_at      TIMESTAMP
) COMMENT 'Append-only: um snapshot por par por execução. Chave: run_id + pair_address';