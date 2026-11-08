# Databricks notebook source
# Extração horária: descoberta + acompanhamento. Grava o JSON bruto na landing e o log da execução.
dbutils.widgets.text("max_tracked", "300")

# COMMAND ----------

import datetime as dt
import json
import os
import time

import requests

LANDING = "/Volumes/workspace/raw_memecoins/landing"
WATCHLIST = "workspace.ops_memecoins.watchlist"
RUN_LOG = "workspace.ops_memecoins.run_log"
HEADERS = {"Accept": "application/json", "User-Agent": "lab-memecoins-bi"}
MIN_INTERVAL = {"api.dexscreener.com": 1.0, "api.geckoterminal.com": 6.0, "api.gopluslabs.io": 2.0}
RETRYABLE = {429, 500, 502, 503, 504}
QUOTES = {"So11111111111111111111111111111111111111112",   # SOL
          "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",   # USDC
          "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"}   # USDT

run_ts = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
run_id = run_ts.strftime("%Y%m%dT%H%M%SZ")
max_tracked = int(dbutils.widgets.get("max_tracked"))
_last_call = {}
log = []   # uma tupla por chamada: (fonte, endpoint, seq, status, erro)


def get_json(url):
    host = url.split("/")[2]
    for attempt in range(4):
        wait = MIN_INTERVAL.get(host, 1.0) - (time.monotonic() - _last_call.get(host, 0.0))
        if wait > 0:
            time.sleep(wait)
        _last_call[host] = time.monotonic()
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code in RETRYABLE and attempt < 3:
            time.sleep(15 * 2 ** attempt)   # 15, 30, 60 s
            continue
        raise RuntimeError(f"HTTP {resp.status_code} | {resp.text[:300]}")


def land(source, endpoint, url, payload, seq):
    folder = (f"{LANDING}/source={source}/endpoint={endpoint}"
              f"/inserted_date={run_ts:%Y-%m-%d}/inserted_hour={run_ts:%H}")
    os.makedirs(folder, exist_ok=True)
    envelope = {"run_id": run_id, "extracted_at": run_ts.isoformat(), "source": source,
                "endpoint": endpoint, "request": url, "payload_json": json.dumps(payload)}
    with open(f"{folder}/{run_id}_{seq:03d}.json", "w", encoding="utf-8") as f:
        json.dump(envelope, f)


def call(source, endpoint, url, seq=0):
    try:
        payload = get_json(url)
        land(source, endpoint, url, payload, seq)
        log.append((source, endpoint, seq, "ok", None))
        return payload
    except Exception as exc:
        print(f"[{source}/{endpoint}#{seq}] falhou: {exc}", flush=True)
        log.append((source, endpoint, seq, "failed", str(exc)[:500]))
        return None

# COMMAND ----------

# Descoberta: quem são os candidatos desta hora (só Solana, sem SOL/USDC/USDT)
candidatos = set()

for ep, url in [("token_profiles_latest", "https://api.dexscreener.com/token-profiles/latest/v1"),
                ("token_boosts_latest", "https://api.dexscreener.com/token-boosts/latest/v1"),
                ("token_boosts_top", "https://api.dexscreener.com/token-boosts/top/v1")]:
    for item in call("dexscreener", ep, url) or []:
        if item.get("chainId") == "solana" and item.get("tokenAddress"):
            candidatos.add(item["tokenAddress"])

for seq, (ep, url) in enumerate([
        ("new_pools", "https://api.geckoterminal.com/api/v2/networks/solana/new_pools?page=1"),
        ("new_pools", "https://api.geckoterminal.com/api/v2/networks/solana/new_pools?page=2"),
        ("trending_pools", "https://api.geckoterminal.com/api/v2/networks/solana/trending_pools?page=1")]):
    for pool in (call("geckoterminal", ep, url, seq) or {}).get("data", []):
        base = pool["relationships"]["base_token"]["data"]["id"]   # "solana_<endereço>"
        candidatos.add(base.removeprefix("solana_"))

candidatos -= QUOTES
print(f"{len(candidatos)} candidatos descobertos")

# COMMAND ----------

# Acompanhamento: watchlist ativa primeiro, depois os candidatos; lotes de 30 (limite da API)
ativos = [r.token_address for r in spark.sql(
    f"SELECT token_address FROM {WATCHLIST} WHERE status = 'tracked' ORDER BY discovered_at DESC").collect()]
todos = list(dict.fromkeys(ativos + sorted(candidatos)))
universo = todos[:max_tracked]
if len(todos) > len(universo):
    print(f"ATENÇÃO: {len(todos) - len(universo)} tokens ficaram fora (max_tracked={max_tracked})")

lotes = [universo[i:i + 30] for i in range(0, len(universo), 30)]
for n, lote in enumerate(lotes):
    call("dexscreener", "tokens", f"https://api.dexscreener.com/tokens/v1/solana/{','.join(lote)}", seq=n)

# COMMAND ----------

# Log da execução + resultado para o Job
spark.createDataFrame(
    [(run_id, run_ts, s, e, q, st, er) for s, e, q, st, er in log],
    "run_id STRING, extracted_at TIMESTAMP, source STRING, endpoint STRING, seq INT, status STRING, error STRING",
).write.mode("append").saveAsTable(RUN_LOG)

lotes_ok = sum(1 for x in log if x[1] == "tokens" and x[3] == "ok")
descoberta_ok = sum(1 for x in log if x[1] != "tokens" and x[3] == "ok")
resumo = {"run_id": run_id, "candidatos": len(candidatos), "acompanhados": len(universo),
          "fora_do_limite": len(todos) - len(universo), "lotes": len(lotes), "lotes_ok": lotes_ok,
          "falhas": [f"{s}/{e}#{q}" for s, e, q, st, _ in log if st == "failed"]}
print(json.dumps(resumo, indent=2))
dbutils.jobs.taskValues.set(key="resumo", value=resumo)

if descoberta_ok == 0:
    raise RuntimeError("Todas as fontes de descoberta falharam")
if lotes and lotes_ok == 0:
    raise RuntimeError("Nenhum lote de acompanhamento respondeu: a hora ficou sem snapshot")