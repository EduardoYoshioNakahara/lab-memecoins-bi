# Databricks notebook source
resumo = dbutils.jobs.taskValues.get(taskKey="extract", key="resumo", default=None, debugValue={})
if resumo is None:
    raise RuntimeError("A extração não terminou: a hora ficou sem coleta")
print(resumo)
if resumo.get("falhas"):
    print("Falhas toleradas nesta hora:", resumo["falhas"])
if resumo.get("fora_do_limite"):
    print("Tokens fora do limite de acompanhamento:", resumo["fora_do_limite"])