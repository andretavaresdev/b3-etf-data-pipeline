"""DAG do pipeline de ETFs da B3: ingestão -> validação -> silver -> histórico -> gold, com gate de qualidade entre cada etapa."""
from __future__ import annotations

import sys

import pendulum
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, "/opt/airflow/project")


@dag(
    dag_id="b3_etf_pipeline",
    description="Coleta cotações dos ETFs do top_etf.json, grava na bronze, valida e publica silver e gold.",
    schedule="30 21 * * 1-5",  # dias úteis, após o fechamento do pregão (horário de Brasília)
    start_date=pendulum.datetime(2026, 9, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    tags=["b3", "etf", "bronze", "silver", "gold", "data-lake"],
)
def b3_etf_pipeline():
    @task
    def ingerir_bronze() -> str:
        from get_data import data_coleta_hoje, ingerir_ativos

        # Data real da coleta em America/Sao_Paulo — não o logical date do Airflow.
        data_execucao = data_coleta_hoje()
        resultados, falhas = ingerir_ativos(data_execucao=data_execucao)

        print(f"{len(resultados)} ativos ingeridos | {len(falhas)} falharam | partição date={data_execucao}")
        for ticker, motivo in falhas:
            print(f"[FALHA] {ticker}: {motivo}")

        if not resultados:
            raise RuntimeError("Nenhum ativo foi ingerido com sucesso nesta execução.")

        return data_execucao

    @task
    def validar_bronze(data_execucao: str) -> str:
        from validator import COBERTURA_MINIMA, STATUS_BLOQUEIAM_DAG, calcular_cobertura, validar_execucao

        resultados = validar_execucao(data_execucao=data_execucao)
        por_status = {}
        for r in resultados:
            por_status.setdefault(r.status, []).append(r)
        bloqueios = [r for r in resultados if r.status in STATUS_BLOQUEIAM_DAG]
        cobertura = calcular_cobertura(resultados)
        cobertura_ok = cobertura >= COBERTURA_MINIMA

        resumo = " | ".join(f"{len(v)} {k}" for k, v in sorted(por_status.items()))
        print(f"{len(resultados)} ativos verificados na bronze | {resumo} | partição date={data_execucao}")
        print(
            f"Cobertura: {cobertura:.2%} (mínimo exigido: {COBERTURA_MINIMA:.0%}) "
            f"{'OK' if cobertura_ok else 'ABAIXO DO MÍNIMO'}"
        )
        for r in bloqueios:
            print(f"[{r.status.upper()}] {r.ticker}: {r.motivo}")

        if bloqueios:
            raise RuntimeError(
                f"Validação da bronze bloqueou {len(bloqueios)} ativo(s) (falha/indeterminado) — "
                f"nenhum dado desta execução foi aprovado para a silver."
            )
        if not cobertura_ok:
            raise RuntimeError(
                f"Cobertura {cobertura:.2%} abaixo do mínimo de {COBERTURA_MINIMA:.0%} — "
                f"execução não aprovada para a silver."
            )

        return data_execucao

    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def transform_silver(data_execucao: str) -> str:
        from transform import transform_bronze_to_silver

        # trigger_rule explícito: só roda se validar_bronze terminar com sucesso.
        destino = transform_bronze_to_silver(data_execucao)
        print(f"Silver publicada em: {destino}")
        return data_execucao

    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def atualizar_historico(data_execucao: str) -> str:
        from ingest_cotahist import ingerir_cotahist

        # trigger_rule explícito: só roda se transform_silver terminar com sucesso. Atualiza só o
        # ano corrente — a B3 acrescenta o pregão mais recente ao COTAHIST anual todo dia útil.
        ano_corrente = int(data_execucao[:4])
        destino = ingerir_cotahist(ano_corrente)
        print(f"Silver histórica ({ano_corrente}) atualizada em: {destino}")
        return data_execucao

    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def transform_gold(data_execucao: str) -> str:
        from gold import transform_silver_to_gold

        # trigger_rule explícito: só roda se atualizar_historico terminar com sucesso.
        destino = transform_silver_to_gold(data_execucao)
        print(f"Gold publicada em: {destino}")
        return data_execucao

    transform_gold(atualizar_historico(transform_silver(validar_bronze(ingerir_bronze()))))


b3_etf_pipeline()
