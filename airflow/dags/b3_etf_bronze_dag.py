"""
DAG de ingestão + validação + silver dos ETFs da B3 (arquitetura medalhão).

Fluxo: scraping grava a resposta bruta na bronze -> validator examina os dados
recém-ingeridos -> transform_bronze_to_silver publica o Parquet diário na silver.
Se algum ativo cair em "falha" ou "indeterminado" (ver validator.py), a DAG para no
validar_bronze e a execução inteira não é aprovada para a silver — a transformação
nunca roda nesse caso. "sem_cotacao" não bloqueia a DAG, mas esse ativo específico
fica fora da silver (ativo reconhecido pela B3, só sem cotação publicada no dia).

A partição date= usa a data real da coleta em America/Sao_Paulo (get_data.data_coleta_hoje),
não o logical date do Airflow — o logical date de um schedule cru é o início do intervalo
agendado, não o instante em que a task roda de fato, e usá-lo gravaria na partição errada.

Além do gate por ativo, há um gate de cobertura: (ok + dados_parciais) / total esperado
precisa ser >= 95% (validator.COBERTURA_MINIMA). Mesmo sem nenhum "falha"/"indeterminado"
individual, cobertura baixa (ex.: excesso de "sem_cotacao") também bloqueia a silver.

A gold (próxima etapa, fora deste DAG) será calculada só a partir da silver.
"""
from __future__ import annotations

import sys

import pendulum
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, "/opt/airflow/project")


@dag(
    dag_id="b3_etf_ingestao_bronze",
    description="Coleta cotações dos ETFs do top_etf.json, grava na bronze e valida antes de liberar para a silver.",
    schedule="30 21 * * 1-5",  # dias úteis, após o fechamento do pregão (horário de Brasília)
    start_date=pendulum.datetime(2026, 9, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    tags=["b3", "etf", "bronze", "data-lake"],
)
def b3_etf_ingestao_bronze():
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

        # trigger_rule=ALL_SUCCESS é o default, mas deixei explícito de propósito: essa task
        # só executa se validar_bronze tiver terminado com sucesso (gate por ativo + cobertura
        # ok). Se validar_bronze falhar ou for pulada, o Airflow pula transform_silver também
        # — nunca roda em paralelo nem antes do validator terminar.
        destino = transform_bronze_to_silver(data_execucao)
        print(f"Silver publicada em: {destino}")
        return str(destino)

    transform_silver(validar_bronze(ingerir_bronze()))


b3_etf_ingestao_bronze()
