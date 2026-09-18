import argparse
import math
import os
import statistics
import uuid
from datetime import date
from pathlib import Path

import duckdb

from get_data import CAMINHO_ATIVOS, carregar_ativos, data_coleta_hoje
from ingest_cotahist import RAIZ_LAKE_SILVER_HISTORICO

RAIZ_LAKE_GOLD = Path(__file__).parent / "datalake" / "gold"

# Janelas de retorno em pregões (sessões de negociação), não dias corridos.
JANELAS_RETORNO = (21, 63, 126, 252)
PREGOES_ANO = 252
# Mínimo de retornos diários pra confiar numa volatilidade anualizada (mesma janela do retorno_21).
MIN_OBS_VOLATILIDADE = 21

# Métricas são retorno de preço puro (PREULT do COTAHIST) — sem ajuste por dividendos,
# desdobramentos, grupamentos ou outros eventos corporativos.
CAMPOS_GOLD = (
    "ticker", "as_of_date", "status_historico", "ultimo_preco",
    "retorno_21", "retorno_63", "retorno_126", "retorno_252",
    "volatilidade_anualizada", "drawdown_atual", "maximo_drawdown",
    "qtd_observacoes", "periodo_inicio", "periodo_fim",
)

# Todo ticker do universo (top_etf.json) sai na gold, mesmo sem histórico — omitir esconderia
# uma limitação real da fonte (alguns ETFs de renda fixa não aparecem no COTAHIST).
STATUS_HISTORICO_OK = "ok"
STATUS_HISTORICO_AUSENTE = "sem_historico_cotahist"

DDL_GOLD = """
    CREATE TABLE gold_etfs (
        ticker VARCHAR NOT NULL,
        as_of_date DATE NOT NULL,
        status_historico VARCHAR NOT NULL,
        ultimo_preco DOUBLE,
        retorno_21 DOUBLE,
        retorno_63 DOUBLE,
        retorno_126 DOUBLE,
        retorno_252 DOUBLE,
        volatilidade_anualizada DOUBLE,
        drawdown_atual DOUBLE,
        maximo_drawdown DOUBLE,
        qtd_observacoes INTEGER NOT NULL,
        periodo_inicio DATE,
        periodo_fim DATE
    )
"""


def calcular_metricas_ticker(serie: list) -> dict:
    """serie: [(data_pregao, preco_fechamento), ...] ordenada por data ascendente, já filtrada <= as_of_date."""
    n = len(serie)
    if n == 0:
        return {
            "ultimo_preco": None,
            **{f"retorno_{j}": None for j in JANELAS_RETORNO},
            "volatilidade_anualizada": None,
            "drawdown_atual": None,
            "maximo_drawdown": None,
            "qtd_observacoes": 0,
            "periodo_inicio": None,
            "periodo_fim": None,
        }

    datas = [d for d, _ in serie]
    precos = [p for _, p in serie]

    retornos = {j: (precos[-1] / precos[-1 - j] - 1 if n > j else None) for j in JANELAS_RETORNO}

    log_retornos = [math.log(precos[i] / precos[i - 1]) for i in range(1, n)]
    volatilidade = (
        statistics.stdev(log_retornos) * math.sqrt(PREGOES_ANO)
        if len(log_retornos) >= MIN_OBS_VOLATILIDADE else None
    )

    pico = precos[0]
    maximo_drawdown = 0.0
    for preco in precos:
        pico = max(pico, preco)
        maximo_drawdown = min(maximo_drawdown, preco / pico - 1)
    drawdown_atual = precos[-1] / pico - 1

    return {
        "ultimo_preco": precos[-1],
        **{f"retorno_{j}": retornos[j] for j in JANELAS_RETORNO},
        "volatilidade_anualizada": volatilidade,
        "drawdown_atual": drawdown_atual,
        "maximo_drawdown": maximo_drawdown,
        "qtd_observacoes": n,
        "periodo_inicio": datas[0],
        "periodo_fim": datas[-1],
    }


def caminho_particao_gold(as_of_date: str) -> Path:
    return RAIZ_LAKE_GOLD / "etfs" / f"as_of_date={as_of_date}" / f"gold_etfs_{as_of_date}.parquet"


def transform_silver_to_gold(data_particao: str, arquivo: Path = CAMINHO_ATIVOS) -> Path:
    """Lê a silver histórica (glob, só datas <= data_particao) e publica as métricas por ticker na gold.

    LEFT JOIN do universo (top_etf.json) com a silver histórica: todo ticker sai, com ou sem
    dado — nunca publica menos linhas que o universo esperado.
    """
    tickers = sorted({a["ticker"].upper() for a in carregar_ativos(arquivo=arquivo)})

    glob_historico = str(RAIZ_LAKE_SILVER_HISTORICO / "year=*" / "*.parquet")
    con = duckdb.connect()
    try:
        serie_completa = con.sql(
            f"""
            SELECT ticker, data_pregao, preco_fechamento
            FROM read_parquet('{glob_historico}')
            WHERE data_pregao <= DATE '{data_particao}'
            ORDER BY ticker, data_pregao
            """
        ).fetchall()
    except duckdb.IOException:
        serie_completa = []

    por_ticker = {t: [] for t in tickers}
    for ticker, data_pregao, preco in serie_completa:
        if ticker in por_ticker:
            por_ticker[ticker].append((data_pregao, preco))

    linhas = []
    for ticker in tickers:
        m = calcular_metricas_ticker(por_ticker[ticker])
        status = STATUS_HISTORICO_AUSENTE if m["qtd_observacoes"] == 0 else STATUS_HISTORICO_OK
        linhas.append((
            ticker, date.fromisoformat(data_particao), status, m["ultimo_preco"],
            m["retorno_21"], m["retorno_63"], m["retorno_126"], m["retorno_252"],
            m["volatilidade_anualizada"], m["drawdown_atual"], m["maximo_drawdown"],
            m["qtd_observacoes"], m["periodo_inicio"], m["periodo_fim"],
        ))

    destino = caminho_particao_gold(data_particao)
    destino.parent.mkdir(parents=True, exist_ok=True)

    con2 = duckdb.connect()
    con2.execute(DDL_GOLD)
    placeholders = ",".join(["?"] * len(CAMPOS_GOLD))
    con2.executemany(f"INSERT INTO gold_etfs VALUES ({placeholders})", linhas)

    tmp = destino.with_name(f".{destino.name}.tmp-{uuid.uuid4().hex}")
    try:
        con2.sql(f"COPY (SELECT * FROM gold_etfs ORDER BY ticker) TO '{tmp.as_posix()}' (FORMAT PARQUET)")
        os.replace(tmp, destino)
    finally:
        if tmp.exists():
            tmp.unlink()

    return destino


def criar_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Calcula métricas de retorno, volatilidade e drawdown a partir da silver histórica."
    )
    parser.add_argument(
        "-d", "--data",
        default=data_coleta_hoje(),
        help="Data de referência (as_of_date) para o cálculo (default: hoje em America/Sao_Paulo).",
    )
    return parser


def main():
    args = criar_parser().parse_args()
    destino = transform_silver_to_gold(args.data)
    print(f"Gold publicada em: {destino}")


if __name__ == "__main__":
    main()
