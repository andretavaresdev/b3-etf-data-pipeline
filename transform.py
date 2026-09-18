import argparse
import json
import os
import re
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import duckdb

from get_data import CAMINHO_ATIVOS, TZ_COLETA, data_coleta_hoje
from validator import COBERTURA_MINIMA, STATUS_BLOQUEIAM_DAG, calcular_cobertura, validar_execucao

RAIZ_LAKE_SILVER = Path(__file__).parent / "datalake" / "silver"

# Únicos status elegíveis pra silver (ver validator.py). Reler aqui, e não importar de
# validator.STATUS_COBERTURA, porque são conceitos diferentes que hoje coincidem em valor:
# cobertura é uma métrica agregada, elegibilidade pra silver é uma regra de publicação —
# podem divergir no futuro.
STATUS_SILVER = ("ok", "dados_parciais")

CAMPOS_SILVER = (
    "ticker", "nome", "valor_atual", "minimo_dia", "maximo_dia", "rentabilidade_dia",
    "rentabilidade_mes", "rentabilidade_ano", "data_coleta", "data_referencia", "coletado_em",
)

DDL_SILVER_ETFS = """
    CREATE TABLE silver_etfs (
        ticker VARCHAR NOT NULL,
        nome VARCHAR NOT NULL,
        valor_atual DOUBLE NOT NULL,
        minimo_dia DOUBLE NOT NULL,
        maximo_dia DOUBLE NOT NULL,
        rentabilidade_dia DOUBLE NOT NULL,
        rentabilidade_mes DOUBLE,
        rentabilidade_ano DOUBLE,
        data_coleta DATE NOT NULL,
        data_referencia TIMESTAMPTZ,
        coletado_em TIMESTAMPTZ NOT NULL
    )
"""

_RE_ULTIMA_ATUALIZACAO = re.compile(r"(\d{2})/(\d{2})/(\d{4})\s+(\d{2})h(\d{2})")


def caminho_particao_silver(data_particao: str) -> Path:
    ano, mes, _dia = data_particao.split("-")
    return RAIZ_LAKE_SILVER / "etfs" / f"year={ano}" / f"month={mes}" / f"etfs_{data_particao}.parquet"


def _parse_data_referencia(texto: Optional[str]) -> Optional[datetime]:
    """Extrai a data/hora que a própria B3 reporta como referência da cotação (ex.: "Atualizado
    às 17/09/2026 17h16. Delay 15 min."). None se o texto estiver ausente ou não bater o formato —
    campo nullable na silver, não derruba a linha (o gate de qualidade já rodou no validator).
    """
    if not texto:
        return None
    m = _RE_ULTIMA_ATUALIZACAO.search(texto)
    if not m:
        return None
    dia, mes, ano, hora, minuto = (int(g) for g in m.groups())
    try:
        return datetime(ano, mes, dia, hora, minuto, tzinfo=TZ_COLETA)
    except ValueError:
        return None


def _linha_silver(dados: dict, data_particao: str) -> tuple:
    fonte = dados.get("_fonte") or {}
    return (
        dados["ticker"],
        dados["nome"],
        dados["valor_atual"],
        dados["minimo_dia"],
        dados["maximo_dia"],
        dados["rentabilidade_dia"],
        dados.get("rentabilidade_mes"),
        dados.get("rentabilidade_ano"),
        date.fromisoformat(data_particao),
        _parse_data_referencia(fonte.get("ultima_atualizacao_texto")),
        datetime.fromisoformat(fonte["coletado_em"]),
    )


def transform_bronze_to_silver(data_particao: str, arquivo: Path = CAMINHO_ATIVOS) -> Path:
    """Lê bronze/etfs/ticker=*/date={data_particao}/cotacao.json, aplica o contrato de schema
    e publica um único Parquet diário na silver.

    Repete o gate de qualidade do validator (bloqueios + cobertura mínima) aqui dentro, e não só
    na task da DAG — quem chamar essa função direto (CLI, notebook, outro script), sem passar
    pelo Airflow, tem que ficar sujeito à mesma regra. Confiar só no gate externo deixaria a
    função perigosa fora do fluxo orquestrado.

    Só entram linhas "ok" ou "dados_parciais" (ver validator.validar_execucao) — "sem_cotacao",
    "indeterminado" e "falha" ficam de fora. Não faz nenhuma requisição de rede: reclassifica
    a partir do que já está na bronze, igual ao gate da DAG.

    Publicação atômica: escreve num arquivo temporário no mesmo diretório e só troca pelo
    definitivo com os.replace (rename atômico) depois que o Parquet inteiro foi gravado com
    sucesso — nunca existe um arquivo final parcial/corrompido. Idempotente: cada chamada pra
    mesma data_particao processa a bronze do zero e substitui o Parquet inteiro, sem acumular.
    """
    resultados = validar_execucao(data_execucao=data_particao, arquivo=arquivo)

    bloqueios = [r for r in resultados if r.status in STATUS_BLOQUEIAM_DAG]
    if bloqueios:
        raise RuntimeError(
            f"Publicação bloqueada para date={data_particao}: {len(bloqueios)} ativo(s) em "
            f"falha/indeterminado ({', '.join(r.ticker for r in bloqueios)})."
        )

    cobertura = calcular_cobertura(resultados)
    if cobertura < COBERTURA_MINIMA:
        raise RuntimeError(
            f"Publicação bloqueada para date={data_particao}: cobertura {cobertura:.2%} "
            f"abaixo do mínimo de {COBERTURA_MINIMA:.0%}."
        )

    incluidos = [r for r in resultados if r.status in STATUS_SILVER]
    if not incluidos:
        raise RuntimeError(
            f"Nenhum ativo elegível (ok/dados_parciais) para date={data_particao} — nada a publicar na silver."
        )

    linhas = []
    for r in incluidos:
        with open(r.caminho, encoding="utf-8") as f:
            dados = json.load(f)
        linhas.append(_linha_silver(dados, data_particao))

    destino = caminho_particao_silver(data_particao)
    destino.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(DDL_SILVER_ETFS)
    placeholders = ",".join(["?"] * len(CAMPOS_SILVER))
    con.executemany(f"INSERT INTO silver_etfs VALUES ({placeholders})", linhas)

    tmp = destino.with_name(f".{destino.name}.tmp-{uuid.uuid4().hex}")
    try:
        con.sql(
            f"COPY (SELECT * FROM silver_etfs ORDER BY ticker) TO '{tmp.as_posix()}' (FORMAT PARQUET)"
        )
        os.replace(tmp, destino)
    finally:
        if tmp.exists():
            tmp.unlink()

    return destino


def criar_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Transforma a bronze validada de um dia num Parquet diário na silver."
    )
    parser.add_argument(
        "-d", "--data",
        default=data_coleta_hoje(),
        help="Partição date= da bronze a transformar (default: hoje em America/Sao_Paulo).",
    )
    return parser


def main():
    args = criar_parser().parse_args()
    destino = transform_bronze_to_silver(args.data)
    print(f"Silver publicada em: {destino}")


if __name__ == "__main__":
    main()
