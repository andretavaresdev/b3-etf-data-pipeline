import argparse
import io
import os
import uuid
import zipfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional

import duckdb
import requests

from get_data import CAMINHO_ATIVOS, carregar_ativos, data_coleta_hoje

URL_COTAHIST_ANUAL = "https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_A{ano}.ZIP"
TAM_REGISTRO = 245

RAIZ_LAKE_BRONZE_COTAHIST = Path(__file__).parent / "datalake" / "bronze" / "cotahist"
RAIZ_LAKE_SILVER_HISTORICO = Path(__file__).parent / "datalake" / "silver" / "etfs_historico"

CAMPOS_SILVER_HISTORICO = (
    "ticker", "nome", "data_pregao", "preco_abertura", "preco_maximo",
    "preco_minimo", "preco_medio", "preco_fechamento", "qtd_negocios", "volume_financeiro",
)

DDL_SILVER_HISTORICO = """
    CREATE TABLE silver_historico (
        ticker VARCHAR NOT NULL,
        nome VARCHAR NOT NULL,
        data_pregao DATE NOT NULL,
        preco_abertura DOUBLE NOT NULL,
        preco_maximo DOUBLE NOT NULL,
        preco_minimo DOUBLE NOT NULL,
        preco_medio DOUBLE NOT NULL,
        preco_fechamento DOUBLE NOT NULL,
        qtd_negocios BIGINT NOT NULL,
        volume_financeiro DOUBLE NOT NULL
    )
"""


@dataclass
class RegistroCotahist:
    ticker: str
    nome: str
    data_pregao: date
    preco_abertura: float
    preco_maximo: float
    preco_minimo: float
    preco_medio: float
    preco_fechamento: float
    qtd_negocios: int
    volume_financeiro: float


def _campo(linha: str, pos_ini: int, pos_fim: int) -> str:
    return linha[pos_ini - 1:pos_fim]


def _preco(linha: str, pos_ini: int, pos_fim: int) -> float:
    return int(_campo(linha, pos_ini, pos_fim)) / 100


def parse_linha_cotahist(linha: str) -> Optional[RegistroCotahist]:
    """Layout oficial B3 (SeriesHistoricas_Layout): registro tipo 01, 245 bytes, só mercado à vista (TPMERC 010)."""
    if len(linha) < TAM_REGISTRO or _campo(linha, 1, 2) != "01":
        return None
    if _campo(linha, 25, 27) != "010":
        return None
    return RegistroCotahist(
        ticker=_campo(linha, 13, 24).strip(),
        nome=_campo(linha, 28, 39).strip(),
        data_pregao=date(int(_campo(linha, 3, 6)), int(_campo(linha, 7, 8)), int(_campo(linha, 9, 10))),
        preco_abertura=_preco(linha, 57, 69),
        preco_maximo=_preco(linha, 70, 82),
        preco_minimo=_preco(linha, 83, 95),
        preco_medio=_preco(linha, 96, 108),
        preco_fechamento=_preco(linha, 109, 121),
        qtd_negocios=int(_campo(linha, 148, 152)),
        volume_financeiro=_preco(linha, 171, 188),
    )


def parse_cotahist_texto(texto: str, tickers_permitidos: set) -> list[RegistroCotahist]:
    """Filtra pra tickers_permitidos; duplicidade (ticker, data_pregao) resolve pela última ocorrência."""
    registros = {}
    for linha in texto.splitlines():
        reg = parse_linha_cotahist(linha)
        if reg is None or reg.ticker not in tickers_permitidos:
            continue
        registros[(reg.ticker, reg.data_pregao)] = reg
    return sorted(registros.values(), key=lambda r: (r.ticker, r.data_pregao))


def baixar_cotahist(ano: int) -> bytes:
    resp = requests.get(URL_COTAHIST_ANUAL.format(ano=ano), timeout=120)
    resp.raise_for_status()
    return resp.content


def caminho_bronze_cotahist(ano: int, data_coleta: str) -> Path:
    """COTAHIST_A{ano}.ZIP muda todo dia (a B3 acrescenta o pregão mais recente) — versiona por
    data de coleta, senão a bronze não preserva o arquivo exato usado em cada processamento."""
    return RAIZ_LAKE_BRONZE_COTAHIST / f"year={ano}" / f"collected_date={data_coleta}" / f"COTAHIST_A{ano}.ZIP"


def salvar_bronze_cotahist(ano: int, data_coleta: str, conteudo: bytes) -> Path:
    caminho = caminho_bronze_cotahist(ano, data_coleta)
    caminho.parent.mkdir(parents=True, exist_ok=True)
    caminho.write_bytes(conteudo)
    return caminho


def caminho_particao_silver_historico(ano: int) -> Path:
    return RAIZ_LAKE_SILVER_HISTORICO / f"year={ano}" / f"etfs_historico_{ano}.parquet"


def glob_silver_historico() -> str:
    """Padrão de glob pra ler todos os anos da silver histórica de uma vez (DuckDB read_parquet)."""
    return str(RAIZ_LAKE_SILVER_HISTORICO / "year=*" / "*.parquet")


def publicar_silver_historico(ano: int, registros: list[RegistroCotahist]) -> Path:
    destino = caminho_particao_silver_historico(ano)
    destino.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(DDL_SILVER_HISTORICO)
    linhas = [
        (r.ticker, r.nome, r.data_pregao, r.preco_abertura, r.preco_maximo,
         r.preco_minimo, r.preco_medio, r.preco_fechamento, r.qtd_negocios, r.volume_financeiro)
        for r in registros
    ]
    if linhas:
        placeholders = ",".join(["?"] * len(CAMPOS_SILVER_HISTORICO))
        con.executemany(f"INSERT INTO silver_historico VALUES ({placeholders})", linhas)

    tmp = destino.with_name(f".{destino.name}.tmp-{uuid.uuid4().hex}")
    try:
        con.sql(
            f"COPY (SELECT * FROM silver_historico ORDER BY ticker, data_pregao) "
            f"TO '{tmp.as_posix()}' (FORMAT PARQUET)"
        )
        os.replace(tmp, destino)
    finally:
        if tmp.exists():
            tmp.unlink()

    return destino


def ingerir_cotahist(ano: int, arquivo: Path = CAMINHO_ATIVOS) -> Path:
    """Baixa o COTAHIST anual, preserva o ZIP original na bronze e publica a silver histórica."""
    tickers_permitidos = {a["ticker"].upper() for a in carregar_ativos(arquivo=arquivo)}

    conteudo_zip = baixar_cotahist(ano)
    salvar_bronze_cotahist(ano, data_coleta_hoje(), conteudo_zip)

    with zipfile.ZipFile(io.BytesIO(conteudo_zip)) as zf:
        texto = zf.read(zf.namelist()[0]).decode("latin-1")

    registros = parse_cotahist_texto(texto, tickers_permitidos)
    return publicar_silver_historico(ano, registros)


def criar_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Baixa o COTAHIST anual da B3 e publica a silver histórica dos ETFs acompanhados."
    )
    parser.add_argument("-a", "--ano", type=int, required=True, help="Ano do arquivo COTAHIST a processar.")
    parser.add_argument(
        "--arquivo", type=Path, default=CAMINHO_ATIVOS,
        help=f"Caminho do JSON com a lista de ativos (default: {CAMINHO_ATIVOS.name}).",
    )
    return parser


def main():
    args = criar_parser().parse_args()
    destino = ingerir_cotahist(args.ano, args.arquivo)
    print(f"Silver histórica publicada em: {destino}")


if __name__ == "__main__":
    main()
