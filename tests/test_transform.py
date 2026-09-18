import duckdb
import pytest

import transform
import validator

DATA = "2026-01-01"


def test_transform_publica_parquet_com_ativos_validos(silver_root, ativos_json, escrever_bronze):
    escrever_bronze("OK11", DATA)
    escrever_bronze("PARC11", DATA, rentabilidade_ano=None)
    arquivo = ativos_json(["OK11", "PARC11"])

    destino = transform.transform_bronze_to_silver(DATA, arquivo=arquivo)

    assert destino.exists()
    n = duckdb.connect().sql(f"SELECT count(*) FROM read_parquet('{destino.as_posix()}')").fetchone()[0]
    assert n == 2


def test_transform_exclui_ativos_sem_cotacao(silver_root, ativos_json, escrever_bronze):
    # Precisa de cobertura >= 95% pra isolar o teste da regra de exclusão do gate de cobertura
    # (ver test_transform_bloqueia_cobertura_abaixo_do_minimo): 39 ok + 1 sem_cotacao = 97,5%.
    tickers_ok = [f"OK{i}" for i in range(39)]
    for t in tickers_ok:
        escrever_bronze(t, DATA)
    escrever_bronze(
        "SEMC11", DATA,
        valor_atual=None, minimo_dia=None, maximo_dia=None,
        rentabilidade_dia=None, rentabilidade_mes=None, rentabilidade_ano=None,
        fonte_overrides={
            "cotacoes_texto": {c: "-" for c in validator.CAMPOS_COTACAO},
            "qtde_negocios_texto": "-",
            "volume_diario_texto": "-",
        },
    )
    arquivo = ativos_json(tickers_ok + ["SEMC11"])

    destino = transform.transform_bronze_to_silver(DATA, arquivo=arquivo)

    tickers_publicados = {
        row[0] for row in duckdb.connect().sql(f"SELECT ticker FROM read_parquet('{destino.as_posix()}')").fetchall()
    }
    assert tickers_publicados == set(tickers_ok)
    assert "SEMC11" not in tickers_publicados


def test_transform_bloqueia_quando_ha_falha(silver_root, ativos_json, escrever_bronze):
    escrever_bronze("OK11", DATA)
    escrever_bronze("RUIM11", DATA, fonte_overrides={"ticker_pagina": "OUTRO99"})
    arquivo = ativos_json(["OK11", "RUIM11"])

    with pytest.raises(RuntimeError, match="falha/indeterminado"):
        transform.transform_bronze_to_silver(DATA, arquivo=arquivo)

    assert not list(silver_root.rglob("*.parquet"))


def test_transform_bloqueia_quando_ha_indeterminado(silver_root, ativos_json, escrever_bronze):
    escrever_bronze("OK11", DATA)
    escrever_bronze(
        "IND11", DATA,
        valor_atual=None, minimo_dia=None, maximo_dia=None,
        rentabilidade_dia=None, rentabilidade_mes=None, rentabilidade_ano=None,
        fonte_overrides={"cotacoes_texto": {}, "qtde_negocios_texto": None, "volume_diario_texto": None},
    )
    arquivo = ativos_json(["OK11", "IND11"])

    with pytest.raises(RuntimeError, match="falha/indeterminado"):
        transform.transform_bronze_to_silver(DATA, arquivo=arquivo)


def test_transform_bloqueia_cobertura_abaixo_do_minimo(silver_root, ativos_json, escrever_bronze):
    # 1 ok em 4 esperados = 25%, bem abaixo dos 95% — sem nenhum "falha"/"indeterminado" individual
    escrever_bronze("OK11", DATA)
    for i in range(3):
        escrever_bronze(
            f"SEMC{i}", DATA,
            valor_atual=None, minimo_dia=None, maximo_dia=None,
            rentabilidade_dia=None, rentabilidade_mes=None, rentabilidade_ano=None,
            fonte_overrides={
                "cotacoes_texto": {c: "-" for c in validator.CAMPOS_COTACAO},
                "qtde_negocios_texto": "-",
                "volume_diario_texto": "-",
            },
        )
    arquivo = ativos_json(["OK11", "SEMC0", "SEMC1", "SEMC2"])

    with pytest.raises(RuntimeError, match="[Cc]obertura"):
        transform.transform_bronze_to_silver(DATA, arquivo=arquivo)


def test_transform_atomico_nao_deixa_arquivo_final_se_replace_falhar(
    silver_root, ativos_json, escrever_bronze, monkeypatch
):
    escrever_bronze("OK11", DATA)
    arquivo = ativos_json(["OK11"])

    def replace_com_falha(src, dst):
        raise OSError("falha simulada no rename atômico")

    monkeypatch.setattr(transform.os, "replace", replace_com_falha)

    with pytest.raises(OSError):
        transform.transform_bronze_to_silver(DATA, arquivo=arquivo)

    destino = transform.caminho_particao_silver(DATA)
    assert not destino.exists()
    if destino.parent.exists():
        assert list(destino.parent.glob(".*.tmp-*")) == []


def test_transform_idempotente(silver_root, ativos_json, escrever_bronze):
    escrever_bronze("OK11", DATA)
    arquivo = ativos_json(["OK11"])

    destino1 = transform.transform_bronze_to_silver(DATA, arquivo=arquivo)
    conteudo1 = destino1.read_bytes()

    destino2 = transform.transform_bronze_to_silver(DATA, arquivo=arquivo)
    conteudo2 = destino2.read_bytes()

    assert destino1 == destino2
    assert conteudo1 == conteudo2


def test_transform_sem_ativos_elegiveis_nao_publica(silver_root, ativos_json, escrever_bronze):
    escrever_bronze(
        "SEMC11", DATA,
        valor_atual=None, minimo_dia=None, maximo_dia=None,
        rentabilidade_dia=None, rentabilidade_mes=None, rentabilidade_ano=None,
        fonte_overrides={
            "cotacoes_texto": {c: "-" for c in validator.CAMPOS_COTACAO},
            "qtde_negocios_texto": "-",
            "volume_diario_texto": "-",
        },
    )
    arquivo = ativos_json(["SEMC11"])

    with pytest.raises(RuntimeError, match="[Cc]obertura"):
        transform.transform_bronze_to_silver(DATA, arquivo=arquivo)


def test_caminho_particao_silver_layout():
    destino = transform.caminho_particao_silver("2026-09-18")
    assert destino.parts[-4:] == ("etfs", "year=2026", "month=09", "etfs_2026-09-18.parquet")
