import json

import pytest

import get_data
import validator
import transform


@pytest.fixture
def bronze_root(tmp_path, monkeypatch):
    """Isola a bronze num diretório temporário, refletido em todos os módulos que a referenciam."""
    raiz = tmp_path / "bronze"
    monkeypatch.setattr(get_data, "RAIZ_LAKE_BRONZE", raiz)
    monkeypatch.setattr(validator, "RAIZ_LAKE_BRONZE", raiz)
    return raiz


@pytest.fixture
def silver_root(tmp_path, monkeypatch):
    """Isola a silver num diretório temporário."""
    raiz = tmp_path / "silver"
    monkeypatch.setattr(transform, "RAIZ_LAKE_SILVER", raiz)
    return raiz


@pytest.fixture
def ativos_json(tmp_path):
    """Cria um arquivo de universo de ativos temporário e devolve o Path."""
    def _criar(tickers, categoria="etfs"):
        caminho = tmp_path / "ativos_teste.json"
        caminho.write_text(
            json.dumps([{"ticker": t, "categoria": categoria} for t in tickers]),
            encoding="utf-8",
        )
        return caminho
    return _criar


@pytest.fixture
def escrever_bronze(bronze_root):
    """Grava um cotacao.json sintético na bronze isolada, com defaults válidos (status "ok")
    que os testes sobrescrevem campo a campo pra montar o cenário que precisam.
    """
    def _escrever(ticker, data, categoria="etfs", fonte_overrides=None, **overrides):
        fonte = {
            "ticker_pagina": ticker,
            "categoria_pagina": "ETFs de Ações",
            "ultima_atualizacao_texto": "Atualizado às 18/09/2026 15h00. Delay 15 min.",
            "cotacoes_texto": {
                "valor_atual": "10,00",
                "minimo_dia": "9,50",
                "maximo_dia": "10,50",
                "rentabilidade_dia": "0,50%",
                "rentabilidade_mes": "1,00%",
                "rentabilidade_ano": "5,00%",
            },
            "qtde_negocios_texto": "100",
            "volume_diario_texto": "R$ 1.000,00",
            "coletado_em": "2026-09-18T12:00:00+00:00",
        }
        if fonte_overrides:
            fonte.update(fonte_overrides)

        dados = {
            "ticker": ticker,
            "nome": f"{ticker} Fundo de Índice",
            "valor_atual": 10.0,
            "minimo_dia": 9.5,
            "maximo_dia": 10.5,
            "rentabilidade_dia": 0.5,
            "rentabilidade_mes": 1.0,
            "rentabilidade_ano": 5.0,
            "_fonte": fonte,
        }
        dados.update(overrides)

        pasta = bronze_root / categoria / f"ticker={ticker}" / f"date={data}"
        pasta.mkdir(parents=True, exist_ok=True)
        caminho = pasta / "cotacao.json"
        caminho.write_text(json.dumps(dados, ensure_ascii=False), encoding="utf-8")
        return caminho

    return _escrever


@pytest.fixture
def escrever_bronze_sem_cotacao(escrever_bronze):
    """Grava bronze no formato sem_cotacao: campos None, evidenciados por "-"."""
    def _escrever(ticker, data, categoria="etfs"):
        return escrever_bronze(
            ticker, data, categoria=categoria,
            valor_atual=None, minimo_dia=None, maximo_dia=None,
            rentabilidade_dia=None, rentabilidade_mes=None, rentabilidade_ano=None,
            fonte_overrides={
                "cotacoes_texto": {c: "-" for c in validator.CAMPOS_COTACAO},
                "qtde_negocios_texto": "-",
                "volume_diario_texto": "-",
            },
        )
    return _escrever
