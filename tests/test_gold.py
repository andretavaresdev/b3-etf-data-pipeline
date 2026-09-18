from datetime import date, timedelta

import duckdb

import gold

D0 = date(2026, 1, 1)


def _serie(precos, data_inicial=D0):
    return [(data_inicial + timedelta(days=i), p) for i, p in enumerate(precos)]


def test_metricas_serie_vazia():
    m = gold.calcular_metricas_ticker([])
    assert m["qtd_observacoes"] == 0
    assert m["ultimo_preco"] is None
    assert m["periodo_inicio"] is None and m["periodo_fim"] is None
    assert all(m[f"retorno_{j}"] is None for j in gold.JANELAS_RETORNO)
    assert m["volatilidade_anualizada"] is None
    assert m["drawdown_atual"] is None
    assert m["maximo_drawdown"] is None


def test_metricas_serie_crescente_sem_drawdown():
    serie = _serie([100 + i for i in range(30)])  # sempre subindo
    m = gold.calcular_metricas_ticker(serie)
    assert m["qtd_observacoes"] == 30
    assert m["ultimo_preco"] == 129
    assert m["retorno_21"] > 0
    assert m["drawdown_atual"] == 0.0
    assert m["maximo_drawdown"] == 0.0


def test_metricas_serie_decrescente_drawdown_atual_igual_maximo():
    serie = _serie([130 - i for i in range(30)])  # sempre caindo
    m = gold.calcular_metricas_ticker(serie)
    assert m["retorno_21"] < 0
    assert m["drawdown_atual"] < 0
    # queda monotônica: o pior momento é sempre o mais recente
    assert m["drawdown_atual"] == m["maximo_drawdown"]


def test_metricas_serie_constante():
    serie = _serie([100.0] * 30)
    m = gold.calcular_metricas_ticker(serie)
    assert m["retorno_21"] == 0.0
    assert m["volatilidade_anualizada"] == 0.0
    assert m["drawdown_atual"] == 0.0
    assert m["maximo_drawdown"] == 0.0


def test_metricas_historico_insuficiente_todas_janelas_none():
    serie = _serie([100 + i for i in range(10)])  # menos que a menor janela (21)
    m = gold.calcular_metricas_ticker(serie)
    assert all(m[f"retorno_{j}"] is None for j in gold.JANELAS_RETORNO)
    assert m["volatilidade_anualizada"] is None
    # drawdown nao depende de janela fixa, continua calculavel
    assert m["drawdown_atual"] is not None


def test_metricas_janela_boundary_exata():
    serie_21 = _serie([100 + i for i in range(21)])
    assert gold.calcular_metricas_ticker(serie_21)["retorno_21"] is None  # precisa de n > 21

    serie_22 = _serie([100 + i for i in range(22)])
    assert gold.calcular_metricas_ticker(serie_22)["retorno_21"] is not None


def test_metricas_volatilidade_minimo_observacoes():
    serie_21 = _serie([100 + (i % 3) for i in range(21)])  # 20 log-retornos, abaixo do minimo
    assert gold.calcular_metricas_ticker(serie_21)["volatilidade_anualizada"] is None

    serie_22 = _serie([100 + (i % 3) for i in range(22)])  # 21 log-retornos, no minimo
    assert gold.calcular_metricas_ticker(serie_22)["volatilidade_anualizada"] is not None


def test_metricas_maximo_drawdown_captura_pior_ponto_no_meio():
    # sobe, desaba no meio, recupera parcialmente
    serie = _serie([100, 110, 120, 60, 90, 95])
    m = gold.calcular_metricas_ticker(serie)
    assert m["maximo_drawdown"] == 60 / 120 - 1
    assert m["drawdown_atual"] == 95 / 120 - 1
    assert m["maximo_drawdown"] < m["drawdown_atual"]


def test_metricas_datas_ausentes_contam_por_observacao_nao_por_calendario():
    # pula fins de semana/feriados de propósito: retorno_N conta pregões (linhas), não dias corridos
    datas_uteis = []
    d = D0
    while len(datas_uteis) < 22:
        if d.weekday() < 5:  # só dias úteis, simula ausência de fins de semana
            datas_uteis.append(d)
        d += timedelta(days=1)
    serie = [(dt, 100 + i) for i, dt in enumerate(datas_uteis)]

    m = gold.calcular_metricas_ticker(serie)
    assert m["qtd_observacoes"] == 22
    assert m["retorno_21"] == 121 / 100 - 1  # baseado em posição na série, não em diferença de datas
    assert (m["periodo_fim"] - m["periodo_inicio"]).days > 22  # calendário é maior que os 22 pregões


def test_metricas_duplicidade_na_serie_nao_quebra():
    serie = _serie([100, 100, 101])  # mesma data duas vezes seria filtrada antes, na silver
    m = gold.calcular_metricas_ticker(serie)
    assert m["qtd_observacoes"] == 3


def test_transform_gold_publica_metricas_e_atomico_idempotente(gold_root, escrever_silver_historico, ativos_json):
    linhas = [
        {"ticker": "BOVA11", "data_pregao": D0 + timedelta(days=i), "preco_fechamento": 100 + i}
        for i in range(25)
    ]
    escrever_silver_historico(2026, linhas)
    arquivo = ativos_json(["BOVA11"])
    data_particao = (D0 + timedelta(days=24)).isoformat()

    destino1 = gold.transform_silver_to_gold(data_particao, arquivo=arquivo)
    conteudo1 = destino1.read_bytes()
    destino2 = gold.transform_silver_to_gold(data_particao, arquivo=arquivo)
    conteudo2 = destino2.read_bytes()

    assert destino1 == destino2
    assert conteudo1 == conteudo2
    assert list(destino1.parent.glob(".*.tmp-*")) == []

    linha = duckdb.connect().sql(
        f"SELECT qtd_observacoes, ultimo_preco FROM read_parquet('{destino1.as_posix()}') WHERE ticker = 'BOVA11'"
    ).fetchone()
    assert linha == (25, 124.0)


def test_transform_gold_nunca_usa_datas_futuras(gold_root, escrever_silver_historico, ativos_json):
    linhas = [
        {"ticker": "BOVA11", "data_pregao": D0 + timedelta(days=i), "preco_fechamento": 100 + i}
        for i in range(10)
    ]
    # linhas "futuras" em relacao a data_particao escolhida abaixo
    linhas += [
        {"ticker": "BOVA11", "data_pregao": D0 + timedelta(days=i), "preco_fechamento": 9999}
        for i in range(10, 15)
    ]
    escrever_silver_historico(2026, linhas)
    arquivo = ativos_json(["BOVA11"])
    data_particao = (D0 + timedelta(days=9)).isoformat()  # so ve os 10 primeiros pontos

    destino = gold.transform_silver_to_gold(data_particao, arquivo=arquivo)
    qtd, ultimo, periodo_fim = duckdb.connect().sql(
        f"SELECT qtd_observacoes, ultimo_preco, periodo_fim FROM read_parquet('{destino.as_posix()}') "
        f"WHERE ticker = 'BOVA11'"
    ).fetchone()
    assert qtd == 10
    assert ultimo == 109  # ultimo preco dentro da janela permitida, nao o 9999 futuro
    assert periodo_fim.isoformat() == data_particao


def test_transform_gold_ticker_sem_historico_aparece_com_nulos(gold_root, escrever_silver_historico, ativos_json):
    escrever_silver_historico(2026, [
        {"ticker": "BOVA11", "data_pregao": D0, "preco_fechamento": 100.0},
    ])
    arquivo = ativos_json(["BOVA11", "SEMHIST11"])

    destino = gold.transform_silver_to_gold(D0.isoformat(), arquivo=arquivo)
    linhas = duckdb.connect().sql(
        f"SELECT ticker, status_historico, qtd_observacoes, ultimo_preco "
        f"FROM read_parquet('{destino.as_posix()}') ORDER BY ticker"
    ).fetchall()
    assert linhas == [
        ("BOVA11", gold.STATUS_HISTORICO_OK, 1, 100.0),
        ("SEMHIST11", gold.STATUS_HISTORICO_AUSENTE, 0, None),
    ]


def test_transform_gold_left_join_nunca_omite_ticker_do_universo(gold_root, escrever_silver_historico, ativos_json):
    # universo de 5: 3 com histórico, 2 sem — a gold tem que sair com as 5 linhas, nunca menos.
    linhas_historico = []
    for ticker in ("A11", "B11", "C11"):
        linhas_historico += [{"ticker": ticker, "data_pregao": D0, "preco_fechamento": 10.0}]
    escrever_silver_historico(2026, linhas_historico)
    arquivo = ativos_json(["A11", "B11", "C11", "D11", "E11"])

    destino = gold.transform_silver_to_gold(D0.isoformat(), arquivo=arquivo)
    linhas = duckdb.connect().sql(
        f"SELECT ticker, status_historico FROM read_parquet('{destino.as_posix()}') ORDER BY ticker"
    ).fetchall()

    assert len(linhas) == 5
    assert [t for t, _ in linhas] == ["A11", "B11", "C11", "D11", "E11"]
    com_historico = [t for t, s in linhas if s == gold.STATUS_HISTORICO_OK]
    sem_historico = [t for t, s in linhas if s == gold.STATUS_HISTORICO_AUSENTE]
    assert com_historico == ["A11", "B11", "C11"]
    assert sem_historico == ["D11", "E11"]


def test_transform_gold_sem_nenhum_arquivo_historico(gold_root, historico_root, ativos_json):
    arquivo = ativos_json(["BOVA11"])
    destino = gold.transform_silver_to_gold(D0.isoformat(), arquivo=arquivo)
    linha = duckdb.connect().sql(
        f"SELECT qtd_observacoes, status_historico FROM read_parquet('{destino.as_posix()}')"
    ).fetchone()
    assert linha == (0, gold.STATUS_HISTORICO_AUSENTE)


def test_caminho_particao_gold_layout():
    destino = gold.caminho_particao_gold("2026-09-18")
    assert destino.parts[-3:] == ("etfs", "as_of_date=2026-09-18", "gold_etfs_2026-09-18.parquet")
