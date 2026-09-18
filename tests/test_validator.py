import validator

DATA = "2026-01-01"


def test_status_ok(escrever_bronze):
    escrever_bronze("OK11", DATA)
    r = validator.validar_ativo_bronze("OK11", "etfs", DATA)
    assert r.status == "ok"


def test_status_dados_parciais_falta_rentabilidade_ano(escrever_bronze):
    escrever_bronze("PARC11", DATA, rentabilidade_ano=None)
    r = validator.validar_ativo_bronze("PARC11", "etfs", DATA)
    assert r.status == "dados_parciais"


def test_status_sem_cotacao_quando_todos_tracos_sao_explicitos(escrever_bronze):
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
    r = validator.validar_ativo_bronze("SEMC11", "etfs", DATA)
    assert r.status == "sem_cotacao"


def test_status_indeterminado_sem_evidencia_de_traco(escrever_bronze):
    escrever_bronze(
        "IND11", DATA,
        valor_atual=None, minimo_dia=None, maximo_dia=None,
        rentabilidade_dia=None, rentabilidade_mes=None, rentabilidade_ano=None,
        fonte_overrides={"cotacoes_texto": {}, "qtde_negocios_texto": None, "volume_diario_texto": None},
    )
    r = validator.validar_ativo_bronze("IND11", "etfs", DATA)
    assert r.status == "indeterminado"


def test_status_indeterminado_traco_parcial_nao_vira_sem_cotacao(escrever_bronze):
    # os 6 itens sao "-", mas a tabela about nao tem evidencia (None) -> nao e sem_cotacao completo
    escrever_bronze(
        "MISTO11", DATA,
        valor_atual=None, minimo_dia=None, maximo_dia=None,
        rentabilidade_dia=None, rentabilidade_mes=None, rentabilidade_ano=None,
        fonte_overrides={
            "cotacoes_texto": {c: "-" for c in validator.CAMPOS_COTACAO},
            "qtde_negocios_texto": None,
            "volume_diario_texto": "-",
        },
    )
    r = validator.validar_ativo_bronze("MISTO11", "etfs", DATA)
    assert r.status == "indeterminado"


def test_status_falha_arquivo_ausente(bronze_root):
    r = validator.validar_ativo_bronze("NUNCA11", "etfs", DATA)
    assert r.status == "falha"
    assert "ausente" in r.motivo.lower()


def test_status_falha_json_invalido(bronze_root):
    pasta = bronze_root / "etfs" / "ticker=RUIM11" / f"date={DATA}"
    pasta.mkdir(parents=True)
    (pasta / "cotacao.json").write_text("{isso nao e json valido", encoding="utf-8")
    r = validator.validar_ativo_bronze("RUIM11", "etfs", DATA)
    assert r.status == "falha"


def test_status_falha_ticker_json_divergente(bronze_root):
    # ticker do JSON ("OUTRO99") diferente da partição em que o arquivo está gravado.
    pasta = bronze_root / "etfs" / "ticker=DIVERGE11" / f"date={DATA}"
    pasta.mkdir(parents=True)
    dados = {
        "ticker": "OUTRO99",
        "nome": "Ativo Divergente",
        "valor_atual": 10.0, "minimo_dia": 9.5, "maximo_dia": 10.5, "rentabilidade_dia": 0.5,
        "rentabilidade_mes": 1.0, "rentabilidade_ano": 5.0,
        "_fonte": {"ticker_pagina": "DIVERGE11", "coletado_em": "2026-09-18T12:00:00+00:00"},
    }
    import json
    (pasta / "cotacao.json").write_text(json.dumps(dados), encoding="utf-8")

    r = validator.validar_ativo_bronze("DIVERGE11", "etfs", DATA)
    assert r.status == "falha"
    assert "ticker do json" in r.motivo.lower()


def test_status_falha_ticker_pagina_divergente(escrever_bronze):
    escrever_bronze("DIVERGE12", DATA, fonte_overrides={"ticker_pagina": "OUTRO99"})
    r = validator.validar_ativo_bronze("DIVERGE12", "etfs", DATA)
    assert r.status == "falha"


def test_status_falha_criticos_parcialmente_ausentes(escrever_bronze):
    escrever_bronze("PARCIALCRIT11", DATA, minimo_dia=None)
    r = validator.validar_ativo_bronze("PARCIALCRIT11", "etfs", DATA)
    assert r.status == "falha"


def test_status_falha_nome_ausente(escrever_bronze):
    escrever_bronze("SEMNOME11", DATA, nome="")
    r = validator.validar_ativo_bronze("SEMNOME11", "etfs", DATA)
    assert r.status == "falha"
    assert "nome" in r.motivo.lower()


def test_status_falha_coletado_em_ausente(escrever_bronze):
    escrever_bronze("SEMCOLETA11", DATA, fonte_overrides={"coletado_em": None})
    r = validator.validar_ativo_bronze("SEMCOLETA11", "etfs", DATA)
    assert r.status == "falha"


def test_status_falha_coletado_em_formato_invalido(escrever_bronze):
    escrever_bronze("COLETAINVAL11", DATA, fonte_overrides={"coletado_em": "18/09/2026 15:00"})
    r = validator.validar_ativo_bronze("COLETAINVAL11", "etfs", DATA)
    assert r.status == "falha"


def test_status_falha_valor_nao_positivo(escrever_bronze):
    escrever_bronze("NEGATIVO11", DATA, valor_atual=-5.0)
    r = validator.validar_ativo_bronze("NEGATIVO11", "etfs", DATA)
    assert r.status == "falha"


def test_status_falha_valor_zero(escrever_bronze):
    escrever_bronze("ZERO11", DATA, minimo_dia=0)
    r = validator.validar_ativo_bronze("ZERO11", "etfs", DATA)
    assert r.status == "falha"


def test_status_falha_minimo_maior_que_maximo(escrever_bronze):
    escrever_bronze("INVERTIDO11", DATA, minimo_dia=20.0, maximo_dia=10.0)
    r = validator.validar_ativo_bronze("INVERTIDO11", "etfs", DATA)
    assert r.status == "falha"


def test_status_falha_tipo_numerico_invalido(escrever_bronze):
    escrever_bronze("TIPOERRADO11", DATA, valor_atual="dez")
    r = validator.validar_ativo_bronze("TIPOERRADO11", "etfs", DATA)
    assert r.status == "falha"


def test_status_ok_rentabilidade_negativa_e_permitida(escrever_bronze):
    # rentabilidade pode ser negativa (dia de queda) — só preço não pode
    escrever_bronze("QUEDA11", DATA, rentabilidade_dia=-3.5, rentabilidade_mes=-10.0, rentabilidade_ano=-20.0)
    r = validator.validar_ativo_bronze("QUEDA11", "etfs", DATA)
    assert r.status == "ok"


def test_calcular_cobertura():
    resultados = [
        validator.ResultadoValidacao("A", "-", "ok"),
        validator.ResultadoValidacao("B", "-", "dados_parciais"),
        validator.ResultadoValidacao("C", "-", "sem_cotacao"),
        validator.ResultadoValidacao("D", "-", "falha"),
    ]
    assert validator.calcular_cobertura(resultados) == 0.5


def test_calcular_cobertura_lista_vazia():
    assert validator.calcular_cobertura([]) == 0.0


def test_validar_execucao_usa_arquivo_de_ativos_informado(escrever_bronze, ativos_json):
    escrever_bronze("A11", DATA)
    escrever_bronze("B11", DATA)
    arquivo = ativos_json(["A11", "B11"])
    resultados = validator.validar_execucao(data_execucao=DATA, arquivo=arquivo)
    assert {r.ticker for r in resultados} == {"A11", "B11"}
    assert all(r.status == "ok" for r in resultados)
