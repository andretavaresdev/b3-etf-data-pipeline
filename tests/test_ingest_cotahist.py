import ingest_cotahist


def _num(valor, largura):
    return str(int(valor)).rjust(largura, "0")


def _txt(valor, largura):
    return str(valor).ljust(largura)[:largura]


def linha_cotahist(
    *, data="20260917", codbdi="02", ticker="BOVA11", tpmerc="010",
    nome="ISHARES IBOV", especi="ETF", abertura=100.0, maximo=101.0, minimo=99.0,
    medio=100.0, fechamento=100.5, totneg=100, quatot=1000, voltot=100000.0, tipreg="01",
) -> str:
    """Monta uma linha de 245 bytes no layout oficial COTAHIST (SeriesHistoricas_Layout)."""
    linha = (
        _txt(tipreg, 2) + _txt(data, 8) + _txt(codbdi, 2) + _txt(ticker, 12) + _txt(tpmerc, 3)
        + _txt(nome, 12) + _txt(especi, 10) + _txt("", 3) + _txt("R$  ", 4)
        + _num(round(abertura * 100), 13) + _num(round(maximo * 100), 13) + _num(round(minimo * 100), 13)
        + _num(round(medio * 100), 13) + _num(round(fechamento * 100), 13)
        + _num(0, 13) + _num(0, 13)
        + _num(totneg, 5) + _num(quatot, 18) + _num(round(voltot * 100), 18)
        + _num(0, 13) + _txt("0", 1) + _txt("0" * 8, 8) + _num(1, 7) + _num(0, 13)
        + _txt("", 12) + _num(0, 3)
    )
    assert len(linha) == ingest_cotahist.TAM_REGISTRO
    return linha


def test_parse_linha_valida():
    reg = ingest_cotahist.parse_linha_cotahist(
        linha_cotahist(ticker="BOVA11", data="20260917", fechamento=182.22, abertura=181.0, maximo=182.6, minimo=180.9)
    )
    assert reg is not None
    assert reg.ticker == "BOVA11"
    assert reg.nome == "ISHARES IBOV"
    assert reg.data_pregao.isoformat() == "2026-09-17"
    assert reg.preco_fechamento == 182.22
    assert reg.preco_abertura == 181.0
    assert reg.preco_maximo == 182.6
    assert reg.preco_minimo == 180.9


def test_parse_linha_ignora_tipo_registro_diferente_de_01():
    assert ingest_cotahist.parse_linha_cotahist(linha_cotahist(tipreg="00")) is None
    assert ingest_cotahist.parse_linha_cotahist(linha_cotahist(tipreg="99")) is None


def test_parse_linha_ignora_mercado_diferente_de_vista():
    assert ingest_cotahist.parse_linha_cotahist(linha_cotahist(tpmerc="070")) is None  # opções de compra


def test_parse_linha_linha_curta_demais():
    assert ingest_cotahist.parse_linha_cotahist("01202609") is None


def test_parse_cotahist_texto_filtra_por_ticker_permitido():
    texto = "\n".join([
        linha_cotahist(ticker="BOVA11", data="20260917"),
        linha_cotahist(ticker="IVVB11", data="20260917"),
    ])
    registros = ingest_cotahist.parse_cotahist_texto(texto, {"BOVA11"})
    assert [r.ticker for r in registros] == ["BOVA11"]


def test_parse_cotahist_texto_remove_duplicidade_mesma_ticker_e_data():
    texto = "\n".join([
        linha_cotahist(ticker="BOVA11", data="20260917", fechamento=100.0),
        linha_cotahist(ticker="BOVA11", data="20260917", fechamento=999.0),
    ])
    registros = ingest_cotahist.parse_cotahist_texto(texto, {"BOVA11"})
    assert len(registros) == 1
    assert registros[0].preco_fechamento == 999.0


def test_parse_cotahist_texto_ordena_por_ticker_e_data():
    texto = "\n".join([
        linha_cotahist(ticker="IVVB11", data="20260916"),
        linha_cotahist(ticker="BOVA11", data="20260917"),
        linha_cotahist(ticker="BOVA11", data="20260916"),
    ])
    registros = ingest_cotahist.parse_cotahist_texto(texto, {"BOVA11", "IVVB11"})
    assert [(r.ticker, r.data_pregao.isoformat()) for r in registros] == [
        ("BOVA11", "2026-09-16"),
        ("BOVA11", "2026-09-17"),
        ("IVVB11", "2026-09-16"),
    ]


def test_caminho_bronze_cotahist_versiona_por_data_de_coleta():
    c1 = ingest_cotahist.caminho_bronze_cotahist(2026, "2026-09-17")
    c2 = ingest_cotahist.caminho_bronze_cotahist(2026, "2026-09-18")
    assert c1 != c2
    assert c1.parts[-3:] == ("year=2026", "collected_date=2026-09-17", "COTAHIST_A2026.ZIP")


def test_salvar_bronze_cotahist_preserva_download_de_cada_dia(bronze_cotahist_root):
    caminho_dia1 = ingest_cotahist.salvar_bronze_cotahist(2026, "2026-09-17", b"conteudo do dia 17")
    caminho_dia2 = ingest_cotahist.salvar_bronze_cotahist(2026, "2026-09-18", b"conteudo do dia 18")

    assert caminho_dia1 != caminho_dia2
    assert caminho_dia1.exists() and caminho_dia2.exists()
    assert caminho_dia1.read_bytes() == b"conteudo do dia 17"
    assert caminho_dia2.read_bytes() == b"conteudo do dia 18"


def test_salvar_bronze_cotahist_mesma_data_sobrescreve(bronze_cotahist_root):
    ingest_cotahist.salvar_bronze_cotahist(2026, "2026-09-17", b"versao 1")
    caminho = ingest_cotahist.salvar_bronze_cotahist(2026, "2026-09-17", b"versao 2")
    assert caminho.read_bytes() == b"versao 2"


def test_publicar_silver_historico_atomico_e_idempotente(historico_root):
    registros = ingest_cotahist.parse_cotahist_texto(
        linha_cotahist(ticker="BOVA11", data="20260917"), {"BOVA11"}
    )
    destino1 = ingest_cotahist.publicar_silver_historico(2026, registros)
    conteudo1 = destino1.read_bytes()
    destino2 = ingest_cotahist.publicar_silver_historico(2026, registros)
    conteudo2 = destino2.read_bytes()

    assert destino1 == destino2
    assert conteudo1 == conteudo2
    assert list(destino1.parent.glob(".*.tmp-*")) == []
