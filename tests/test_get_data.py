import pytest

import get_data

HTML_OK = """
<html><body>
<h1 class="asset__title">TEST11<span class="asset__subtitle d-block">Teste Fundo de Índice</span></h1>
<p class="asset__desc">
  <span class="asset__label">ETFs de Ações</span>
  <span class="asset__last-update">Atualizado às 18/09/2026 15h00. Delay 15 min.</span>
</p>
<ul class="asset__info">
  <li class="asset__info__item"><span class="asset__info__value">100,00</span><p class="asset__info__desc">Valor atual (R$)</p></li>
  <li class="asset__info__item"><span class="asset__info__value">99,00</span><p class="asset__info__desc">Mín (dia)</p></li>
  <li class="asset__info__item"><span class="asset__info__value">101,00</span><p class="asset__info__desc">Máx (dia)</p></li>
  <li class="asset__info__item"><span class="asset__info__value">1,00%</span><p class="asset__info__desc">Renta. dia</p></li>
  <li class="asset__info__item"><span class="asset__info__value">2,00%</span><p class="asset__info__desc">Renta. mês</p></li>
  <li class="asset__info__item"><span class="asset__info__value">10,00%</span><p class="asset__info__desc">Renta. ano</p></li>
</ul>
<table class="asset__about">
  <tr class="asset__about__item"><th class="asset__about__item__title">Quantidade de negócios (média diária 6 meses)</th><td>123</td></tr>
  <tr class="asset__about__item"><th class="asset__about__item__title">Volúme diário (média diária 6 meses)</th><td>R$ 1.000,00</td></tr>
</table>
</body></html>
"""

HTML_SEM_COTACAO = """
<html><body>
<h1 class="asset__title">SEMC11<span class="asset__subtitle d-block">Sem Cotação Fundo</span></h1>
<p class="asset__desc">
  <span class="asset__label">ETFs de Ações</span>
</p>
<ul class="asset__info">
  <li class="asset__info__item"><span class="asset__info__value">-</span><p class="asset__info__desc">Valor atual (R$)</p></li>
  <li class="asset__info__item"><span class="asset__info__value">-</span><p class="asset__info__desc">Mín (dia)</p></li>
  <li class="asset__info__item"><span class="asset__info__value">-</span><p class="asset__info__desc">Máx (dia)</p></li>
  <li class="asset__info__item"><span class="asset__info__value">-</span><p class="asset__info__desc">Renta. dia</p></li>
  <li class="asset__info__item"><span class="asset__info__value">-</span><p class="asset__info__desc">Renta. mês</p></li>
  <li class="asset__info__item"><span class="asset__info__value">-</span><p class="asset__info__desc">Renta. ano</p></li>
</ul>
<table class="asset__about">
  <tr class="asset__about__item"><th class="asset__about__item__title">Quantidade de negócios (média diária 6 meses)</th><td>-</td></tr>
  <tr class="asset__about__item"><th class="asset__about__item__title">Volúme diário (média diária 6 meses)</th><td>-</td></tr>
</table>
</body></html>
"""


def test_extrair_cotacao_campos_numericos():
    cotacao = get_data.extrair_cotacao(HTML_OK, "TEST11")
    assert cotacao.ticker == "TEST11"
    assert cotacao.nome == "Teste Fundo de Índice"
    assert cotacao.valor_atual == 100.0
    assert cotacao.minimo_dia == 99.0
    assert cotacao.maximo_dia == 101.0
    assert cotacao.rentabilidade_dia == 1.0
    assert cotacao.rentabilidade_mes == 2.0
    assert cotacao.rentabilidade_ano == 10.0


def test_extrair_cotacao_fonte_evidencias():
    cotacao = get_data.extrair_cotacao(HTML_OK, "TEST11")
    fonte = cotacao._fonte
    assert fonte.ticker_pagina == "TEST11"
    assert fonte.categoria_pagina == "ETFs de Ações"
    assert fonte.cotacoes_texto["valor_atual"] == "100,00"
    assert fonte.qtde_negocios_texto == "123"
    assert fonte.volume_diario_texto == "R$ 1.000,00"
    assert fonte.coletado_em is not None


def test_extrair_cotacao_sem_cotacao_preserva_tracos_explicitos():
    cotacao = get_data.extrair_cotacao(HTML_SEM_COTACAO, "SEMC11")
    assert cotacao.valor_atual is None
    assert cotacao.rentabilidade_ano is None
    assert cotacao._fonte.cotacoes_texto["valor_atual"] == "-"
    assert cotacao._fonte.qtde_negocios_texto == "-"
    assert cotacao._fonte.volume_diario_texto == "-"


def test_extrair_cotacao_elemento_ausente_nao_vira_traco():
    # sem a tag h1.asset__title inteira -> ticker_pagina deve ser None, nunca "-"
    html_sem_titulo = HTML_OK.replace(
        '<h1 class="asset__title">TEST11<span class="asset__subtitle d-block">Teste Fundo de Índice</span></h1>',
        "",
    )
    cotacao = get_data.extrair_cotacao(html_sem_titulo, "TEST11")
    assert cotacao._fonte.ticker_pagina is None


def test_carregar_ativos_arquivo_ausente_leva_erro(tmp_path):
    with pytest.raises(FileNotFoundError):
        get_data.carregar_ativos(arquivo=tmp_path / "nao_existe.json")


def test_carregar_ativos_arquivo_vazio_leva_erro(tmp_path):
    caminho = tmp_path / "vazio.json"
    caminho.write_text("[]", encoding="utf-8")
    with pytest.raises(ValueError):
        get_data.carregar_ativos(arquivo=caminho)


def test_carregar_ativos_arquivo_valido(tmp_path):
    caminho = tmp_path / "ativos.json"
    caminho.write_text('[{"ticker": "AAA11", "categoria": "etfs"}]', encoding="utf-8")
    ativos = get_data.carregar_ativos(arquivo=caminho)
    assert ativos == [{"ticker": "AAA11", "categoria": "etfs"}]


def test_ingerir_ativos_recusa_data_execucao_no_passado():
    """A ingestão sempre faz scraping ao vivo — gravar isso numa partição passada seria um
    backfill falso (snapshot de hoje rotulado como se fosse de outro dia)."""
    with pytest.raises(ValueError):
        get_data.ingerir_ativos(ticker="QUALQUER11", data_execucao="2000-01-01")


def test_ingerir_ativos_recusa_data_execucao_no_futuro():
    with pytest.raises(ValueError):
        get_data.ingerir_ativos(ticker="QUALQUER11", data_execucao="2999-01-01")
