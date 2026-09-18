import argparse
import json
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup, Tag

BASE_URL = "https://borainvestir.b3.com.br/cotacoes/{categoria}/{ticker}/"
CAMINHO_ATIVOS = Path(__file__).parent / "top_etf.json"

# Fuso da coleta: a partição date= sempre usa a data real de quando o scraping rodou
# em America/Sao_Paulo, nunca o logical date do Airflow (que é o início do intervalo
# agendado, não o instante em que a task de fato executa).
TZ_COLETA = ZoneInfo("America/Sao_Paulo")


def data_coleta_hoje() -> str:
    return datetime.now(TZ_COLETA).date().isoformat()

# Zona bronze do Data Lake: dados brutos, particionados por categoria/ticker/data. Não é
# imutável no sentido estrito — reingerir a mesma partição sobrescreve o JSON existente.
# Só chegam à silver os ativos aprovados pelo validator (ver validator.py).
RAIZ_LAKE_BRONZE = Path(__file__).parent / "datalake" / "bronze"

# Mapeia o rótulo exibido na página para o campo do dataclass.
FIELD_MAP = {
    "Valor atual (R$)": "valor_atual",
    "Mín (dia)": "minimo_dia",
    "Máx (dia)": "maximo_dia",
    "Renta. dia": "rentabilidade_dia",
    "Renta. mês": "rentabilidade_mes",
    "Renta. ano": "rentabilidade_ano",
}


@dataclass
class FonteEvidencias:
    """Evidências textuais da página, capturadas como estão (sem parse), para o validator auditar
    o comportamento do parser sem precisar refazer a requisição. None = elemento ausente na página;
    "-" (string) = elemento presente, mas a B3 publicou o traço — são sinais diferentes, não confundir.
    """
    ticker_pagina: Optional[str] = None
    categoria_pagina: Optional[str] = None
    ultima_atualizacao_texto: Optional[str] = None
    cotacoes_texto: dict = field(default_factory=dict)
    qtde_negocios_texto: Optional[str] = None
    volume_diario_texto: Optional[str] = None
    coletado_em: Optional[str] = None


@dataclass
class CotacaoAtivo:
    ticker: str
    nome: Optional[str] = None
    valor_atual: Optional[float] = None
    minimo_dia: Optional[float] = None
    maximo_dia: Optional[float] = None
    rentabilidade_dia: Optional[float] = None
    rentabilidade_mes: Optional[float] = None
    rentabilidade_ano: Optional[float] = None
    _fonte: Optional[FonteEvidencias] = None


def _para_float(texto: str) -> Optional[float]:
    if not texto:
        return None
    limpo = texto.strip().replace("%", "").replace(".", "").replace(",", ".")
    try:
        return float(limpo)
    except ValueError:
        return None


def _texto_direto(tag: Optional[Tag]) -> Optional[str]:
    """Texto do nó imediatamente dentro da tag, sem descer em elementos filhos.

    Usado no h1.asset__title, que tem o ticker como texto direto e o nome do ativo
    num <span> filho — pegar get_text() traria os dois concatenados.
    """
    if tag is None:
        return None
    texto = tag.find(string=True, recursive=False)
    return texto.strip() if texto else None


def _texto_about(soup: BeautifulSoup, prefixo_rotulo: str) -> Optional[str]:
    """Texto do <td> da linha de table.asset__about cujo <th> começa com `prefixo_rotulo`."""
    tabela = soup.select_one("table.asset__about")
    if tabela is None:
        return None
    for row in tabela.select("tr.asset__about__item"):
        th = row.select_one("th")
        td = row.select_one("td")
        if th is None or td is None:
            continue
        if th.get_text(strip=True).startswith(prefixo_rotulo):
            return td.get_text(strip=True)
    return None


def buscar_html(ticker: str, categoria: str = "etfs") -> str:
    url = BASE_URL.format(categoria=categoria, ticker=ticker.upper())
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    return response.text


def extrair_cotacao(html: str, ticker: str) -> CotacaoAtivo:
    soup = BeautifulSoup(html, "lxml")
    cotacao = CotacaoAtivo(ticker=ticker.upper())

    titulo = soup.select_one("h1.asset__title")
    if titulo:
        subtitulo = titulo.select_one(".asset__subtitle")
        cotacao.nome = (subtitulo or titulo).get_text(strip=True)

    # Texto bruto dos 6 itens (pré-parse) junto com o valor numérico, na mesma passagem.
    cotacoes_texto = {campo: None for campo in FIELD_MAP.values()}
    for item in soup.select("ul.asset__info li.asset__info__item"):
        valor_el = item.select_one(".asset__info__value")
        desc_el = item.select_one(".asset__info__desc")
        if valor_el is None or desc_el is None:
            continue
        campo = FIELD_MAP.get(desc_el.get_text(strip=True))
        if campo is None:
            continue
        texto = valor_el.get_text(strip=True)
        cotacoes_texto[campo] = texto
        setattr(cotacao, campo, _para_float(texto))

    label = soup.select_one(".asset__label")
    last_update = soup.select_one(".asset__last-update")

    cotacao._fonte = FonteEvidencias(
        ticker_pagina=_texto_direto(titulo),
        categoria_pagina=label.get_text(strip=True) if label else None,
        ultima_atualizacao_texto=last_update.get_text(strip=True) if last_update else None,
        cotacoes_texto=cotacoes_texto,
        qtde_negocios_texto=_texto_about(soup, "Quantidade de negócios"),
        volume_diario_texto=_texto_about(soup, "Volúme diário"),
        coletado_em=datetime.now(timezone.utc).isoformat(),
    )

    return cotacao


def obter_cotacao(ticker: str, categoria: str = "etfs") -> CotacaoAtivo:
    html = buscar_html(ticker, categoria)
    return extrair_cotacao(html, ticker)


def caminho_particao_bronze(categoria: str, ticker: str, data_execucao: str) -> Path:
    """Layout Hive-style (categoria/ticker=X/date=Y), já pronto para leitura por partição no futuro."""
    return RAIZ_LAKE_BRONZE / categoria / f"ticker={ticker.upper()}" / f"date={data_execucao}"


def salvar_bronze(cotacao: CotacaoAtivo, categoria: str, data_execucao: str) -> Path:
    pasta = caminho_particao_bronze(categoria, cotacao.ticker, data_execucao)
    pasta.mkdir(parents=True, exist_ok=True)
    arquivo = pasta / "cotacao.json"
    with open(arquivo, "w", encoding="utf-8") as f:
        json.dump(asdict(cotacao), f, ensure_ascii=False, indent=2)
    return arquivo


def criar_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Busca cotações de ativos na Boa Investir/B3."
    )
    parser.add_argument(
        "-t", "--ticker",
        help="Código do ativo (ex.: IVVB11). Se informado, ignora o arquivo de ativos.",
    )
    parser.add_argument(
        "-c", "--categoria",
        default="etfs",
        help="Categoria/segmento da URL da B3 (default: etfs). Só é usada junto com --ticker.",
    )
    parser.add_argument(
        "-a", "--arquivo",
        type=Path,
        default=CAMINHO_ATIVOS,
        help=f"Caminho do JSON com a lista de ativos (default: {CAMINHO_ATIVOS.name}).",
    )
    return parser


def carregar_ativos(
    ticker: Optional[str] = None,
    categoria: str = "etfs",
    arquivo: Path = CAMINHO_ATIVOS,
) -> list:
    """Carrega o universo de ativos a consultar. Sem fallback silencioso: arquivo ausente ou
    vazio é erro — processar só o que sobrar (ou um default embutido) mascararia uma falha de
    configuração como se fosse uma execução normal, com cobertura de 100% sobre um universo errado.
    """
    if ticker:
        return [{"ticker": ticker, "categoria": categoria}]
    if not arquivo.exists():
        raise FileNotFoundError(f"Arquivo de ativos não encontrado: {arquivo}")
    with open(arquivo, encoding="utf-8") as f:
        ativos = json.load(f)
    if not ativos:
        raise ValueError(f"Arquivo de ativos está vazio: {arquivo}")
    return ativos


def ingerir_ativos(
    ticker: Optional[str] = None,
    categoria: str = "etfs",
    arquivo: Path = CAMINHO_ATIVOS,
    data_execucao: Optional[str] = None,
) -> tuple[list[CotacaoAtivo], list[tuple[str, str]]]:
    """Busca cada ativo ao vivo e grava o retorno na Bronze. Usada pela CLI e pela DAG do Airflow.

    data_execucao só pode ser a data real da coleta (America/Sao_Paulo) — como a busca é sempre
    ao vivo, gravar num data_execucao diferente de hoje rotularia o snapshot atual como se fosse
    de outro dia (backfill falso). Reprocessar uma data passada é responsabilidade de
    validator/transform, que só leem o que já existe na bronze, sem nova requisição.
    """
    hoje = data_coleta_hoje()
    if data_execucao is not None and data_execucao != hoje:
        raise ValueError(
            f"data_execucao={data_execucao!r} diferente da data real da coleta ({hoje!r}). "
            "A ingestão só grava na partição de hoje — para reprocessar uma data passada, use "
            "validator.validar_execucao ou transform.transform_bronze_to_silver sobre a bronze já existente."
        )
    data_execucao = hoje

    resultados = []
    falhas = []
    for ativo in carregar_ativos(ticker, categoria, arquivo):
        ticker_ativo = ativo["ticker"]
        categoria_ativo = ativo.get("categoria", "etfs")
        try:
            cotacao = obter_cotacao(ticker_ativo, categoria_ativo)
            salvar_bronze(cotacao, categoria_ativo, data_execucao)
            resultados.append(cotacao)
        except requests.RequestException as exc:
            falhas.append((ticker_ativo, str(exc)))

    return resultados, falhas


def main():
    args = criar_parser().parse_args()

    resultados, falhas = ingerir_ativos(args.ticker, args.categoria, args.arquivo)

    for ticker_falho, motivo in falhas:
        print(f"Erro ao buscar {ticker_falho}: {motivo}", file=sys.stderr)

    for cotacao in resultados:
        print(cotacao)


if __name__ == "__main__":
    main()
