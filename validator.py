import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

from get_data import CAMINHO_ATIVOS, RAIZ_LAKE_BRONZE, carregar_ativos, data_coleta_hoje

# Campos default
CAMPOS_CRITICOS = ("valor_atual", "minimo_dia", "maximo_dia", "rentabilidade_dia")
# Opcionais, pode incluir recém-listados que ainda não completaram 1 mês/ano de histórico.
CAMPOS_OPCIONAIS = ("rentabilidade_mes", "rentabilidade_ano")
# Os 6 itens de ul.asset__info, na ordem em que _fonte.cotacoes_texto os guarda.
CAMPOS_COTACAO = CAMPOS_CRITICOS + CAMPOS_OPCIONAIS

# Únicos status que impedem a execução de seguir para a silver.
STATUS_BLOQUEIAM_DAG = ("falha", "indeterminado")

# Cobertura = (ok + dados_parciais) / quantidade esperada de ativos. Abaixo disso, mesmo sem
# nenhum "falha"/"indeterminado" individual, a execução não está boa o bastante pra silver
# (ex.: excesso de "sem_cotacao" indicando problema amplo na fonte, não nos ativos em si).
COBERTURA_MINIMA = 0.95
STATUS_COBERTURA = ("ok", "dados_parciais")


@dataclass
class ResultadoValidacao:
    ticker: str
    caminho: str
    status: str  # "ok", "dados_parciais", "sem_cotacao", "indeterminado" ou "falha"
    motivo: str = ""


def caminho_bronze(categoria: str, ticker: str, data_execucao: str) -> Path:
    return RAIZ_LAKE_BRONZE / categoria / f"ticker={ticker.upper()}" / f"date={data_execucao}" / "cotacao.json"


def _texto_e_traco(valor) -> bool:
    """True só quando o valor é a string literal "-" (evidência de "sem cotação" publicada pela B3).
    None (chave/elemento ausente) NUNCA conta como "-" — são sinais diferentes: um é a B3 dizendo
    explicitamente que não há cotação, o outro é ausência de evidência (possível parser quebrado).
    """
    return valor == "-"


def validar_ativo_bronze(ticker: str, categoria: str, data_execucao: str) -> ResultadoValidacao:
    """Classifica o que a ingestão gravou na bronze pra esse ticker/data, sem bater na rede.
    Status possíveis: ok, os_parciaisdad, sem_cotacao, indeterminado, falha.
    Só "falha" e "indeterminado" bloqueiam a DAG (ver STATUS_BLOQUEIAM_DAG) — só "ok" e
    "dados_parciais" são publicáveis na silver; "sem_cotacao" passa pela DAG mas fica de fora
    da silver.
    """
    caminho = caminho_bronze(categoria, ticker, data_execucao)
    ticker_esperado = ticker.upper()

    if not caminho.exists():
        return ResultadoValidacao(ticker_esperado, str(caminho), "falha", "Arquivo ausente na bronze")

    try:
        with open(caminho, encoding="utf-8") as f:
            dados = json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        return ResultadoValidacao(ticker_esperado, str(caminho), "falha", f"Erro ao ler/parsear JSON: {exc}")

    fonte = dados.get("_fonte") or {}
    ticker_json = dados.get("ticker")
    ticker_pagina = fonte.get("ticker_pagina")

    if ticker_json is None or ticker_json.upper() != ticker_esperado:
        return ResultadoValidacao(
            ticker_esperado, str(caminho), "falha",
            f"Ticker do JSON ({ticker_json!r}) diferente do esperado ({ticker_esperado!r})",
        )
    if ticker_pagina is None or ticker_pagina.upper() != ticker_esperado:
        return ResultadoValidacao(
            ticker_esperado, str(caminho), "falha",
            f"ticker_pagina ({ticker_pagina!r}) diferente do esperado ({ticker_esperado!r})",
        )

    criticos_none = [c for c in CAMPOS_CRITICOS if dados.get(c) is None]

    if len(criticos_none) == len(CAMPOS_CRITICOS):
        # Todos os críticos vieram None: ou é um ativo sem cotação publicada (evidenciado pelos
        # "-" explícitos), ou o parser não achou os elementos esperados na página (indeterminado).
        cotacoes_texto = fonte.get("cotacoes_texto") or {}
        seis_tracos = all(_texto_e_traco(cotacoes_texto.get(c)) for c in CAMPOS_COTACAO)
        negocios_volume_tracos = (
            _texto_e_traco(fonte.get("qtde_negocios_texto"))
            and _texto_e_traco(fonte.get("volume_diario_texto"))
        )
        if seis_tracos and negocios_volume_tracos:
            return ResultadoValidacao(
                ticker_esperado, str(caminho), "sem_cotacao",
                "Ativo reconhecido pela B3, mas sem cotação publicada (evidenciado por \"-\" explícito)",
            )
        return ResultadoValidacao(
            ticker_esperado, str(caminho), "indeterminado",
            "Campos críticos ausentes sem evidência textual suficiente de \"-\" — possível quebra do parser",
        )

    if criticos_none:
        # Só parte dos críticos ausente: inconsistente, não é o padrão nem de "ok" nem de "sem_cotacao".
        return ResultadoValidacao(
            ticker_esperado, str(caminho), "falha",
            f"Campos críticos parcialmente ausentes (inconsistente): {', '.join(criticos_none)}",
        )

    faltando_opcionais = [c for c in CAMPOS_OPCIONAIS if dados.get(c) is None]
    if faltando_opcionais:
        return ResultadoValidacao(
            ticker_esperado, str(caminho), "dados_parciais", f"Ainda sem histórico: {', '.join(faltando_opcionais)}"
        )

    return ResultadoValidacao(ticker_esperado, str(caminho), "ok")


def validar_execucao(
    data_execucao: Optional[str] = None,
    arquivo: Path = CAMINHO_ATIVOS,
) -> list[ResultadoValidacao]:
    """Valida, pra uma data de execução, os dados que a ingestão gravou na bronze.

    Gate entre bronze e silver: usada pela DAG logo após a ingestão — só quando nenhum ativo
    cai em "falha"/"indeterminado" é que a execução está aprovada a seguir pra silver.
    """
    data_execucao = data_execucao or data_coleta_hoje()
    return [
        validar_ativo_bronze(ativo["ticker"], ativo.get("categoria", "etfs"), data_execucao)
        for ativo in carregar_ativos(arquivo=arquivo)
    ]


def calcular_cobertura(resultados: list[ResultadoValidacao]) -> float:
    """(ok + dados_parciais) / quantidade esperada. "sem_cotacao"/"indeterminado"/"falha" não contam."""
    if not resultados:
        return 0.0
    aprovados = sum(1 for r in resultados if r.status in STATUS_COBERTURA)
    return aprovados / len(resultados)


def criar_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Valida os dados gravados na bronze do Data Lake para uma data de execução."
    )
    parser.add_argument(
        "-d", "--data",
        default=data_coleta_hoje(),
        help="Data de execução (partição date=) a validar (default: hoje em America/Sao_Paulo).",
    )
    parser.add_argument(
        "-a", "--arquivo",
        type=Path,
        default=CAMINHO_ATIVOS,
        help=f"Caminho do JSON com a lista de ativos (default: {CAMINHO_ATIVOS.name}).",
    )
    return parser


def main():
    # Evita mojibake quando a saída é redirecionada para arquivo/log no Windows.
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

    args = criar_parser().parse_args()
    resultados = validar_execucao(args.data, args.arquivo)

    rotulo = {
        "ok": "OK",
        "dados_parciais": "PARCIAL",
        "sem_cotacao": "SEM_COTACAO",
        "indeterminado": "INDETERMINADO",
        "falha": "FALHA",
    }
    for r in resultados:
        print(f"[{r.ticker}] {rotulo[r.status]}")

    por_status = {status: [r for r in resultados if r.status == status] for status in rotulo}
    bloqueios = [r for r in resultados if r.status in STATUS_BLOQUEIAM_DAG]

    for r in por_status["falha"]:
        print(f"[FALHA] {r.ticker}: {r.motivo} ({r.caminho})", file=sys.stderr)
    for r in por_status["indeterminado"]:
        print(f"[INDETERMINADO] {r.ticker}: {r.motivo} ({r.caminho})", file=sys.stderr)
    for r in por_status["sem_cotacao"]:
        print(f"[SEM_COTACAO] {r.ticker}: {r.motivo}", file=sys.stderr)
    for r in por_status["dados_parciais"]:
        print(f"[PARCIAL] {r.ticker}: {r.motivo}", file=sys.stderr)

    cobertura = calcular_cobertura(resultados)
    cobertura_ok = cobertura >= COBERTURA_MINIMA

    print(
        f"\n{len(por_status['ok'])} ok | {len(por_status['dados_parciais'])} parciais | "
        f"{len(por_status['sem_cotacao'])} sem cotação | {len(por_status['indeterminado'])} indeterminados | "
        f"{len(por_status['falha'])} falharam | {len(resultados)} ativos verificados na bronze (date={args.data})."
    )
    print(
        f"Cobertura: {cobertura:.2%} (mínimo exigido: {COBERTURA_MINIMA:.0%}) "
        f"{'OK' if cobertura_ok else 'ABAIXO DO MÍNIMO'}"
    )

    with open("validacao_etfs.json", "w", encoding="utf-8") as f:
        json.dump([asdict(r) for r in resultados], f, ensure_ascii=False, indent=2)

    if bloqueios or not cobertura_ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
