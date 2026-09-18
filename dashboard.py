import duckdb
import pandas as pd
import streamlit as st

import gold
import ingest_cotahist

st.set_page_config(page_title="ETFs B3", page_icon="📊", layout="wide")


@st.cache_data(ttl=300)
def carregar_gold():
    """Lê só o Parquet da gold mais recente. None se ainda não houver nenhum publicado."""
    caminho = gold.caminho_gold_mais_recente()
    if caminho is None:
        return None
    return duckdb.connect().sql(f"SELECT * FROM read_parquet('{caminho.as_posix()}')").df()


@st.cache_data(ttl=300)
def carregar_serie_historica(ticker: str) -> pd.DataFrame:
    """Série completa (todos os anos) de um ticker na silver histórica, ordenada por data."""
    glob = ingest_cotahist.glob_silver_historico()
    try:
        return duckdb.connect().execute(
            f"SELECT data_pregao, preco_fechamento FROM read_parquet('{glob}') "
            f"WHERE ticker = ? ORDER BY data_pregao",
            [ticker],
        ).df()
    except duckdb.IOException:
        return pd.DataFrame(columns=["data_pregao", "preco_fechamento"])


st.title("📊 ETFs da B3")

df_gold = carregar_gold()

if df_gold is None:
    st.error(
        "Nenhum Parquet da gold encontrado ainda. Rode a DAG `b3_etf_pipeline` ou "
        "`python gold.py` antes de abrir o dashboard."
    )
    st.stop()

st.warning(
    "⚠️ **Retorno de preço, não de investimento.** As métricas desta página são calculadas "
    "sobre o preço de fechamento do COTAHIST (PREULT), **sem ajuste** por dividendos, "
    "desdobramentos, grupamentos ou outros eventos corporativos."
)

as_of_date = df_gold["as_of_date"].iloc[0]
com_historico = df_gold[df_gold["status_historico"] == gold.STATUS_HISTORICO_OK].copy()
sem_historico = df_gold[df_gold["status_historico"] == gold.STATUS_HISTORICO_AUSENTE].copy()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Data de referência", str(as_of_date))
col2.metric("ETFs no universo", len(df_gold))
col3.metric("Com histórico no COTAHIST", len(com_historico))
col4.metric("Sem histórico no COTAHIST", len(sem_historico))

if len(sem_historico) > 0:
    with st.expander(f"⚠️ {len(sem_historico)} ETFs sem histórico no COTAHIST", expanded=True):
        st.caption(
            "Esses ativos fazem parte do universo acompanhado, mas não têm nenhum registro no "
            "COTAHIST em nenhum mercado — limitação real da fonte, não filtro do pipeline. "
            "Sem preço, retorno, volatilidade ou drawdown calculáveis."
        )
        st.dataframe(
            sem_historico[["ticker"]].rename(columns={"ticker": "Ticker"}).reset_index(drop=True),
            width="stretch", hide_index=True,
        )

st.subheader("Ranking")
colunas_ranking = {
    "ticker": "Ticker",
    "status_historico": "Status",
    "ultimo_preco": "Último preço",
    "retorno_21": "Retorno 21p",
    "retorno_63": "Retorno 63p",
    "retorno_126": "Retorno 126p",
    "retorno_252": "Retorno 252p",
    "volatilidade_anualizada": "Volatilidade (a.a.)",
    "drawdown_atual": "Drawdown atual",
    "maximo_drawdown": "Máximo drawdown",
    "qtd_observacoes": "Nº observações",
    "periodo_inicio": "Período início",
    "periodo_fim": "Período fim",
}
tabela_ranking = df_gold[list(colunas_ranking)].rename(columns=colunas_ranking).sort_values(
    "Retorno 21p", ascending=False, na_position="last"
)
colunas_percentual = (
    "Retorno 21p", "Retorno 63p", "Retorno 126p", "Retorno 252p",
    "Volatilidade (a.a.)", "Drawdown atual", "Máximo drawdown",
)
for col in colunas_percentual:
    tabela_ranking[col] = tabela_ranking[col].apply(lambda v: f"{v:.2%}" if pd.notna(v) else "—")
tabela_ranking["Último preço"] = tabela_ranking["Último preço"].apply(
    lambda v: f"R$ {v:.2f}" if pd.notna(v) else "—"
)
st.dataframe(tabela_ranking, width="stretch", hide_index=True)

st.subheader("Comparar ETFs")
tickers_com_historico = sorted(com_historico["ticker"].tolist())
selecionados = st.multiselect(
    "Selecione tickers pra comparar os retornos",
    tickers_com_historico,
    default=tickers_com_historico[:5],
)
if selecionados:
    comparacao = (
        com_historico[com_historico["ticker"].isin(selecionados)]
        .set_index("ticker")[["retorno_21", "retorno_63", "retorno_126", "retorno_252"]]
        .rename(columns={"retorno_21": "21p", "retorno_63": "63p", "retorno_126": "126p", "retorno_252": "252p"})
    )
    st.bar_chart(comparacao)
else:
    st.caption("Selecione ao menos um ticker.")

st.subheader("Evolução histórica")
if tickers_com_historico:
    ticker_evolucao = st.selectbox("Ticker", tickers_com_historico)
    serie = carregar_serie_historica(ticker_evolucao)
    if serie.empty:
        st.info("Sem histórico disponível pra esse ticker.")
    else:
        st.line_chart(serie.set_index("data_pregao")["preco_fechamento"])
        st.caption(
            f"{len(serie)} pregões, de {serie['data_pregao'].min()} até {serie['data_pregao'].max()}."
        )
else:
    st.caption("Nenhum ticker do universo tem histórico no COTAHIST ainda.")
