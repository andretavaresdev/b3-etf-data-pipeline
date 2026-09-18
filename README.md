# Pipeline de ETFs da B3

Este projeto coleta diariamente informações de 42 ETFs negociados na B3 e prepara os dados para análises futuras.

O pipeline acessa as páginas públicas do portal Bora Investir, valida a qualidade das informações coletadas e publica os registros aprovados em Parquet. Na execução orquestrada, todo o processo roda pelo Airflow dentro de containers Docker.

Atualmente, o fluxo está concluído até a camada Gold. A próxima etapa é o dashboard.

## Arquitetura

```text
Portal B3 (diário)          COTAHIST B3 (histórico oficial)
    ↓                              ↓
Ingestão                    Ingestão do histórico
    ↓                              ↓
Bronze (JSON)                Bronze (ZIP original)
    ↓                              ↓
Validação e gate               Silver histórica
de qualidade                  (Parquet, por ano)
    ↓                              ↓
Silver (Parquet, dia atual)        │
                                    ↓
                    Gold (métricas por ticker, todo o universo)
```

A Silver diária (scraping) e a Silver histórica (COTAHIST) são duas fontes independentes: a primeira alimenta o snapshot do dia, a segunda alimenta a série temporal que a Gold usa pra calcular retorno, volatilidade e drawdown.

### Bronze

Armazena um JSON por ativo e data de coleta. Cada arquivo contém os valores extraídos da página e evidências da fonte, como os textos originais das cotações, o ticker encontrado e o horário da coleta.

Os arquivos seguem uma estrutura particionada:

```text
datalake/bronze/
└── etfs/
    └── ticker=IVVB11/
        └── date=2026-09-18/
            └── cotacao.json
```

### Validação

O validator lê somente os arquivos da Bronze, sem realizar novas requisições ao site.

Cada ativo é classificado antes de seguir no pipeline. A execução também precisa atingir uma cobertura mínima de 95%. Se houver falhas críticas, resultados indeterminados ou cobertura insuficiente, a DAG é interrompida.

### Silver

Recebe apenas os ativos aprovados pela validação. Os dados são padronizados, tipados e publicados em um arquivo Parquet diário.

```text
datalake/silver/
└── etfs/
    └── year=2026/
        └── month=09/
            └── etfs_2026-09-18.parquet
```

A publicação é atômica e idempotente. O arquivo é gravado primeiro em um caminho temporário e só substitui o arquivo definitivo depois que a operação termina com sucesso.

### Histórico (COTAHIST)

A B3 publica anualmente o arquivo COTAHIST, com o histórico oficial de todos os pregões. `ingest_cotahist.py` baixa o arquivo do ano, preserva o ZIP original na Bronze e publica uma Silver histórica só com os ETFs de `top_etf.json`.

```text
datalake/bronze/cotahist/
└── year=2026/
    └── collected_date=2026-09-18/
        └── COTAHIST_A2026.ZIP

datalake/silver/etfs_historico/
└── year=2026/
    └── etfs_historico_2026.parquet
```

O arquivo anual muda todo pregão (a B3 acrescenta o dia mais recente); por isso a Bronze versiona por `collected_date` — salvar sempre no mesmo caminho faria o download de hoje sobrescrever o de ontem, perdendo o registro de qual arquivo foi realmente usado em cada processamento.

A Silver histórica filtra só o mercado à vista, remove duplicidade por `(ticker, data_pregao)` e publica com o mesmo padrão de escrita atômica e idempotente das outras camadas.

**Limitação real da fonte**: nem todo ETF de `top_etf.json` aparece no COTAHIST. Os 10 ETFs de renda fixa do universo (IMAB11, LFTS11, LLFT11, entre outros) não têm nenhum registro no arquivo, em nenhum mercado — não é filtro do pipeline, é ausência na própria fonte.

### Gold

`gold.py` calcula métricas por ticker a partir da Silver histórica: último preço, retorno em 21/63/126/252 pregões, volatilidade anualizada, drawdown atual e máximo, quantidade de observações e período utilizado.

As métricas são **retorno de preço puro** — calculadas sobre o preço de fechamento do COTAHIST, sem ajuste por dividendos, desdobramentos, grupamentos ou outros eventos corporativos.

A publicação faz o equivalente a um `LEFT JOIN` entre o universo completo (`top_etf.json`) e a Silver histórica: todo ticker sai na Gold, com ou sem dado. Quem não tem histórico no COTAHIST aparece com `status_historico = "sem_historico_cotahist"` e as demais métricas nulas — omitir essas linhas esconderia uma limitação real da fonte.

```text
datalake/gold/etfs/
└── as_of_date=2026-09-18/
    └── gold_etfs_2026-09-18.parquet
```

A leitura da Silver histórica é feita por glob no DuckDB com o filtro `data_pregao <= as_of_date` explícito na query — dados de datas futuras à partição nunca entram no cálculo, o que garante backfills reproduzíveis. Publicação atômica e idempotente, como as demais camadas.

## Tecnologias utilizadas

* Python
* Apache Airflow
* Docker e Docker Compose
* DuckDB
* Parquet
* Requests
* BeautifulSoup
* PostgreSQL

## Estrutura do projeto

```text
get_data.py
validator.py
transform.py
ingest_cotahist.py
gold.py
inspect_schema.py
top_etf.json
docker-compose.yml
requirements.txt
requirements-dev.txt
pytest.ini

airflow/
├── dags/
│   └── b3_etf_pipeline_dag.py
├── Dockerfile
└── requirements-project.txt

tests/
├── conftest.py
├── test_get_data.py
├── test_validator.py
├── test_transform.py
├── test_ingest_cotahist.py
└── test_gold.py

datalake/
├── bronze/
├── silver/
└── gold/
```

Os arquivos do Data Lake são gerados durante a execução e não são versionados no Git.

## Fluxo da DAG

A DAG `b3_etf_pipeline` possui cinco tasks executadas em sequência, cada uma só rodando se a anterior terminar com sucesso (dependência estrutural por XCom, `TriggerRule.ALL_SUCCESS`):

1. `ingerir_bronze`: consulta os tickers definidos em `top_etf.json` e grava os resultados na Bronze.

2. `validar_bronze`: classifica os ativos e calcula a cobertura da execução. O pipeline é interrompido se os critérios de qualidade não forem atendidos.

3. `transform_silver`: transforma os registros aprovados e publica o Parquet diário na Silver.

4. `atualizar_historico`: baixa o COTAHIST do ano corrente e atualiza a Silver histórica, que passou a ter o pregão mais recente.

5. `transform_gold`: calcula as métricas por ticker e publica a Gold.

A data da partição representa a data real da coleta no fuso `America/Sao_Paulo`. A logical date do Airflow não é usada como data da cotação.

## Regras de validação

| Status           | Significado                                                               | Bloqueia a DAG? | Publicado na Silver? |
| ---------------- | ------------------------------------------------------------------------- | --------------: | -------------------: |
| `ok`             | Todos os campos necessários estão preenchidos                             |             Não |                  Sim |
| `dados_parciais` | Apenas campos opcionais estão ausentes                                    |             Não |                  Sim |
| `sem_cotacao`    | O ativo foi reconhecido, mas a fonte ainda não possui cotação disponível  |             Não |                  Não |
| `indeterminado`  | Existem campos ausentes sem evidência suficiente para explicar o motivo   |             Sim |                  Não |
| `falha`          | Arquivo ausente, JSON inválido, ticker divergente ou dados inconsistentes |             Sim |                  Não |

A cobertura é calculada por:

```text
ativos publicáveis / ativos esperados
```

São considerados publicáveis os ativos classificados como `ok` ou `dados_parciais`. A execução precisa atingir pelo menos 95%.

## Universo acompanhado

O arquivo `top_etf.json` contém 42 ETFs selecionados por critérios de relevância, como patrimônio líquido, número de investidores e volume diário negociado.

Essa é uma lista controlada. O pipeline consulta os dados atuais desses ativos, mas ainda não identifica automaticamente novos ETFs listados pela B3.

## Resultado da validação atual

Na última execução completa:

```text
42 ativos coletados
40 ativos classificados como ok
2 ativos com dados parciais
0 falhas
100% de cobertura
42 registros publicados na Silver
```

Esses números podem mudar conforme a atualização da fonte.

## Resultado da Gold atual

Última publicação, com histórico de 2025-01-02 até o pregão mais recente:

```text
42 tickers na Gold (universo completo, LEFT JOIN com top_etf.json)
32 com status_historico = ok
10 com status_historico = sem_historico_cotahist (métricas nulas)
```

## Testes

```bash
pip install -r requirements-dev.txt
pytest
```

Os testes rodam em memória/`tmp_path`, sem tocar no Data Lake real e sem acessar a rede — parser, cobertura, gate da Silver, ingestão do COTAHIST e métricas da Gold usam fixtures sintéticas. Rodam automaticamente em cada push e pull request via GitHub Actions (`.github/workflows/tests.yml`).

## Como executar

Na raiz do projeto, construa e inicie os containers:

```bash
docker compose up -d --build
```

O Airflow ficará disponível em:

```text
http://localhost:8080
```

A DAG pode ser executada pela interface do Airflow ou pelo terminal:

```bash
docker compose exec airflow-webserver \
  airflow dags trigger b3_etf_pipeline
```

Para executar os scripts diretamente no Windows (PowerShell):

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt

python get_data.py
python validator.py
python transform.py
python ingest_cotahist.py --ano 2026
python gold.py
```

`ingest_cotahist.py` precisa rodar pelo menos uma vez por ano já concluído (ex.: `--ano 2025`) para ter histórico suficiente pras janelas de 252 pregões — o ano corrente é atualizado automaticamente pela DAG.

## Limitações atuais

* A lista de ETFs é mantida manualmente.
* A fonte fornece o snapshot disponível no momento da consulta.
* 10 dos 42 ETFs (todos de renda fixa) não têm nenhum registro no COTAHIST — aparecem na Gold com `status_historico = "sem_historico_cotahist"` e métricas nulas, não são omitidos.
* As métricas da Gold são retorno de preço puro, sem ajuste por dividendos, desdobramentos ou outros eventos corporativos.
* O backfill do histórico (`ingest_cotahist.py --ano`) precisa ser rodado manualmente para anos já fechados; a DAG só atualiza o ano corrente.
* O dashboard ainda não foi desenvolvido.
* O login admin/admin do Airflow (dev local) ainda está fixo no `docker-compose.yml`; mover para variáveis de ambiente configuráveis é próximo passo de segurança.

## Próximas etapas

* Desenvolver um dashboard em Streamlit, consumindo a Gold para rankings/indicadores e a Silver histórica para gráficos de evolução.
* Mover as credenciais do Airflow para variáveis de ambiente configuráveis.

## Aviso

Este projeto possui finalidade educacional e de portfólio. Os dados apresentados não constituem recomendação de investimento.
