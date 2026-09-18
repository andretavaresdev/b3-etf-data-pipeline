# Pipeline de ETFs da B3

Pipeline de dados end-to-end que coleta, valida e transforma informações de 42 ETFs da B3. O projeto combina a coleta diária do portal Bora Investir com o histórico oficial do COTAHIST, calcula indicadores de risco e retorno e apresenta os resultados em um dashboard Streamlit.

Todo o fluxo é executado com Airflow e Docker. Os dados são organizados em um Data Lake nas camadas Bronze, Silver e Gold, com validações de qualidade, proteção contra duplicidades, testes automatizados e integração contínua.

## Visão geral

```text
Portal Bora Investir → Bronze JSON → Validação → Silver diária

COTAHIST B3 → Bronze ZIP → Silver histórica → Gold → Dashboard
```

A DAG `b3_etf_pipeline` executa cinco tasks em sequência:

```text
ingerir_bronze
    → validar_bronze
    → transform_silver
    → atualizar_historico
    → transform_gold
```

O dashboard roda como um serviço independente e apenas consome os arquivos publicados pelo pipeline.

## Principais resultados

- 42 ETFs acompanhados a partir de uma lista controlada.
- Publicação permitida somente quando pelo menos 95% dos ativos têm dados válidos.
- Histórico oficial da B3 processado por ticker e data de pregão.
- Gold com os 42 ETFs do universo, inclusive os que não possuem histórico no COTAHIST.
- Retornos de 21, 63, 126 e 252 pregões, volatilidade anualizada e drawdown.
- Gravação segura em Parquet, sem deixar arquivos incompletos ou duplicar dados em uma nova execução.
- 68 testes automatizados executados também pelo GitHub Actions.
- Dashboard com rankings, comparação entre ativos e evolução dos preços.

Na execução de referência, os 42 ativos foram coletados com 100% de cobertura. O COTAHIST forneceu histórico para 32 deles; os outros 10 permanecem na Gold com o status `sem_historico_cotahist` e métricas nulas, sem serem ocultados do resultado.

## Como a qualidade dos dados é garantida

- A Bronze preserva os dados coletados e os textos usados pelo parser, facilitando auditorias e correções.
- Arquivos ausentes, JSON inválido, divergência de ticker, dados inconsistentes ou cobertura abaixo de 95% interrompem a DAG.
- A Silver recebe apenas registros aprovados, com colunas e tipos padronizados em Parquet.
- A série histórica remove duplicidades por ticker e data de pregão.
- A Gold usa somente dados disponíveis até sua data de referência, evitando que informações futuras entrem em reprocessamentos antigos.
- Todos os 42 ETFs aparecem no resultado, mesmo quando o COTAHIST não possui histórico para o ativo.
- Cada arquivo é gravado primeiro em um caminho temporário e publicado somente depois de concluído. Executar novamente a mesma carga não cria registros duplicados.

## Dashboard

O Streamlit utiliza a Gold para indicadores e rankings e a Silver histórica para os gráficos de preços. A interface apresenta:

- data de referência e cobertura do histórico;
- rankings de retorno, volatilidade e drawdown;
- comparação entre ETFs;
- evolução do preço de fechamento;
- identificação explícita dos ativos sem histórico no COTAHIST.

## Tecnologias e ferramentas

- **Linguagens e consultas:** Python e SQL.
- **Engenharia de dados:** ETL, Data Lake, arquitetura medalhão (Bronze/Silver/Gold), Data Quality, particionamento Hive, JSON e Apache Parquet.
- **Processamento e análise:** DuckDB e Pandas.
- **Orquestração:** Apache Airflow, TaskFlow API, XCom e agendamento com cron.
- **Infraestrutura:** Docker, Docker Compose e PostgreSQL.
- **Coleta de dados:** Requests, BeautifulSoup, lxml, Web Scraping e COTAHIST B3.
- **Visualização:** Streamlit.
- **Testes e versionamento:** Pytest, Git, GitHub, GitHub Actions e CI.

## Componentes principais

| Componente | Responsabilidade |
| --- | --- |
| `get_data.py` | Coleta diária e escrita da Bronze |
| `validator.py` | Classificação dos registros e controle de qualidade |
| `transform.py` | Transformação da Bronze para a Silver diária |
| `ingest_cotahist.py` | Ingestão e tratamento do histórico oficial |
| `gold.py` | Cálculo e publicação das métricas por ETF |
| `dashboard.py` | Aplicação Streamlit |
| `airflow/dags/b3_etf_pipeline_dag.py` | Orquestração do pipeline |
| `tests/` | Testes unitários e de integração |

Os arquivos do Data Lake, logs, ZIPs e Parquets são gerados durante a execução e não são versionados no Git.

## Como executar

Construa e inicie os serviços a partir da raiz do projeto:

```bash
docker compose up -d --build
```

Serviços disponíveis localmente:

- Airflow: [http://localhost:8080](http://localhost:8080)
- Dashboard: [http://localhost:8501](http://localhost:8501)

A DAG pode ser iniciada pela interface do Airflow ou pelo terminal:

```bash
docker compose exec airflow-webserver \
  airflow dags trigger b3_etf_pipeline
```

Para executar os testes:

```bash
pip install -r requirements-dev.txt
pytest
```

## Limitações conhecidas

- O universo de ETFs é mantido manualmente em `top_etf.json`.
- Dez ETFs de renda fixa acompanhados não possuem registros no COTAHIST.
- As métricas representam retorno de preço e não são ajustadas por dividendos ou eventos corporativos.
- A carga de anos anteriores precisa ser iniciada manualmente; a DAG atualiza automaticamente apenas o ano corrente.
- A configuração do Docker Compose foi projetada para desenvolvimento local, não para exposição pública.

## Aviso

Este projeto tem finalidade educacional e de portfólio. Os dados e indicadores apresentados não constituem recomendação de investimento.
