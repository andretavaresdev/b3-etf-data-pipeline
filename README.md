# Pipeline de ETFs da B3

Este projeto coleta diariamente informações de 42 ETFs negociados na B3 e prepara os dados para análises futuras.

O pipeline acessa as páginas públicas do portal Bora Investir, valida a qualidade das informações coletadas e publica os registros aprovados em Parquet. Na execução orquestrada, todo o processo roda pelo Airflow dentro de containers Docker.

Atualmente, o fluxo está concluído até a camada Silver. A próxima etapa será construir o histórico necessário para calcular indicadores e desenvolver um dashboard.

## Arquitetura

```text
Portal B3
    ↓
Ingestão
    ↓
Bronze (JSON)
    ↓
Validação e gate de qualidade
    ↓
Silver (Parquet)
    ↓
Gold (planejada)
```

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

### Gold

A camada Gold ainda não foi implementada. Ela será responsável por métricas como ROI, volatilidade e drawdown, calculadas a partir do histórico consolidado da Silver.

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
inspect_schema.py
top_etf.json
docker-compose.yml
requirements.txt

airflow/
├── dags/
│   └── b3_etf_pipeline_dag.py
├── Dockerfile
└── requirements-project.txt

datalake/
├── bronze/
├── silver/
└── gold/
```

Os arquivos do Data Lake são gerados durante a execução e não são versionados no Git.

## Fluxo da DAG

A DAG `b3_etf_pipeline` possui três tasks executadas em sequência:

1. `ingerir_bronze`: consulta os tickers definidos em `top_etf.json` e grava os resultados na Bronze.

2. `validar_bronze`: classifica os ativos e calcula a cobertura da execução. O pipeline é interrompido se os critérios de qualidade não forem atendidos.

3. `transform_silver`: transforma os registros aprovados e publica o Parquet diário na Silver.

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
```

## Limitações atuais

* A lista de ETFs é mantida manualmente.
* A fonte fornece o snapshot disponível no momento da consulta.
* O pipeline ainda não possui uma carga histórica completa.
* As métricas da camada Gold ainda não foram implementadas.
* O dashboard ainda não foi desenvolvido.
* O login admin/admin do Airflow (dev local) ainda está fixo no `docker-compose.yml`; mover para variáveis de ambiente configuráveis é próximo passo de segurança.

## Próximas etapas

* Definir uma fonte e uma estratégia para o histórico de preços.
* Consolidar a série temporal por ticker e data de referência.
* Implementar ROI, volatilidade e drawdown.
* Construir a camada Gold.
* Desenvolver um dashboard em Streamlit.

## Aviso

Este projeto possui finalidade educacional e de portfólio. Os dados apresentados não constituem recomendação de investimento.
