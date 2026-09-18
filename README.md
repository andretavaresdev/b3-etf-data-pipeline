# Projeto B3 — Pipeline de ETFs

Pipeline de dados que acompanha ETFs negociados na B3 diariamente: coleta as cotações,
organiza tudo num data lake em camadas (bronze/silver/gold) e deixa pronto pra virar
indicadores e um dashboard mais pra frente.

A ideia é cobrir o fluxo inteiro — da fonte pública da B3 até algo que dê pra explorar —
sem depender de planilha nem de dado pago.

## Arquitetura

```
B3 (scraping) -> Bronze (JSON) -> Validação -> Silver (Parquet) -> Gold (métricas, ainda não implementado)
```

- **Bronze**: resposta bruta da página de cada ativo, particionada por `ticker` e `date`.
  Guarda os números já convertidos e também o texto original da página (pra dar pra auditar
  o parser depois, sem precisar buscar de novo).
- **Validação**: audita o que caiu na bronze e classifica cada ativo antes de liberar pra
  silver. Um ativo com problema não passa — ver seção abaixo.
- **Silver**: só os ativos aprovados, com schema fixo e tipado, em um Parquet por dia.
- **Gold**: métricas (ROI, volatilidade, drawdown) calculadas em cima da silver. Próxima etapa.

## Estrutura

```
get_data.py          # scraping + gravação na bronze
validator.py          # classifica e audita a bronze, gate de qualidade
transform.py           # bronze validada -> Parquet diário na silver
inspect_schema.py      # inspeção ad-hoc do schema com DuckDB
top_etf.json            # universo de ETFs acompanhados
airflow/                # DAG e imagem do Airflow
datalake/                # bronze/silver/gold (gerado, não versionado)
docker-compose.yml        # sobe Postgres + Airflow
```

## Rodando

Tudo sobe com Docker a partir da raiz do projeto:

```bash
docker compose up -d
```

Airflow fica em `http://localhost:8080` (login `admin` / `admin`). A DAG `b3_etf_pipeline`
roda em dias úteis, depois do fechamento do pregão, e pode ser disparada manualmente pela UI
ou por:

```bash
docker compose exec airflow-webserver airflow dags trigger b3_etf_pipeline
```

Pra rodar os scripts fora do Airflow (dev local), crie um venv e instale `requirements.txt`:

```bash
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt
python get_data.py          # ingestão do dia
python validator.py         # valida a bronze do dia
python transform.py         # publica a silver do dia
```

## DAG

Três tasks encadeadas, cada uma só roda se a anterior terminou com sucesso:

1. `ingerir_bronze` — coleta cada ticker de `top_etf.json` e grava na bronze, na partição
   da data real da coleta (fuso de São Paulo, não o horário lógico do Airflow).
2. `validar_bronze` — classifica cada ativo e calcula a cobertura da execução. Se algo
   bloquear ou a cobertura ficar abaixo de 95%, a DAG para aqui.
3. `transform_silver` — publica o Parquet do dia, só com os ativos aprovados.

## Validação

Cada ativo recebe um status depois da ingestão:

| Status | Significado | Bloqueia a DAG? | Vai pra silver? |
|---|---|---|---|
| `ok` | Todos os campos preenchidos | Não | Sim |
| `dados_parciais` | Falta só rentabilidade de mês/ano (comum em ativo recém-listado) | Não | Sim |
| `sem_cotacao` | Ativo existe na B3, mas sem cotação publicada no dia | Não | Não |
| `indeterminado` | Campos ausentes sem evidência clara do motivo — possível quebra no parser | Sim | Não |
| `falha` | Arquivo ausente, JSON inválido, ticker divergente ou inconsistência nos campos | Sim | Não |

Além do status por ativo, a execução inteira precisa de cobertura mínima de 95%
(`ok` + `dados_parciais` sobre o total esperado) pra ser aprovada.

## ETFs acompanhados

`top_etf.json` lista os ETFs cobertos pela ingestão, selecionados por patrimônio líquido,
número de cotistas e volume diário negociado na B3.

## O que falta

- Camada gold (indicadores calculados a partir da silver)
- Dashboard interativo
