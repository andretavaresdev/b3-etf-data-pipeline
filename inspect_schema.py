"""Inspeciona o schema dos JSONs da bronze com DuckDB, sem tocar nos arquivos."""
from pathlib import Path

import duckdb

RAIZ_LAKE_BRONZE = Path(__file__).parent / "datalake" / "bronze"
GLOB_ETFS = str(RAIZ_LAKE_BRONZE / "etfs" / "*" / "*" / "cotacao.json")


def main():
    con = duckdb.connect()
    fonte = f"read_json_auto('{GLOB_ETFS}', hive_partitioning=true, filename=true)"

    print(f"Lendo: {GLOB_ETFS}\n")

    print("== Schema (DESCRIBE) ==")
    con.sql(f"DESCRIBE SELECT * FROM {fonte}").show(max_width=120)

    print("\n== Estatísticas (SUMMARIZE) ==")
    con.sql(f"SUMMARIZE SELECT * FROM {fonte}").show(max_width=120)

    print("\n== Amostra (5 linhas) ==")
    con.sql(f"SELECT * FROM {fonte} LIMIT 5").show(max_width=120)


if __name__ == "__main__":
    main()
