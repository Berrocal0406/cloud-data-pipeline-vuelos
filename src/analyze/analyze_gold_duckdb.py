import duckdb
from src.utils.paths import GOLD_DIR

def main():
    gold_path = str(GOLD_DIR / "routes_gold.parquet" / "*.parquet")

    con = duckdb.connect()
    con.execute(f"""
        CREATE VIEW routes_gold AS
        SELECT * FROM read_parquet('{gold_path}');
    """)

    # 1) Top 10 rutas por número de rutas
    print("\nTop 10 rutas (routes_count):")
    print(con.execute("""
        SELECT source_airport, dest_airport, routes_count, direct_routes_count, avg_stops
        FROM routes_gold
        ORDER BY routes_count DESC
        LIMIT 10;
    """).fetchdf())

     # 2) ¿Qué porcentaje son directas? (promedio de direct_routes/routes)
    print("\n% directas (aprox):")
    print(con.execute("""
        SELECT
          SUM(direct_routes_count)::DOUBLE / NULLIF(SUM(routes_count), 0) AS pct_direct
        FROM routes_gold;
    """).fetchdf())

if __name__ == "__main__":
    main()