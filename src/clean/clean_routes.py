import pandas as pd
from src.utils.paths import RAW_DIR, SILVER_DIR

def main():
    # 1. Definir columnas
    columns = [
        "airline",
        "airline_id",
        "source_airport",
        "source_id",
        "dest_airport",
        "dest_id",
        "codeshare",
        "stops",
        "equipment",
    ]

    # 2. Leer RAW (sin headers)
    df = pd.read_csv(
        RAW_DIR / "routes_raw.csv",
        header=None,
        names=columns
    )

    print("RAW rows:", len(df))

    # 3. Limpieza básica
    df = df.dropna(subset=["source_airport", "dest_airport"])
    df["stops"] = pd.to_numeric(df["stops"], errors="coerce").fillna(0).astype(int)

    # 4. Guardar SILVER
    output_path = SILVER_DIR / "routes_silver.csv"
    df.to_csv(output_path, index=False)

    print("SILVER rows:", len(df))
    print(f"Archivo generado: {output_path}")

if __name__ == "__main__":
    main()
