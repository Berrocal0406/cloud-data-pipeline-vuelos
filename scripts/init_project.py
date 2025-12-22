from pathlib import Path

STRUCTURE = [
    "data/raw",
    "data/silver",
    "data/gold",
    "sql",
    "src/utils",
    "src/ingest",
    "src/clean",
    "src/transform",
    "src/analyze",
    "docs",
]

FILES = {
    ".gitignore": """\
.venv/
__pycache__/
*.pyc
data/raw/*
data/silver/*
data/gold/*
!.gitkeep
.DS_Store
""",
    "requirements.txt": """\
pandas==2.2.2
pyarrow==17.0.0
pyspark==3.5.1
duckdb==1.0.0
python-dotenv==1.0.1
""",
    "README.md": "# Cloud Data Pipeline – Análisis de Datos de Vuelos\n",
    "src/config.py": 'APP_NAME = "Cloud Data Pipeline – Análisis de Datos de Vuelos"\n',
    "src/utils/paths.py": """\
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

SQL_DIR = ROOT / "sql"
DOCS_DIR = ROOT / "docs"
""",
    "src/analyze/run_sql_analysis.py": """\
import duckdb
import pandas as pd

def main():
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    con = duckdb.connect()
    con.register("t", df)
    result = con.execute("SELECT COUNT(*) AS n FROM t").fetchdf()
    print(result)

if __name__ == "__main__":
    main()
""",
    "docs/architecture.md": """\
# Arquitectura (simulación cloud)

Raw (CSV) -> Silver (limpio) -> Gold (métricas)

En el mundo real:
- Raw: S3 / ADLS (Data Lake)
- Transform: Databricks / Spark
- Gold: Snowflake / Databricks SQL (para BI)
""",
}

def touch_gitkeep(folder: Path):
    (folder / ".gitkeep").write_text("")

def main():
    root = Path.cwd()

    # Create folders
    for rel in STRUCTURE:
        p = root / rel
        p.mkdir(parents=True, exist_ok=True)

    # Keep data folders in git
    for rel in ["data/raw", "data/silver", "data/gold"]:
        touch_gitkeep(root / rel)

    # Create base files (only if they don't exist)
    for rel, content in FILES.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        if not p.exists():
            p.write_text(content, encoding="utf-8")

    print("✅ Estructura creada correctamente.")

if __name__ == "__main__":
    main()
