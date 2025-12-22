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
