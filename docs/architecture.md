# Arquitectura (simulación cloud)

Raw (CSV) -> Silver (limpio) -> Gold (métricas)

En el mundo real:
- Raw: S3 / ADLS (Data Lake)
- Transform: Databricks / Spark
- Gold: Snowflake / Databricks SQL (para BI)
