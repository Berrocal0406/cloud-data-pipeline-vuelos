# run_spark.ps1
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

$py = (Resolve-Path ".\.venv\Scripts\python.exe").Path
$env:PYSPARK_PYTHON = $py
$env:PYSPARK_DRIVER_PYTHON = $py
$env:SPARK_LOCAL_IP = "127.0.0.1"

# opcional: evitar temp en AppData
$env:SPARK_LOCAL_DIRS = (Resolve-Path ".\spark-temp").Path 2>$null
if (-not $env:SPARK_LOCAL_DIRS) {
  New-Item -ItemType Directory -Force ".\spark-temp" | Out-Null
  $env:SPARK_LOCAL_DIRS = (Resolve-Path ".\spark-temp").Path
}

.\.venv\Scripts\python.exe -m src.transform.transform_routes
