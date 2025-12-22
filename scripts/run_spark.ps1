Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Paths
$python = (Resolve-Path ".\.venv\Scripts\python.exe").Path
$tempDir = "C:\spark-temp"

# Ensure temp dir exists
New-Item -ItemType Directory -Force -Path $tempDir | Out-Null

# Env for Spark
$env:PYSPARK_PYTHON = $python
$env:PYSPARK_DRIVER_PYTHON = $python
$env:SPARK_LOCAL_IP = "127.0.0.1"
$env:SPARK_LOCAL_DIRS = $tempDir

# Run your script
& $python "src\transform\spark_smoke_test.py"
