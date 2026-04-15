import argparse
import os
import sys
from pathlib import Path

# Ensure PySpark uses the active interpreter on Windows.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Windows: JVM must find hadoop.dll (same major.minor as Spark's Hadoop client).
_hadoop_home = Path(os.environ.get("HADOOP_HOME", r"C:\hadoop"))
_hadoop_bin = _hadoop_home / "bin"
if _hadoop_bin.is_dir():
    os.environ["PATH"] = str(_hadoop_bin) + os.pathsep + os.environ.get("PATH", "")
_java_lib_path = str(_hadoop_bin).replace("\\", "/")
# Ensure emoji/error text prints safely on Windows terminals.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Daily ETL pipeline writing CSV data into a Delta table."
    )
    parser.add_argument(
        "day",
        type=int,
        choices=[1, 2],
        help="Day number to process (1 or 2).",
    )
    return parser.parse_args()


def get_spark_session() -> SparkSession:
    python_exec = sys.executable
    builder = (
        SparkSession.builder.appName("SchemaDriftPipeline")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.pyspark.python", python_exec)
        .config("spark.pyspark.driver.python", python_exec)
        .config(
            "spark.driver.extraJavaOptions",
            f"-Djava.library.path={_java_lib_path}",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def read_day_csv(spark: SparkSession, day: int):
    csv_path = Path("data") / f"day_{day}_orders.csv"
    return spark.read.csv(str(csv_path), header=True, inferSchema=True)


def run_pipeline(day: int) -> None:
    spark = get_spark_session()
    delta_path = Path("data") / "delta" / "orders"
    delta_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        df = read_day_csv(spark, day)
        df.write.format("delta").mode("append").save(str(delta_path))
        print(
            f"Pipeline succeeded for day {day}. Data appended to Delta table at {delta_path}."
        )
    except Exception as exc:
        print("\n🚨 PIPELINE FAILED! SCHEMA DRIFT DETECTED 🚨")
        print(str(exc))
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    args = parse_args()
    run_pipeline(args.day)
