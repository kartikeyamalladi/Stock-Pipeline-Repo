import glob
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    from_json,
    to_json,
    explode,
    arrays_zip,
    # NEW
    input_file_name,
    regexp_extract,
    to_timestamp,
    from_unixtime,
    year,
    month,
)
from pyspark.sql.types import StructType, StructField, StringType, MapType


def create_spark_session():
    spark = (
        SparkSession.builder.appName("StockDataPipeline")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark


def get_raw_paths():

    # OLD (full scan)
    # alpha_path = "data/raw/alpha_vantage/**/*.json"
    # yahoo_path = "data/raw/yahoo/**/*.json"
    # finnhub_path = "data/raw/finnhub/**/*.json"

    # NEW → process only today's data
    today = datetime.now().strftime("%Y/%m/%d")

    alpha_path = f"data/raw/alpha_vantage/{today}/*.json"
    yahoo_path = f"data/raw/yahoo/{today}/*.json"
    finnhub_path = f"data/raw/finnhub/{today}/*.json"

    alpha_files = glob.glob(alpha_path)
    yahoo_files = glob.glob(yahoo_path)
    finnhub_files = glob.glob(finnhub_path)

    return alpha_files, yahoo_files, finnhub_files


def process_alpha_vantage(spark, files):
    if not files:
        empty_schema = StructType(
            [
                StructField("timestamp", StringType(), True),
                StructField("open", StringType(), True),
                StructField("high", StringType(), True),
                StructField("low", StringType(), True),
                StructField("close", StringType(), True),
                StructField("volume", StringType(), True),
                StructField("source", StringType(), True),
                StructField("symbol", StringType(), True),  # NEW
            ]
        )
        return spark.createDataFrame([], schema=empty_schema)

    df = spark.read.json(files)

    df = df.withColumn("file_path", input_file_name())
    df = df.withColumn(
        "symbol",
        regexp_extract(col("file_path"), r".*/([A-Z]+)\.json", 1),
    )

    ts_value_schema = StructType(
        [
            StructField("1. open", StringType(), True),
            StructField("2. high", StringType(), True),
            StructField("3. low", StringType(), True),
            StructField("4. close", StringType(), True),
            StructField("5. volume", StringType(), True),
        ]
    )

    ts_map_schema = MapType(StringType(), ts_value_schema)

    df = df.withColumn(
        "ts_map",
        from_json(to_json(col("Time Series (Daily)")), ts_map_schema),
    )

    ts_df = df.selectExpr("explode(ts_map) as (timestamp, data)", "symbol")

    flat_df = ts_df.select(
        col("timestamp"),
        col("data.`1. open`").alias("open"),
        col("data.`2. high`").alias("high"),
        col("data.`3. low`").alias("low"),
        col("data.`4. close`").alias("close"),
        col("data.`5. volume`").alias("volume"),
        col("symbol"),  # NEW
    )

    flat_df = flat_df.withColumn(
        "timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd")
    )

    flat_df = flat_df.withColumn("source", lit("alpha_vantage"))

    return flat_df


def process_yahoo(spark, files):

    if not files:
        empty_schema = StructType(
            [
                StructField("timestamp", StringType(), True),
                StructField("open", StringType(), True),
                StructField("high", StringType(), True),
                StructField("low", StringType(), True),
                StructField("close", StringType(), True),
                StructField("volume", StringType(), True),
                StructField("source", StringType(), True),
                StructField("symbol", StringType(), True),  # NEW
            ]
        )
        return spark.createDataFrame([], schema=empty_schema)

    df = spark.read.json(files)

    # NEW → extract symbol
    df = df.withColumn("file_path", input_file_name())
    df = df.withColumn(
        "symbol",
        regexp_extract(col("file_path"), r".*/([A-Z]+)\.json", 1),
    )

    result_df = df.selectExpr("explode(chart.result) as result", "symbol")

    data_df = result_df.selectExpr(
        "result.timestamp as timestamp",
        "result.indicators.quote[0].open as open",
        "result.indicators.quote[0].high as high",
        "result.indicators.quote[0].low as low",
        "result.indicators.quote[0].close as close",
        "result.indicators.quote[0].volume as volume",
        "symbol",
    )

    exploded = data_df.selectExpr(
        "explode(arrays_zip(timestamp, open, high, low, close, volume)) as row",
        "symbol",
    )

    flat_df = exploded.select(
        col("row.timestamp"),
        col("row.open"),
        col("row.high"),
        col("row.low"),
        col("row.close"),
        col("row.volume"),
        col("symbol"),
    )

    # NEW → timestamp fix
    flat_df = flat_df.withColumn(
        "timestamp", to_timestamp(from_unixtime(col("timestamp")))
    )

    flat_df = flat_df.withColumn("source", lit("yahoo"))

    return flat_df


def process_finnhub(spark, files):
    if not files:
        empty_schema = StructType(
            [
                StructField("timestamp", StringType(), True),
                StructField("open", StringType(), True),
                StructField("high", StringType(), True),
                StructField("low", StringType(), True),
                StructField("close", StringType(), True),
                StructField("volume", StringType(), True),
                StructField("source", StringType(), True),
                StructField("symbol", StringType(), True),  # NEW
            ]
        )
        return spark.createDataFrame([], schema=empty_schema)

    df = spark.read.json(files)

    # NEW → extract symbol
    df = df.withColumn("file_path", input_file_name())
    df = df.withColumn(
        "symbol",
        regexp_extract(col("file_path"), r".*/([A-Z]+)\.json", 1),
    )

    df = df.filter(col("s") == "ok")

    df = df.withColumn(
        "zipped",
        arrays_zip(col("t"), col("o"), col("h"), col("l"), col("c"), col("v")),
    )

    exploded_df = df.select(explode(col("zipped")).alias("data"), "symbol")

    flat_df = exploded_df.select(
        col("data.t").cast(StringType()).alias("timestamp"),
        col("data.o").cast(StringType()).alias("open"),
        col("data.h").cast(StringType()).alias("high"),
        col("data.l").cast(StringType()).alias("low"),
        col("data.c").cast(StringType()).alias("close"),
        col("data.v").cast(StringType()).alias("volume"),
        col("symbol"),
    )

    # NEW → timestamp fix
    flat_df = flat_df.withColumn(
        "timestamp", to_timestamp(from_unixtime(col("timestamp")))
    )

    flat_df = flat_df.withColumn("source", lit("finnhub"))

    return flat_df


def add_metadata(df):

    df = df.withColumn("ingestion_time", current_timestamp())

    # NEW → partition columns
    df = df.withColumn("year", year(col("timestamp")))
    df = df.withColumn("month", month(col("timestamp")))

    # NEW → deduplication
    df = df.dropDuplicates(["symbol", "timestamp", "source"])

    return df


def merge_datasets(alpha_df, yahoo_df, finnhub_df):
    return alpha_df.unionByName(yahoo_df).unionByName(finnhub_df)


def write_parquet(df):

    # OLD
    # df.write.mode("append").partitionBy("source").parquet(...)

    # NEW → production-style partitioning, overwrite mode for idempotency
    df.write.mode("overwrite").partitionBy("symbol", "year", "month").parquet(
        "data/processed/parquet/"
    )


def main():

    spark = create_spark_session()

    alpha_files, yahoo_files, finnhub_files = get_raw_paths()

    alpha_df = process_alpha_vantage(spark, alpha_files)
    yahoo_df = process_yahoo(spark, yahoo_files)
    finnhub_df = process_finnhub(spark, finnhub_files)

    merged = merge_datasets(alpha_df, yahoo_df, finnhub_df)

    final_df = add_metadata(merged)

    # DEBUG (NEW)
    print("Final count:", final_df.count())
    final_df.show(5, False)

    write_parquet(final_df)

    spark.stop()


if __name__ == "__main__":
    main()
