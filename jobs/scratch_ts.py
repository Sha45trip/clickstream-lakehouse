from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[1]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
df = spark.createDataFrame([("2019-10-01 00:00:00 UTC",)], ["t"])

candidates = {
    "default":  None,
    "unquoted": "yyyy-MM-dd HH:mm:ss UTC",
    "quoted":   "yyyy-MM-dd HH:mm:ss 'UTC'",
}
for name, fmt in candidates.items():
    try:
        col = F.to_timestamp("t") if fmt is None else F.to_timestamp("t", fmt)
        print(name, "->", df.select(col).first()[0])
    except Exception as e:
        print(name, "-> ERROR:", str(e)[:120])