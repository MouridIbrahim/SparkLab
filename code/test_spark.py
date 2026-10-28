from pyspark.sql import SparkSession
spark = (SparkSession.builder.appName("FirstSparkExperiment")
.master("local[*]").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
print("Spark Version:", spark.version)
data = [("Alice", 85), ("Bob", 92), ("Cindy", 78)]
df = spark.createDataFrame(data, ["name", "score"])
df.show()
print("Total Records:", df.count())
spark.stop()
