from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import sys

if len(sys.argv) != 2:
    print("Usage: mnmcount <mnm_dataset_file>")
    sys.exit(1)

mnm_file = sys.argv[1]

spark = SparkSession.builder.appName("MnMCount").getOrCreate()

# Leer el CSV
mnm_df = (spark.read.format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(mnm_file))

# Agregar por estado y color
count_mnm_df = (mnm_df
                .groupBy("State", "Color")
                .agg(count("Count").alias("Total"))
                .orderBy(desc("Total")))

count_mnm_df.show(60)
print(f"Total Rows = {count_mnm_df.count()}")

# Filtrar California
ca_count = (mnm_df
            .where(col("State") == "CA")
            .groupBy("State", "Color")
            .agg(count("Count").alias("Total"))
            .orderBy(desc("Total")))

ca_count.show(10)

spark.stop()
