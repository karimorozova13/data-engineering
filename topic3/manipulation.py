from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType, IntegerType

spark = SparkSession.builder.appName("MyGoitSparkSandbox").getOrCreate()

nuek_df = spark.read.csv('./nuek-vuh3.csv', header=True)

nuek_df.createTempView("nuek_view")

print(nuek_df.select('call_type')
      .where(col("call_type").isNotNull())
      .distinct()
      .count())
# # Скільки унікальних call_type є в датасеті? (з використанням SQL)
df = spark.sql("""SELECT COUNT(DISTINCT call_type) as count
                    FROM nuek_view
                    WHERE call_type IS NOT NULL""")

df.show()

# Витягуємо дані колонки з датафрейму
print(df.collect(), type(df.collect()))
# Дотягуємось до самого значення за номером рядка та іменем колонки
print(df.collect()[0]['count'])
# або за номером рядка та номером колонки
print(df.collect()[0][0])

# Які call_type є найбільш популярними (топ-3)?
nuek_df.groupBy('call_type').count().orderBy(col('count').desc()).limit(3).show()


# Які call_type є найбільш популярними (топ-3)? (з використанням SQL)
spark.sql("""SELECT call_type, COUNT(call_type) as count
                    FROM nuek_view
                    GROUP BY call_type
                    ORDER BY count DESC
                    LIMIT 3""").show()

nuek_df.select("received_dttm", "response_dttm") \
    .withColumn("delay_s", col("response_dttm") - (col("received_dttm"))) \
    .show(5)
nuek_df.select("received_dttm", "response_dttm").printSchema()
# Повторюємо рахування колонок, тільки попередньо
# перетворюємо колонки в тип Timestamp
df_times = nuek_df.select("received_dttm", "response_dttm") \
    .withColumn("received_dttm", col("received_dttm").cast(TimestampType())) \
    .withColumn("response_dttm", col("response_dttm").cast(TimestampType())) \
    .withColumn("delay_s", col("response_dttm") - (col("received_dttm")))

df_times = nuek_df.select("received_dttm", "response_dttm") \
    .withColumn("received_dttm", col("received_dttm").cast(TimestampType())) \
    .withColumn("response_dttm", col("response_dttm").cast(TimestampType())) \
    .withColumn("delay_s", unix_timestamp(col("response_dttm")) - unix_timestamp(col("received_dttm")))

df_times.printSchema()
df_times.show(5)

df_times.groupby().agg(
    count("*").alias("total_"),
    count_if(col("delay_s").isNotNull()).alias("delayed_not_null"),
    count_if(col("delay_s").isNull()).alias("delayed_null"),
    min(col("delay_s")).alias("min_delay"),
    max(col("delay_s")).alias("max_delay"),
    avg(
        col("delay_s")
    ).alias("avg_delay"),
    avg(
        when(col("delay_s").isNotNull(), col("delay_s")).otherwise(0)
    ).alias("avg_zeroed"),
    avg(
        when(col("delay_s").isNotNull(), col("delay_s")).otherwise(220.0615)
    ).alias("avg_replaced")
).show(10)

zip_station = nuek_df.select('zipcode_of_incident', 'station_area') \
    .withColumnRenamed("station_area", "station_area_1")

nuek_df.join(zip_station, nuek_df.zipcode_of_incident == zip_station.zipcode_of_incident, 'inner') \
      .drop(zip_station.zipcode_of_incident) \
      .select('zipcode_of_incident', 'station_area', 'station_area_1') \
      .dropDuplicates(['station_area', 'station_area_1']) \
      .dropna() \
      .where(col('station_area') != col('station_area_1')) \
      .groupBy('zipcode_of_incident') \
      .agg(
          collect_list("station_area").alias("station_area_list"),
          collect_list("station_area_1").alias("station_area_list_1")
          ) \
      .withColumn("station_area_united", array_union('station_area_list', 'station_area_list_1')) \
      .withColumn("station_area_distinct", array_distinct('station_area_united')) \
      .show()








