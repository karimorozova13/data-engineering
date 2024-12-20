from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, min, max, avg, unix_timestamp, count_if, round, when
from pyspark.sql.types import TimestampType, IntegerType

# Створюємо сесію Spark
spark = SparkSession.builder.appName("MyGoitSparkSandbox").getOrCreate()
spark.conf.set("spark.sql.stackTracesInDataFrameContext", "true")

# Завантажуємо датасет
nuek_df = spark.read.csv('./nuek-vuh3.csv', header=True)

# Створюємо тимчасове представлення для виконання SQL-запитів
nuek_df.createTempView("nuek_view")

# Скільки унікальних call_type є в датасеті?
print(nuek_df.select('call_type')
      .where(col("call_type").isNotNull())
      .distinct()
      .count())
# Скільки унікальних call_type є в датасеті? (з використанням SQL)
df = spark.sql("""SELECT COUNT(DISTINCT call_type) as count
                    FROM nuek_view 
                    WHERE call_type IS NOT NULL""")
# Виводимо датафрейм на дисплей
df.show()

# Витягуємо дані колонки з датафрейму
print(df.collect(), type(df.collect()))
# Дотягуємось до самого значення за номером рядка та іменем колонки
print(df.collect()[0]['count'])
# або за номером рядка та номером колонки
print(df.collect()[0][0])