import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, to_date
from pyspark.sql.types import *


def main():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('PySpark_de_ta_4') \
        .getOrCreate()

    schema = StructType() \
        .add("bookID", IntegerType(), True) \
        .add("title", StringType(), True) \
        .add("authors", StringType(), True) \
        .add("average_rating", DoubleType(), True) \
        .add("isbn", StringType(), True) \
        .add("isbn13", LongType(), True) \
        .add("language_code", StringType(), True) \
        .add("num_pages", IntegerType(), True) \
        .add("ratings_count", IntegerType(), True) \
        .add("text_reviews_count", IntegerType(), True) \
        .add("publication_date", StringType(), True) \
        .add("publisher", StringType(), True)

    # 1. Прочитать csv файл: book.csv
    df = spark.read.format("csv") \
        .option("header", True) \
        .option("sep", ",") \
        .option("enforceSchema", True) \
        .schema(schema) \
        .load("books.csv")

    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    df = df.withColumn("publication_date", to_date(col("publication_date"),"MM/dd/yyyy") )

    # Показать строки с невалидными данными
    df.filter(col("average_rating").isNull()).show()

    # 2. Вывести схему для dataframe полученного из п.1
    df.printSchema()

    # 3. Вывести количество записей
    print(f'Количество записей: {df.count()}')

    # 4. Вывести информацию по книгам у которых рейтинг выше 4.50

    df.filter(col("average_rating") >= 4.50) \
        .show()

    # 5. Вывести средний рейтинг для всех книг.

    df_avg = df.agg(avg(col("average_rating")))
    df_avg.show()

    # 6. Вывести агрегированную инфорацию по количеству книг в диапазонах avg raiting:

    print(f'Количество книг со ср. рейтингом 0-1: {df.filter((col("average_rating") >= 0) & (col("average_rating") <= 1)).count()}')
    print(f'Количество книг со ср. рейтингом 1-2: {df.filter((col("average_rating") >= 1) & (col("average_rating") <= 2)).count()}')
    print(f'Количество книг со ср. рейтингом 2-3: {df.filter((col("average_rating") >= 2) & (col("average_rating") <= 3)).count()}')
    print(f'Количество книг со ср. рейтингом 3-4: {df.filter((col("average_rating") >= 3) & (col("average_rating") <= 4)).count()}')
    print(f'Количество книг со ср. рейтингом 4-5: {df.filter((col("average_rating") >= 4) & (col("average_rating") <= 5)).count()}')


if __name__ == "__main__":
    main()
