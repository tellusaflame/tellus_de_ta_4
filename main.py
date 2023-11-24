from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, to_date, regexp_replace, split


def main():
    spark = (
        SparkSession.builder.master("local[*]").appName("PySpark_de_ta_4").getOrCreate()
    )

    # 1. Прочитать csv файл: book.csv
    df = spark.read.text("books.csv")

    header = df.first()[0]
    df = (
        df.filter(~col("value").contains(header))
        .withColumn("value", regexp_replace("value", ",  ", ","))
        .withColumn("value", regexp_replace("value", ", ", ". "))
    )

    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    split_col = split(
        df.value,
        "\\,",
    )
    df = (
        df.withColumn("bookID", split_col.getItem(0).cast("Integer"))
        .withColumn("title", split_col.getItem(1))
        .withColumn("authors", split_col.getItem(2))
        .withColumn("average_rating", split_col.getItem(3).cast("Double"))
        .withColumn("isbn", split_col.getItem(4))
        .withColumn("isbn13", split_col.getItem(5).cast("Long"))
        .withColumn("language_code", split_col.getItem(6))
        .withColumn("num_pages", split_col.getItem(7).cast("Integer"))
        .withColumn("ratings_count", split_col.getItem(8).cast("Integer"))
        .withColumn("text_reviews_count", split_col.getItem(9).cast("Integer"))
        .withColumn("publication_date", split_col.getItem(10))
        .withColumn("publisher", split_col.getItem(11))
        .withColumn("publication_date", to_date(col("publication_date"), "MM/dd/yyyy"))
        .drop("value")
    )

    # 2. Вывести схему для dataframe полученного из п.1
    df.printSchema()

    # 3. Вывести количество записей
    print(f"Количество записей: {df.count()}")

    # 4. Вывести информацию по книгам у которых рейтинг выше 4.50

    df.filter(col("average_rating") >= 4.50).show()

    # 5. Вывести средний рейтинг для всех книг.

    df_avg = df.agg(avg(col("average_rating")))
    df_avg.show()

    # 6. Вывести агрегированную инфорацию по количеству книг в диапазонах avg raiting:

    print(
        f'Количество книг со ср. рейтингом 0-1: {df.filter((col("average_rating") >= 0) & (col("average_rating") <= 1)).count()}'
    )
    print(
        f'Количество книг со ср. рейтингом 1-2: {df.filter((col("average_rating") >= 1) & (col("average_rating") <= 2)).count()}'
    )
    print(
        f'Количество книг со ср. рейтингом 2-3: {df.filter((col("average_rating") >= 2) & (col("average_rating") <= 3)).count()}'
    )
    print(
        f'Количество книг со ср. рейтингом 3-4: {df.filter((col("average_rating") >= 3) & (col("average_rating") <= 4)).count()}'
    )
    print(
        f'Количество книг со ср. рейтингом 4-5: {df.filter((col("average_rating") >= 4) & (col("average_rating") <= 5)).count()}'
    )


if __name__ == "__main__":
    main()
