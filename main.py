from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg


def main():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('PySpark_de_ta_4') \
        .getOrCreate()

    # 1. Прочитать csv файл: book.csv
    df = spark.read.csv(
        'books.csv',
        sep=',',
        header=True,
    )

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
