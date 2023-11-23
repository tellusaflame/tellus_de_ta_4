# Spark приложение

Написать spark приложение, которое в локальном режиме выполняет следующее:
По имеющимся данным о рейтингах книг посчитать агрегированную статистику по ним.

1. Прочитать csv файл: book.csv
2. Вывести схему для dataframe полученного из п.1
3. Вывести количество записей
4. Вывести информацию по книгам у которых рейтинг выше 4.50
5. Вывести средний рейтинг для всех книг.
6. Вывести агрегированную инфорацию по количеству книг в диапазонах avg raiting:
0 - 1,
1 - 2,
2 - 3,
3 - 4,
4 - 5

---

## Результат выполнения приложения

### 1. Прочитать csv файл: book.csv
```python
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

    df = spark.read.format("csv") \
        .option("header", True) \
        .option("sep", ",") \
        .option("enforceSchema", True) \
        .schema(schema) \
        .load("books.csv")

    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    df = df.withColumn("publication_date", to_date(col("publication_date"),"MM/dd/yyyy") )
```
#### Строки с невалидными данными:
```python
df.filter(col("average_rating").isNull()).show()
```
```console
+------+--------------------+--------------------+--------------+----+----------+-------------+---------+-------------+------------------+----------------+---------+
|bookID|               title|             authors|average_rating|isbn|    isbn13|language_code|num_pages|ratings_count|text_reviews_count|publication_date|publisher|
+------+--------------------+--------------------+--------------+----+----------+-------------+---------+-------------+------------------+----------------+---------+
| 12224|Streetcar Suburbs...|     Sam Bass Warner|          NULL|3.58| 674842111|9780674842113|     NULL|          236|                61|            NULL|4/20/2004|
| 16914|The Tolkien Fan's...|David E. Smith (T...|          NULL|3.58|1593600119|9781593600112|     NULL|          400|                26|            NULL| 4/6/2004|
| 22128|Patriots (The Com...|        James Wesley|          NULL|3.63|      NULL|9781563841552|     NULL|          342|                38|            NULL|1/15/1999|
| 34889|Brown's Star Atla...|               Brown|          NULL|0.00| 851742718|9780851742717|     NULL|           49|                 0|            NULL| 5/1/1977|
+------+--------------------+--------------------+--------------+----+----------+-------------+---------+-------------+------------------+----------------+---------+
```

### 2. Вывести схему для dataframe полученного из п.1
```python
df.printSchema()
```
```console
root
 |-- bookID: integer (nullable = true)
 |-- title: string (nullable = true)
 |-- authors: string (nullable = true)
 |-- average_rating: double (nullable = true)
 |-- isbn: string (nullable = true)
 |-- isbn13: long (nullable = true)
 |-- language_code: string (nullable = true)
 |-- num_pages: integer (nullable = true)
 |-- ratings_count: integer (nullable = true)
 |-- text_reviews_count: integer (nullable = true)
 |-- publication_date: date (nullable = true)
 |-- publisher: string (nullable = true)
```

### 3. Вывести количество записей
```python
print(f'Количество записей: {df.count()}')
```
```console
Количество записей: 11127
```

### 4. Вывести информацию по книгам у которых рейтинг выше 4.50
```python
df.filter(col("average_rating") >= 4.50)
```
```console
+------+--------------------+--------------------+--------------+----------+-------------+-------------+---------+-------------+------------------+----------------+--------------------+
|bookID|               title|             authors|average_rating|      isbn|       isbn13|language_code|num_pages|ratings_count|text_reviews_count|publication_date|           publisher|
+------+--------------------+--------------------+--------------+----------+-------------+-------------+---------+-------------+------------------+----------------+--------------------+
|     1|Harry Potter and ...|J.K. Rowling/Mary...|          4.57|0439785960|9780439785969|          eng|      652|      2095690|             27591|      2006-09-16|     Scholastic Inc.|
|     5|Harry Potter and ...|J.K. Rowling/Mary...|          4.56|043965548X|9780439655484|          eng|      435|      2339585|             36325|      2004-05-01|     Scholastic Inc.|
|     8|Harry Potter Boxe...|J.K. Rowling/Mary...|          4.78|0439682584|9780439682589|          eng|     2690|        41428|               164|      2004-09-13|          Scholastic|
|    10|Harry Potter Coll...|        J.K. Rowling|          4.73|0439827604|9780439827607|          eng|     3342|        28242|               808|      2005-09-12|          Scholastic|
|    30|J.R.R. Tolkien 4-...|      J.R.R. Tolkien|          4.59|0345538374|9780345538376|          eng|     1728|       101233|              1550|      2012-09-25|    Ballantine Books|
|    31|The Lord of the R...|      J.R.R. Tolkien|           4.5|0618517650|9780618517657|          eng|     1184|         1710|                91|      2004-10-21|Houghton Mifflin ...|
|    35|The Lord of the R...|J.R.R. Tolkien/Al...|           4.5|0618260587|9780618260584|        en-US|     1216|         1618|               140|      2002-10-01|Houghton Mifflin ...|
|    36|The Lord of the R...|Chris   Smith/Chr...|          4.53|0618391002|9780618391004|          eng|      218|        19822|                46|      2003-11-05|Houghton Mifflin ...|
|    37|The Lord of the R...|         Jude Fisher|           4.5|0618510826|9780618510825|          eng|      224|          359|                 6|      2004-11-15|Houghton Mifflin ...|
|   119|The Lord of the R...|        Gary Russell|          4.59|0618212906|9780618212903|          eng|      192|        26153|               102|      2002-06-12|Houghton Mifflin ...|
|   313|100 Years of Lync...|      Ralph Ginzburg|          4.61|0933121180|9780933121188|          eng|      270|           88|                 4|      1996-11-22| Black Classic Press|
|   397|The Gettysburg Ad...|Abraham Lincoln/M...|          4.53|0395883970|9780395883976|          eng|       32|         5239|                76|      1998-02-02|HMH Books for You...|
|   426|We Tell Ourselves...|Joan Didion/John ...|           4.5|0307264874|9780307264879|          eng|     1122|         1564|               108|      2006-10-17|  Everyman's Library|
|   866|Fullmetal Alchemi...|Hiromu Arakawa/Ak...|          4.57|142150460X|9781421504605|          eng|      192|         9013|               153|      2006-09-19|       VIZ Media LLC|
|   868|Fullmetal Alchemi...|Hiromu Arakawa/Ak...|          4.56|1591169259|9781591169253|          eng|      192|        16666|               299|      2005-09-13|       VIZ Media LLC|
|   869|Fullmetal Alchemi...|Hiromu Arakawa/Ak...|          4.57|1421504596|9781421504599|          eng|      192|        11451|               161|      2006-07-18|       VIZ Media LLC|
|   870|Fullmetal Alchemi...|Hiromu Arakawa/Ak...|           4.5|1591169208|9781591169208|          eng|      192|       111091|              1427|      2005-05-03|       VIZ Media LLC|
|   871|Fullmetal Alchemi...|Hiromu Arakawa/Ak...|          4.55|1591169291|9781591169291|          eng|      200|        10752|               294|      2005-11-08|       VIZ Media LLC|
|   873|Fullmetal Alchemi...|Hiromu Arakawa/Ak...|          4.52|1591169232|9781591169239|          eng|      192|        14923|               419|      2005-07-05|       VIZ Media LLC|
|   955|The 5 Love Langua...|        Gary Chapman|           4.7|0802415318|9780802415318|          eng|        0|           22|                 4|      2005-01-01|    Moody Publishers|
+------+--------------------+--------------------+--------------+----------+-------------+-------------+---------+-------------+------------------+----------------+--------------------+
only showing top 20 rows
```

### 5. Вывести средний рейтинг для всех книг.
```python
df_avg = df.agg(avg(col("average_rating")))
df_avg.show()
```
```console
+-------------------+
|avg(average_rating)|
+-------------------+
| 3.9336308079446125|
+-------------------+
```

### 6. Вывести агрегированную инфорацию по количеству книг в диапазонах avg raiting:
```python
    print(f'Количество книг со ср. рейтингом 0-1: {df.filter((col("average_rating") >= 0) & (col("average_rating") <= 1)).count()}')
    print(f'Количество книг со ср. рейтингом 1-2: {df.filter((col("average_rating") >= 1) & (col("average_rating") <= 2)).count()}')
    print(f'Количество книг со ср. рейтингом 2-3: {df.filter((col("average_rating") >= 2) & (col("average_rating") <= 3)).count()}')
    print(f'Количество книг со ср. рейтингом 3-4: {df.filter((col("average_rating") >= 3) & (col("average_rating") <= 4)).count()}')
    print(f'Количество книг со ср. рейтингом 4-5: {df.filter((col("average_rating") >= 4) & (col("average_rating") <= 5)).count()}')
```
```console
Количество книг со ср. рейтингом 0-1: 28
Количество книг со ср. рейтингом 1-2: 9
Количество книг со ср. рейтингом 2-3: 75
Количество книг со ср. рейтингом 3-4: 6307
Количество книг со ср. рейтингом 4-5: 4954
```