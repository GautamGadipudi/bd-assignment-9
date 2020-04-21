import timeit
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import split, regexp_replace, array_contains

'''
SELECT
mem.id, mem.name
FROM movie_producer mp
INNER JOIN movie_genre mg
	ON mg.movie = mp.movie
	AND mg.genre = 23
INNER JOIN (
	SELECT 
		*
	FROM member
	WHERE name like '%Gill%'
) mem
	ON mem.id = mp.producer
INNER JOIN (
	SELECT
		id
	FROM movie
	WHERE startYear = 2017
) m
	ON m.id = mp.movie
GROUP BY
	mem.id,
	mem.name
'''

spark = SparkSession \
    .builder \
    .appName("Assignment 9 - Question 2") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

all_principals = spark.read.csv("C:/Users/14085/Downloads/title.principals.tsv.gz", header=True, sep="\t")
all_titles = spark.read.csv("C:/Users/14085/Downloads/title.basics.tsv.gz", header=True, sep="\t")
all_members = spark.read.csv("C:/Users/14085/Downloads/name.basics.tsv.gz", header=True, sep="\t")

all_titles = all_titles.withColumn(
    "genres",
    split(all_titles.genres, ",")
)

talkshows_2017 = \
    all_titles \
        .filter(
            (all_titles.startYear == 2017) &
            (array_contains(all_titles.genres, "Talk-Show"))
        )

filtered_members = \
    all_members \
        .filter(all_members.primaryName.like("%Gill%"))

result = \
    

print(filtered_members.show(30, False))
