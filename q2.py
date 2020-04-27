from util import get_args

import timeit
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType
import pyspark.sql.functions as f

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

member_file, principal_file, title_file = get_args()

all_principals = spark.read.csv(principal_file, header=True, sep="\t")
all_titles = spark.read.csv(title_file, header=True, sep="\t")
all_members = spark.read.csv(member_file, header=True, sep="\t")

all_titles = all_titles.withColumn(
    "genres",
    f.split(all_titles.genres, ",")
)

talkshows_2017 = \
    all_titles \
        .filter(
            (all_titles.startYear == 2017) &
            (f.array_contains(all_titles.genres, "Talk-Show"))
        )

filtered_members = \
    all_members \
        .filter(all_members.primaryName.like("%Gill%"))

result = \
    all_principals \
        .filter(all_principals.category == 'producer') \
        .join(
            talkshows_2017,
            talkshows_2017.tconst == all_principals.tconst,
            "inner"
        ) \
        .join(
            filtered_members,
            filtered_members.nconst == all_principals.nconst,
            "inner"
        ) \
        .groupBy(all_principals.nconst, filtered_members.primaryName) \
        .agg(f.count(all_principals.tconst).alias("ts_count"))
        

max_count = \
    result \
        .agg(f.max(result.ts_count).alias("max_ts_count"))

start = timeit.default_timer()
print(result.show())
print(max_count.show())
stop = timeit.default_timer()

print(f'Time: {str(stop - start)}')