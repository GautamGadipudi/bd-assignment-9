from util import get_args

import timeit
from pyspark.sql import SparkSession
'''
SELECT 
	mem.* 
FROM movie_producer mp
	INNER JOIN (
		SELECT
			id
		FROM movie
		WHERE runtime > 120
	) m
		ON m.id = mp.movie
	INNER JOIN (
		SELECT
			*
		FROM member
		WHERE deathYear IS NULL
	) mem
		ON mem.id = mp.producer
GROUP BY
	mem.id,
	mem.name,
	mem.birthYear,
	mem.deathYear
'''

spark = SparkSession \
    .builder \
    .appName("Assignment 9 - Question 3") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

member_file, principal_file, title_file = get_args()

all_principals = spark.read.csv(principal_file, header=True, sep="\t")
all_titles = spark.read.csv(title_file, header=True, sep="\t")
all_members = spark.read.csv(member_file, header=True, sep="\t")

filtered_titles = all_titles.filter(all_titles.runtimeMinutes > 120)

alive_members = all_members.filter(all_members.deathYear == '\\N')

result = \
    all_principals \
        .filter(all_principals.category == 'producer') \
        .join(
            filtered_titles,
            filtered_titles.tconst == all_principals.tconst,
            "inner"
        ) \
        .join(
            alive_members,
            alive_members.nconst == all_principals.nconst,
            "inner"
        ) \
        .select(
            alive_members.nconst,
            alive_members.primaryName
        ) \
        .distinct()

start = timeit.default_timer()
print(result.show())
stop = timeit.default_timer()

print(f'Time: {str(stop - start)}')