from util import get_args

import timeit
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import split, regexp_replace, array_contains

'''
SELECT 
mem.id, mem.name
FROM actor_movie_role amr
INNER JOIN (
	SELECT 
		*
	FROM member
	WHERE deathYear IS NULL
) mem
	ON mem.id = amr.actor
INNER JOIN (
	SELECT
		id
	FROM role
	WHERE name = 'Jesus' OR name = 'Christ' OR name = 'Jesus Christ'
) r
	ON r.id = amr.role
GROUP BY mem.id, mem.name
'''

spark = SparkSession \
    .builder \
    .appName("Assignment 9 - Question 4") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

member_file, principal_file = get_args(True)

all_principals = spark.read.csv(principal_file, header=True, sep="\t")
all_members = spark.read.csv(member_file, header=True, sep="\t")

all_principals2 = \
    all_principals \
        .withColumn(
            "characters2",
            regexp_replace(all_principals.characters, "[\[\]]", "")
        )

all_principals3 = \
    all_principals2 \
        .withColumn(
            "characters_array",
            split(all_principals2.characters2, ",\s*").cast(ArrayType(StringType()))
        )

alive_members = \
    all_members \
        .filter(all_members.deathYear == "\\N")

result = \
    all_principals3 \
        .filter(
            (all_principals3.category == 'actor') &
            (
                (array_contains(all_principals3.characters_array, "Christ")) |
                (array_contains(all_principals3.characters_array, "Jesus")) |
                (array_contains(all_principals3.characters_array, "Jesus Christ"))
            )
        ) \
        .join(
            alive_members,
            alive_members.nconst == all_principals3.nconst,
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