from util import get_args

import timeit
from pyspark.sql import SparkSession

'''
SELECT 
	mem.id, 
	mem.name, 
	mem.deathYear
FROM 
	movie_actor ma
	INNER JOIN 
	(
		SELECT 
			id 
		FROM 
			movie 
		WHERE startYear = 2014
			AND type = 'movie'
	) m
		ON m.id = ma.movie
	INNER JOIN
	(
		SELECT 
			*
		FROM 
			member
		WHERE name like 'Phi%'
			AND deathYear IS NULL
	) mem
		ON mem.id != ma.actor
GROUP BY 
	mem.id, 
	mem.name, 
	mem.deathYear
'''

spark = SparkSession \
    .builder \
    .appName("Assignment 9 - Question 1") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

member_file, principal_file, title_file = get_args()

all_principals = spark.read.csv(principal_file, header=True, sep="\t")
all_titles = spark.read.csv(title_file, header=True, sep="\t")
all_members = spark.read.csv(member_file, header=True, sep="\t")

movies_2014 = \
    all_titles \
        .filter(
            (all_titles.startYear == 2014) &
            (all_titles.titleType == 'movie')
        )

phi_alive_members = \
    all_members \
        .filter(
            (all_members.deathYear == '\\N') & 
            (all_members.primaryName.like('Phi%'))
        )

result = \
    all_principals \
        .filter(all_principals.category == 'actor') \
        .join(
                movies_2014,
                movies_2014.tconst == all_principals.tconst,
                "inner"
        ) \
        .join(
                phi_alive_members,
                phi_alive_members.nconst != all_principals.nconst,
                "inner"
        ) \
        .select(
            phi_alive_members.nconst, 
            phi_alive_members.primaryName
        ) \
        .distinct()

start = timeit.default_timer()
print(result.show())
stop = timeit.default_timer()

print(f'Time: {str(stop - start)}')