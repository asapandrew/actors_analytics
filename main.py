from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Film data analytics").getOrCreate()

# Reading CSV in DataFrame
df_actors = spark.read.csv("actors.csv", header=True, inferSchema=True, sep=",")
df_movie_actors = spark.read.csv(
    "movie_actors.csv", header=True, inferSchema=True, sep=","
)
df_movies = spark.read.csv("movies.csv", header=True, inferSchema=True, sep=",")

# Creating temporary views
df_actors.createOrReplaceTempView("actors")
df_movie_actors.createOrReplaceTempView("movie_actors")
df_movies.createOrReplaceTempView("movies")

# Top 5 genres by movies count
df_top5_genres = spark.sql(
    """
SELECT genre, count(movie_id) as cnt_movies
from movies
group by genre
order by cnt_movies desc
limit 5
"""
)

# Actor with most movies count
df_actor_most_movies_cnt = spark.sql(
    """
select a.name, count(m.movie_id) as cnt_movies
from actors a
inner join movie_actors ma on ma.actor_id = a.actor_id
inner join movies m on m.movie_id = ma.movie_id
group by a.name
order by cnt_movies desc
limit 1                                    
"""
)

# Average movie budget by genre
df_avg_movie_budget = spark.sql(
    """
SELECT genre, avg(budget) as average_budget
from movies
group by genre                                  
"""
)

# Movie with more than 1 actor from same country
df_movie_actor_same_country = spark.sql(
    """
select m.title, a.country, count(a.actor_id) as cnt_actors
from actors a
inner join movie_actors ma on ma.actor_id = a.actor_id
inner join movies m on m.movie_id = ma.movie_id
group by m.title, a.country    
having count(a.actor_id) > 1                             
"""
)


spark.stop()
