from pyspark.sql import SparkSession
import plotly.express as px
import pandas as pd

# Inicialize a sessão Spark
spark = SparkSession.builder.appName("MovieAnalysis").getOrCreate()

movies_spark_df = spark.read.csv("data/movie.csv", header=True, inferSchema=True)
ratings_spark_df = spark.read.csv("data/rating.csv", header=True, inferSchema=True)

movies_spark_df.cache()
ratings_spark_df.cache()

avg_ratings_by_genre = ratings_spark_df.join(movies_spark_df, "movieId").groupBy("genres").avg("rating")

avg_ratings_by_genre_pandas = avg_ratings_by_genre.toPandas()
fig = px.bar(avg_ratings_by_genre_pandas, x="genres", y="avg(rating)", title="Avaliação Média por Gênero")
fig.show()

print("Análise de Avaliação Média por Gênero (Spark DataFrame):")
avg_ratings_by_genre.show()

avg_ratings_by_genre_sorted = avg_ratings_by_genre_pandas.sort_values(by="avg(rating)", ascending=False)

fig = px.bar(avg_ratings_by_genre_sorted, x="avg(rating)", y="genres", orientation="h", 
             title="Avaliação Média por Gênero", labels={"avg(rating)": "Avaliação Média"})
fig.show()

genre_counts = movies_spark_df.select("genres").groupBy("genres").count().toPandas()

fig_pie = px.pie(genre_counts, values="count", names="genres", 
                 title="Distribuição de Gêneros de Filmes")
fig_pie.update_traces(textinfo="percent+label")
fig_pie.show()

rating_ranges = ['0-1', '1-2', '2-3', '3-4', '4-5']

avg_ratings_by_genre_sorted["rating_range"] = pd.cut(avg_ratings_by_genre_sorted["avg(rating)"], bins=[0, 1, 2, 3, 4, 5])

fig_stacked = px.bar(avg_ratings_by_genre_sorted, x="genres", y="count", color="rating_range", 
                     title="Distribuição de Avaliações por Gênero", labels={"count": "Número de Filmes"})
fig_stacked.show()

spark.stop()
