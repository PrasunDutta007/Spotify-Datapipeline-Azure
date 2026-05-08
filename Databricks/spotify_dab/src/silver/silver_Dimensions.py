# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import sys
project_pth = os.path.join(os.getcwd(),'..','..')
sys.path.append(project_pth)

from utils.transformations import reusable

# COMMAND ----------

# MAGIC %md
# MAGIC ### **AUTOLOADER**

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DimUser**

# COMMAND ----------

df_user = spark.read.format("parquet")\
        .load("abfss://bronze@spotifyprasunproject.dfs.core.windows.net/DimUser")

display(df_user)

# COMMAND ----------

df_user_preview = spark.read.format("parquet")\
        .load("abfss://bronze@spotifyprasunproject.dfs.core.windows.net/DimUser")

df_user_preview = df_user_preview.withColumn("user_name", upper(col("user_name")))

display(df_user_preview)

# COMMAND ----------

df_user = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "parquet")\
        .option("cloudFiles.schemaLocation", "abfss://silver@spotifyprasunproject.dfs.core.windows.net/DimUser/checkpoint")\
        .load("abfss://bronze@spotifyprasunproject.dfs.core.windows.net/DimUser")

# COMMAND ----------

df_user = df_user.withColumn("user_name", upper(col("user_name")))

# COMMAND ----------

df_user_obj = reusable()

df_user = df_user_obj.dropColumns(df_user,['_rescued_data'])
df_user = df_user.dropDuplicates(['user_id'])

# COMMAND ----------

df_user.writeStream.format("delta")\
        .option("checkpointLocation", "abfss://silver@spotifyprasunproject.dfs.core.windows.net/DimUser/checkpoint")\
        .trigger(once=True)\
        .option("path","abfss://silver@spotifyprasunproject.dfs.core.windows.net/DimUser/data")\
        .toTable("spotify_cata.silver.DimUser")

# COMMAND ----------

display(spark.read.format("delta").load("abfss://silver@spotifyprasunproject.dfs.core.windows.net/DimUser/data"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DimArtist**

# COMMAND ----------

df_art = spark.read.format("parquet")\
        .load("abfss://bronze@spotifyprasunproject.dfs.core.windows.net/DimArtist")

display(df_art)

# COMMAND ----------

df_art = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "parquet")\
        .option("cloudFiles.schemaLocation", "abfss://silver@spotifyprasunproject.dfs.core.windows.net/DimArt/checkpoint")\
        .load("abfss://bronze@spotifyprasunproject.dfs.core.windows.net/DimArtist")

# COMMAND ----------

df_art_obj = reusable()

df_art = df_art_obj.dropColumns(df_art,['_rescued_data'])
df_art = df_art.dropDuplicates(['artist_id'])

# COMMAND ----------

dbutils.fs.rm("abfss://silver@spotifyprasunproject.dfs.core.windows.net/DimArt/checkpoint_write", recurse=True)

# COMMAND ----------

df_art.writeStream.format("delta")\
        .option("checkpointLocation", "abfss://silver@spotifyprasunproject.dfs.core.windows.net/DimArt/checkpoint")\
        .trigger(once=True)\
        .option("path","abfss://silver@spotifyprasunproject.dfs.core.windows.net/DimArt/data")\
        .toTable("spotify_cata.silver.DimArt")

# COMMAND ----------

display(spark.read.format("delta").load("abfss://silver@spotifyprasunproject.dfs.core.windows.net/DimArt/data"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DimTrack**

# COMMAND ----------

df_track = spark.read.format("parquet")\
        .load("abfss://bronze@spotifyprasunproject.dfs.core.windows.net/DimTrack")

display(df_track)

# COMMAND ----------

df_track_preview = df_track.withColumn("durationFlag", when(col('duration_sec')<150, "low")\
                    .when(col('duration_sec')<300, "medium")\
                    .otherwise("high"))

df_track_preview = df_track_preview.withColumn("track_name", regexp_replace(col("track_name"), '-', ' '))

df_track_preview = reusable().dropColumns(df_track_preview, ['_rescued_data'])

display(df_track_preview)

# COMMAND ----------

df_track = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "parquet")\
        .option("cloudFiles.schemaLocation", "abfss://silver@spotifyprasunproject.dfs.core.windows.net/DimTrack/checkpoint")\
        .load("abfss://bronze@spotifyprasunproject.dfs.core.windows.net/DimTrack")

# COMMAND ----------

df_track = df_track.withColumn("durationFlag", when(col('duration_sec')<150, "low")\
                    .when(col('duration_sec')<300, "medium")\
                    .otherwise("high"))

df_track = df_track.withColumn("track_name", regexp_replace(col("track_name"), '-', ' '))

df_track = reusable().dropColumns(df_track, ['_rescued_data'])

# COMMAND ----------

df_track.writeStream.format("delta")\
        .option("checkpointLocation", "abfss://silver@spotifyprasunproject.dfs.core.windows.net/DimTrack/checkpoint")\
        .trigger(once=True)\
        .option("path","abfss://silver@spotifyprasunproject.dfs.core.windows.net/DimTrack/data")\
        .toTable("spotify_cata.silver.DimTrack")

# COMMAND ----------

display(spark.read.format("delta").load("abfss://silver@spotifyprasunproject.dfs.core.windows.net/DimTrack/data"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DimDate**

# COMMAND ----------

df_date = spark.read.format("parquet")\
        .load("abfss://bronze@spotifyprasunproject.dfs.core.windows.net/DimDate")

display(df_date)

# COMMAND ----------

df_date = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "parquet")\
        .option("cloudFiles.schemaLocation", "abfss://silver@spotifyprasunproject.dfs.core.windows.net/DimDate/checkpoint")\
        .load("abfss://bronze@spotifyprasunproject.dfs.core.windows.net/DimDate")

# COMMAND ----------

df_date = reusable().dropColumns(df_date, ["_rescued_data"])


# COMMAND ----------

df_date.writeStream.format("delta")\
        .option("checkpointLocation", "abfss://silver@spotifyprasunproject.dfs.core.windows.net/DimDate/checkpoint")\
        .trigger(once=True)\
        .option("path","abfss://silver@spotifyprasunproject.dfs.core.windows.net/DimDate/data")\
        .toTable("spotify_cata.silver.DimDate")

# COMMAND ----------

# Drop the table from Unity Catalog
spark.sql("DROP TABLE IF EXISTS spotify_cata.silver.DimDate")

# Clear the delta data
dbutils.fs.rm("abfss://silver@spotifyprasunproject.dfs.core.windows.net/DimDate/data", recurse=True)

# Clear the checkpoint
dbutils.fs.rm("abfss://silver@spotifyprasunproject.dfs.core.windows.net/DimDate/checkpoint", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **FactStream**

# COMMAND ----------

df_fact = spark.read.format("parquet")\
        .load("abfss://bronze@spotifyprasunproject.dfs.core.windows.net/FactStream")

display(df_fact)

# COMMAND ----------

df_fact = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "parquet")\
        .option("cloudFiles.schemaLocation", "abfss://silver@spotifyprasunproject.dfs.core.windows.net/FactStream/checkpoint")\
        .load("abfss://bronze@spotifyprasunproject.dfs.core.windows.net/FactStream")

# COMMAND ----------

df_fact = reusable().dropColumns(df_fact, ["_rescued_data"])

# COMMAND ----------

df_fact.writeStream.format("delta")\
        .option("checkpointLocation", "abfss://silver@spotifyprasunproject.dfs.core.windows.net/FactStream/checkpoint")\
        .trigger(once=True)\
        .option("path","abfss://silver@spotifyprasunproject.dfs.core.windows.net/FactStream/data")\
        .toTable("spotify_cata.silver.FactStream")

# COMMAND ----------

# MAGIC %md
# MAGIC #### SCD2 Check

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM spotify_cata.gold.dimtrack
# MAGIC WHERE `__END_AT` IS NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM spotify_cata.gold.dimtrack
# MAGIC WHERE track_id in (46,5)