# Databricks notebook source
# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png)
# MAGIC 
# MAGIC <img width="920" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/pip-engineer-scientist.png'>
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC ### 3D = [EADS]
# MAGIC > 3 D's - é um conceito para estruturação empresarial dos silos de dados
# MAGIC 
# MAGIC **1. Data Lake** [Bronze] = *Staging*  
# MAGIC **2. Delta Lake** [Siver] = *Data Warehouse*  
# MAGIC **3. Data Sets** [Gold] = *DataSet*  
# MAGIC 
# MAGIC <br>
# MAGIC ### Data Engineering Deliverables  
# MAGIC > toda a parte de aquisição e tratamento do dados para disponibilização:  
# MAGIC > Data Lake, Analytical Data Store [Dw] e Datasets
# MAGIC 
# MAGIC   1. Data Collection [EL]
# MAGIC   2. Data Discovery
# MAGIC   3. Data Cleansing [T]
# MAGIC   4. **Delta Lake** = Data Warehouse
# MAGIC   5. **Delta Lake** = DataSet
# MAGIC 
# MAGIC <br>
# MAGIC ### Data Science Deliverables
# MAGIC > toda parte de entendimento dos dados para:  
# MAGIC > criação de um modelo matemático ou gráfico para análise de dados
# MAGIC 
# MAGIC   1. Data Exploration
# MAGIC   1. Data Wrangling [Feature Engineering]
# MAGIC   1. Data Analysis
# MAGIC   1. Machine Learning
# MAGIC   1. Storytelling
# MAGIC   <br>
# MAGIC   <br>

# COMMAND ----------

# DBTITLE 1,Utilizando o Banco de Dados Yelp
# MAGIC %sql
# MAGIC 
# MAGIC USE Yelp

# COMMAND ----------

# DBTITLE 1,Dimensão = yelp_users_delta = Delta Lake
# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM yelp_users_delta
# MAGIC LIMIT 100

# COMMAND ----------

# DBTITLE 1,Dimensão = yelp_business_delta = Delta Lake
# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM yelp_business_delta
# MAGIC LIMIT 100

# COMMAND ----------

# DBTITLE 1,PySpark = Fato [/delta/reviews] = Delta Lake
df_delta_reviews = spark.read.format("delta").load("/delta/reviews")
display(df_delta_reviews)

# COMMAND ----------

# DBTITLE 1,Criando a Tabela de [fact_reviews] no Delta Lake
# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS fact_reviews 
# MAGIC (
# MAGIC business_id STRING,
# MAGIC cool BIGINT,
# MAGIC date STRING,
# MAGIC funny BIGINT,
# MAGIC review_id STRING,
# MAGIC stars BIGINT,
# MAGIC text STRING,
# MAGIC useful BIGINT,
# MAGIC user_id STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/delta/reviews'

# COMMAND ----------

# DBTITLE 1,Quantidade de Registros = 175+ milhões de registros
# MAGIC %sql
# MAGIC 
# MAGIC --175.776.721
# MAGIC --313.707.629
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM fact_reviews

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png)
# MAGIC 
# MAGIC 
# MAGIC > geralmente segregamos o Delta Lake em 3 partes
# MAGIC > 1 - Bronze = Staging
# MAGIC > 2 - Silver = Dw
# MAGIC > 3 - Gold = DataSet
# MAGIC 
# MAGIC > é nesse momento que o *Data Scientist* assume a parte de ML + Pipeline
# MAGIC 
# MAGIC <img width="600" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/delta-lake-store.png'>
# MAGIC 
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Criando DataSet [Gold] no Delta Lake
# MAGIC %sql
# MAGIC 
# MAGIC --11.43 minutes
# MAGIC 
# MAGIC DROP TABLE IF EXISTS vw_reviews_sanitized;
# MAGIC 
# MAGIC CREATE TABLE vw_reviews_sanitized
# MAGIC USING delta
# MAGIC AS
# MAGIC SELECT r.review_id, 
# MAGIC        r.business_id, 
# MAGIC        r.user_id, 
# MAGIC        r.stars AS review_stars, 
# MAGIC        r.text AS review_text, 
# MAGIC        r.useful AS review_useful, 
# MAGIC        
# MAGIC        b.name AS store_name, 
# MAGIC        b.city AS store_city, 
# MAGIC        b.state AS store_state, 
# MAGIC        b.category AS store_category, 
# MAGIC        b.review_count AS store_review_count, 
# MAGIC        b.stars AS store_stars, 
# MAGIC        
# MAGIC        u.name AS user_name, 
# MAGIC        u.average_stars AS user_average_stars, 
# MAGIC        u.importance AS user_importance
# MAGIC FROM fact_reviews AS r
# MAGIC INNER JOIN yelp_business_delta AS b
# MAGIC ON r.business_id = b.business_id
# MAGIC INNER JOIN yelp_users_delta AS u
# MAGIC ON r.user_id = u.user_id

# COMMAND ----------

# DBTITLE 1,Quantidade de Registros
# MAGIC %sql
# MAGIC 
# MAGIC --131.933.912
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM vw_reviews_sanitized

# COMMAND ----------

# DBTITLE 1,Lendo Registros
# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM vw_reviews_sanitized
# MAGIC LIMIT 10

# COMMAND ----------

