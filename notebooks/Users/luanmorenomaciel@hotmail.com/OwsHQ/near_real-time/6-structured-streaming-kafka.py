# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC > https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#fault-tolerance-semantics  
# MAGIC > https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html  
# MAGIC > https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html  

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # SparkSQL ~ [Structured Streaming]
# MAGIC > spark-sql possui a habilidade de realizar streaming, 
# MAGIC > o streaming estruturado faz com que seje possível a conexão   
# MAGIC > com fontes de dados como file e kafka para ingestão dos dados em **stream** em grande escala, com sql
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC 
# MAGIC #### Structured Streaming
# MAGIC 
# MAGIC > **Stream Processing on Spark SQL Engine** = Fast | Scalable | Fault-Tolerant  
# MAGIC > **Deal with Complex Data & Complex Workloads** = Rich | Unified | High-Level APIs  
# MAGIC > **Rich Ecosystem of Data Sources** = Integrate with Many Storage Systems  
# MAGIC <br>
# MAGIC   
# MAGIC <img width="800px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/structured-streaming.png'>
# MAGIC   
# MAGIC <br>
# MAGIC > sparksql + structured streaming  
# MAGIC > https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # delta.`/delta/reviews`

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC USE Yelp

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > **reviews** são as informações de todas as revisões realizadas por um *usuário* para um ou mais *locais*, 
# MAGIC > com isso, iremos usar a capacidade do **SparkSQL + SparkStreaming + Delta** para persistir de forma eficiente,  
# MAGIC > as informações estruturadas no layer do *delta lake*

# COMMAND ----------

# DBTITLE 1,Listando Novos Arquivos vindo da Aplicação [Data Lake] - Landing Zone
# MAGIC %fs ls "dbfs:/mnt/bs-stg-files/yelp_dataset"

# COMMAND ----------

# DBTITLE 1,Structured Streaming com PySpark - [Anatomia do Streaming]
# importando biblioteca
from pyspark.sql.types import *

# definindo local de entrada [Data Lake]
inputPath = "dbfs:/mnt/bs-stg-files/yelp_dataset"

# streaming estruturado
# necessita a definição do streaming
# JSON vs. Parquet [implicit schema]

# estruturando a estrutura JSON
jsonSchema = StructType(
[
 StructField('business_id', StringType(), True), 
 StructField('cool', LongType(), True), 
 StructField('date', StringType(), True), 
 StructField('funny', LongType(), True), 
 StructField('review_id', StringType(), True), 
 StructField('stars', LongType(), True), 
 StructField('text', StringType(), True), 
 StructField('useful', LongType(), True), 
 StructField('user_id', StringType(), True)
]
)

# definindo structured streaming
StreamingDfReviews = (
     spark
    .readStream
    .schema(jsonSchema)
    .option("maxFilesPerTrigger", 1)
    .json(inputPath)
)

# verificando streaming
StreamingDfReviews.isStreaming

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) Source
# MAGIC 
# MAGIC > como podemos ver, o DataFrame = **StreamingDfReviews** é de de fato um streaming pelo tipo de ingestão
# MAGIC > e por ser configurado como **readStream**

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img width="300px" src="https://docs.delta.io/latest/_static/delta-lake-logo.png"> 
# MAGIC <br>
# MAGIC ### Sink 
# MAGIC 
# MAGIC > uma das capacidades do **Delta Lake** é ser configurado como **sink** utilizando *structured streaming*  
# MAGIC > isso irá comitar [ACID] os dados de forma *exactly-once processing* quando criado pelo streaming  
# MAGIC > e não irá competir contra por exemplo um bach executando ao mesmo momento

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > salvando o streaming diretamente no **Delta Lake**  
# MAGIC > structured streaming com delta lake = https://docs.databricks.com/delta/delta-streaming.html

# COMMAND ----------

# DBTITLE 1,Iniciando o Processamento com Structured Streaming [Delta Lake]
spark.conf.set("spark.sql.shuffle.partitions", "2")  

StreamingDfReviews \
  .writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/delta/reviews/_checkpoints/reviews-file-source") \
  .start("/delta/reviews") \

# COMMAND ----------

# DBTITLE 1,Lendo Registros do Delta Lake em Streaming [Parte 1]
# MAGIC %sql
# MAGIC 
# MAGIC --145.791.741
# MAGIC --295.716.641
# MAGIC --307.710.633
# MAGIC 
# MAGIC SELECT COUNT(*) AS reviews
# MAGIC FROM delta.`/delta/reviews`

# COMMAND ----------

from time import sleep
sleep(30)

# COMMAND ----------

# DBTITLE 1,Lendo Registros do Delta Lake em Streaming [Parte 1]
# MAGIC %sql
# MAGIC 
# MAGIC --145.791.741
# MAGIC --295.716.641
# MAGIC --307.710.633
# MAGIC 
# MAGIC SELECT COUNT(*) AS reviews
# MAGIC FROM delta.`/delta/reviews`

# COMMAND ----------

# DBTITLE 1,Agrupamento dos Dados em Streaming usando Delta Lake
# MAGIC %sql
# MAGIC 
# MAGIC WITH data_summary_reviews AS 
# MAGIC (
# MAGIC SELECT date,
# MAGIC        SUM(stars) AS stars,
# MAGIC        COUNT(*) AS reviews
# MAGIC FROM delta.`/delta/reviews`
# MAGIC WHERE date BETWEEN '2018-01-01' AND '2018-02-01'
# MAGIC GROUP BY date
# MAGIC )
# MAGIC SELECT date,
# MAGIC        reviews
# MAGIC FROM data_summary_reviews
# MAGIC ORDER BY date ASC

# COMMAND ----------

from time import sleep 
sleep(30)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC WITH data_summary_reviews AS 
# MAGIC (
# MAGIC SELECT date,
# MAGIC        SUM(stars) AS stars,
# MAGIC        COUNT(*) AS reviews
# MAGIC FROM delta.`/delta/reviews`
# MAGIC WHERE date BETWEEN '2018-01-01' AND '2018-02-01'
# MAGIC GROUP BY date
# MAGIC )
# MAGIC SELECT date,
# MAGIC        reviews
# MAGIC FROM data_summary_reviews
# MAGIC ORDER BY date ASC

# COMMAND ----------

# DBTITLE 1,PySpark - Lendo Dados do Delta Lake
df_delta_reviews = spark.read.format("delta").load("/delta/reviews")
display(df_delta_reviews)

# COMMAND ----------

# DBTITLE 1,Contando Quantidade de Registros [PySpark]
df_delta_reviews.count()

# COMMAND ----------

# DBTITLE 1,Contando Quantidade de Registros [SparkSQL]
# MAGIC %sql
# MAGIC 
# MAGIC --175.776.721
# MAGIC --313.707.629
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM delta.`/delta/reviews`

# COMMAND ----------

