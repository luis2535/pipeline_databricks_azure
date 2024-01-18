// Databricks notebook source
// MAGIC %python
// MAGIC dbutils.fs.mkdirs("/mnt/dados")
// MAGIC

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt")
// MAGIC

// COMMAND ----------

// MAGIC %scala
// MAGIC val configs = Map(
// MAGIC   "fs.azure.account.auth.type" -> "OAuth",
// MAGIC   "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
// MAGIC   "fs.azure.account.oauth2.client.id" -> "2dd4f801-d23f-4cd9-a617-49dea18f81d6",
// MAGIC   "fs.azure.account.oauth2.client.secret" -> "y0T8Q~8Fm4z35wsCYnWQljVdWiPkxnUdp3goGcE_",
// MAGIC   "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/f880a7da-33fa-417d-bffd-bdb13c76ea89/oauth2/token")
// MAGIC // Optionally, you can add <directory-name> to the source URI of your mount point.
// MAGIC dbutils.fs.mount(
// MAGIC   source = "abfss://imoveis@recursosdatalake.dfs.core.windows.net/",
// MAGIC   mountPoint = "/mnt/dados",
// MAGIC   extraConfigs = configs)
// MAGIC

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/dados")

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/dados/inbound")

// COMMAND ----------

val path = "dbfs:/mnt/dados/inbound/dados_brutos_imoveis.json"
val dados = spark.read.json(path)


// COMMAND ----------

display(dados)

// COMMAND ----------

val dados_anuncio = dados.drop("imagens", "usuario")
display(dados_anuncio)

// COMMAND ----------

import org.apache.spark.sql.functions.col

// COMMAND ----------

val df_bronze = dados_anuncio.withColumn("id", col("anuncio.id"))
display(df_bronze)

// COMMAND ----------

val path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
df_bronze.write.format("delta").mode(SaveMode.Overwrite).save(path)

// COMMAND ----------


