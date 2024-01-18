# Databricks notebook source

dbutils.fs.ls("/mnt/dadospy/inbound")

# COMMAND ----------

# MAGIC %md
# MAGIC Lendo os dados na camada de inbound

# COMMAND ----------

path = "dbfs:/mnt/dadospy/inbound/dados_brutos_imoveis.json"
df = spark.read.json(path)
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Removendo colunas

# COMMAND ----------

df = df.drop("imagens", "usuario")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df_bronze = df.withColumn("id", col("anuncio.id"))
display(df_bronze)

# COMMAND ----------

spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")
path = "dbfs:/mnt/dadospy/bronze/pydataset_imoveis"
df.write.format("delta").save(path)


# COMMAND ----------


