# Databricks notebook source
dbutils.fs.ls("/mnt/dadospy/bronze")

# COMMAND ----------

path = "dbfs:/mnt/dadospy/bronze/pydataset_imoveis/"
df = spark.read.format("delta").load(path)
display(df)

# COMMAND ----------

display(df.select("anuncio.*"))


# COMMAND ----------

dados_detalhados = df.select("anuncio.*", "anuncio.endereco.*")

# COMMAND ----------

df_silver = dados_detalhados.drop("caracteristicas", "endereco")
display(df_silver)

# COMMAND ----------

spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")
path = "dbfs:/mnt/dadospy/silver/pydataset_imoveis"
df_silver.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(path)


# COMMAND ----------


