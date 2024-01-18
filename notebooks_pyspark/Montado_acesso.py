# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "2dd4f801-d23f-4cd9-a617-49dea18f81d6",
          "fs.azure.account.oauth2.client.secret": "y0T8Q~8Fm4z35wsCYnWQljVdWiPkxnUdp3goGcE_",
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/f880a7da-33fa-417d-bffd-bdb13c76ea89/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://imoveis@recursosdatalake.dfs.core.windows.net/",
  mount_point = "/mnt/dadospy",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/dadospy")

# COMMAND ----------


