# Databricks notebook source
# This took bloody ages to figure out, see: https://docs.google.com/document/d/1JG2c4vzfJ8yA64eBHCeu2lq129GDh8zRXd_Cc2FFraY/edit#heading=h.1847089san7i

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "3db4a48c-2d9c-4144-a4dd-84eaa1286d1d",
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="deltalake-secret",key="deltalake-credential"),
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/a603898f-7de2-45ba-b67d-d35fb519b2cf/oauth2/token"}


dbutils.fs.mount(
  source = "abfss://deltalake@adworksingestion.dfs.core.windows.net/",
  mount_point = "/mnt/deltalake",
  extra_configs = configs)



# COMMAND ----------

from pyspark.sql.functions import expr
from pyspark.sql.functions import from_unixtime

events = spark.read \
  .option("inferSchema", "true") \
  .json("/databricks-datasets/structured-streaming/events/") \
  .withColumn("date", expr("time")) \
  .drop("time") \
  .withColumn("date", from_unixtime("date", 'yyyy-MM-dd'))
  
display(events)

# COMMAND ----------

events.write.format("delta").mode("overwrite").partitionBy("date").save("/mnt/deltalake/events/")

# COMMAND ----------

delta_events = spark.sql("DROP TABLE EVENTS")

# COMMAND ----------

display(spark.sql("CREATE TABLE events USING DELTA LOCATION '/mnt/deltalake/events/'"))


# COMMAND ----------

display(spark.sql("DESCRIBE HISTORY events"))

# COMMAND ----------

display(spark.sql("DESCRIBE DETAIL events"))

# COMMAND ----------

display(spark.sql("SELECT action, count(action) FROM events group by action"))