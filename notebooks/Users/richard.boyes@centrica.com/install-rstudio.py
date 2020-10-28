# Databricks notebook source
# MAGIC %sh
# MAGIC cat /etc/os-release

# COMMAND ----------

script = """#!/bin/bash

set -euxo pipefail
RSTUDIO_BIN="/usr/sbin/rstudio-server"


if [[ ! -f "$RSTUDIO_BIN" && $DB_IS_DRIVER = "TRUE" ]]; then
  apt-get -o Acquire::http::proxy=false update
  apt-get -o Acquire::http::proxy=false install -y gdebi-core
  cd /tmp
  # You can find new releases at https://rstudio.com/products/rstudio/download-server/debian-ubuntu/.
  wget wget https://download2.rstudio.org/server/bionic/amd64/rstudio-server-1.3.1093-amd64.deb
  sudo gdebi rstudio-server-1.3.1093-amd64.deb
  rstudio-server restart || true
fi
"""

# COMMAND ----------

dbutils.fs.mkdirs("/databricks/rstudio")

# COMMAND ----------

dbutils.fs.ls("/databricks/rstudio")

# COMMAND ----------

dbutils.fs.put("/databricks/rstudio/rstudio-install.sh", script, True)

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks/rstudio/rstudio-install.sh"))

# COMMAND ----------

