# Databricks notebook source
# MAGIC %md
# MAGIC ## Query the COVID19 API and retrieve data
# MAGIC 
# MAGIC We swiped most of this code from the government COVID data API

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from covid_sum

# COMMAND ----------

from typing import Iterable, Dict, Union, List
from json import dumps
from requests import get
from http import HTTPStatus


StructureType = Dict[str, Union[dict, str]]
FiltersType = Iterable[str]
APIResponseType = Union[List[StructureType], str]


def get_paginated_dataset(filters: FiltersType, structure: StructureType,
                          as_csv: bool = False) -> APIResponseType:
    """
    Extracts paginated data by requesting all of the pages
    and combining the results.

    Parameters
    ----------
    filters: Iterable[str]
        API filters. See the API documentations for additional
        information.

    structure: Dict[str, Union[dict, str]]
        Structure parameter. See the API documentations for
        additional information.

    as_csv: bool
        Return the data as CSV. [default: ``False``]

    Returns
    -------
    Union[List[StructureType], str]
        Comprehensive list of dictionaries containing all the data for
        the given ``filters`` and ``structure``.
    """
    endpoint = "https://api.coronavirus.data.gov.uk/v1/data"

    api_params = {
        "filters": str.join(";", filters),
        "structure": dumps(structure, separators=(",", ":")),
        "format": "json" if not as_csv else "csv"
    }

    data = list()

    page_number = 1

    while True:
        # Adding page number to query params
        api_params["page"] = page_number

        response = get(endpoint, params=api_params, timeout=10)

        if response.status_code >= HTTPStatus.BAD_REQUEST:
            raise RuntimeError(f'Request failed: {response.text}')
        elif response.status_code == HTTPStatus.NO_CONTENT:
            break

        if as_csv:
            csv_content = response.content.decode()

            # Removing CSV header (column names) where page 
            # number is greater than 1.
            if page_number > 1:
                data_lines = csv_content.split("\n")[1:]
                csv_content = str.join("\n", data_lines)

            data.append(csv_content.strip())
            page_number += 1
            continue

        current_data = response.json()
        page_data: List[StructureType] = current_data['data']
        
        data.extend(page_data)

        # The "next" attribute in "pagination" will be `None`
        # when we reach the end.
        if current_data["pagination"]["next"] is None:
            break

        page_number += 1

    if not as_csv:
        return data

    # Concatenating CSV pages
    return str.join("\n", data)


# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import StringType,BooleanType,DateType

# Get the JSON data and convert it to a spark dataframe

query_filters = [
    f"areaType=ltla"
]

query_structure = {
    "date": "date",
    "name": "areaName",
    "code": "areaCode",
    "daily": "newCasesBySpecimenDate",
    "cumulative": "cumCasesBySpecimenDate"
}

json_data = get_paginated_dataset(query_filters, query_structure)

sdf_covid = spark.createDataFrame(json_data).withColumn("date", col("date").cast(DateType()))


# COMMAND ----------

# Chuck it into the deltalake as a managed table

sdf_covid.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .option("path", "/mnt/deltalake/covid") \
  .partitionBy("code") \
  .saveAsTable("covid")

# COMMAND ----------

from delta.tables import *

dl = DeltaTable.forName(spark, 'covid_sum')

display(dl.history())

# COMMAND ----------

display(sdf_covid.groupBy("name").sum("daily").withColumnRenamed("sum(daily)", "sum"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Structured streaming group by
# MAGIC 
# MAGIC We use a group by and structured streaming to get out the grand total of covid cases for each city

# COMMAND ----------

spark.readStream \
  .format("delta") \
  .option("ignoreChanges", "True") \
  .table('covid') \
  .groupBy("name") \
  .sum("daily") \
  .withColumnRenamed("sum(daily)", "sum") \
  .writeStream \
  .trigger(once=True) \
  .format("delta") \
  .option("checkpointLocation", "/mnt/deltalake/covid_sum/_checkpoint") \
  .outputMode("complete") \
  .table("covid_sum")
