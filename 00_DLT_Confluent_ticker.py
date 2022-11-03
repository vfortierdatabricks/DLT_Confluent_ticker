# Databricks notebook source
myschema="""
{
  "fields": [
    {
      "default": null,
      "name": "DOLLAR_AMOUNT",
      "type": [
        "null",
        {
          "logicalType": "decimal",
          "precision": 29,
          "scale": 13,
          "type": "bytes"
        }
      ]
    },
    {
      "default": null,
      "name": "SYMBOL",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "REGIONID",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "GENDER",
      "type": [
        "null",
        "string"
      ]
    }
  ],
  "name": "KsqlDataSourceSchema",
  "namespace": "io.confluent.ksql.avro_schemas",
  "type": "record"
}"""


# COMMAND ----------

import dlt

options = {
    "kafka.sasl.jaas.config": dbutils.secrets.get(scope="vfortier-keyvault-2", key="kafkajaasconfig"),
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol" : "SASL_SSL",
    "kafka.bootstrap.servers": dbutils.secrets.get(scope="vfortier-keyvault-2", key="kafkaserver"),
    "subscribe": "pksqlc-dv5d1STOCKAPP_TRADES_TRANSFORMED_ENRICHED"
}

from pyspark.sql.functions import *
from pyspark.sql.types import *
from_avro_options= {"mode":"PERMISSIVE"}

df = spark.readStream.format("kafka").options(**options).load()

raw_confluent_events = (spark.readStream
    .format("kafka")
    .options(**options)
    .load()
    )

@dlt.table(table_properties={"pipelines.reset.allowed":"false",
   "delta.autoOptimize.optimizeWrite": "true",
   "delta.autoOptimize.autoCompact": "true",
   "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_confluent_tickers_raw():
  return raw_confluent_events


# COMMAND ----------



from pyspark.sql.avro.functions import from_avro, to_avro
from pyspark.sql.functions import expr
from pyspark.sql.functions import col
from_avro_options= {"mode":"PERMISSIVE"}


@dlt.table(comment="Confluent Ticker events with proper schema",
           temporary=False,
           table_properties={
           "delta.autoOptimize.optimizeWrite": "true",
           "delta.autoOptimize.autoCompact": "true",
           "pipelines.autoOptimize.managed": "true"
    }
        )


def silver_ticker():
  return (
    dlt.read_stream("bronze_confluent_tickers_raw")
    .select(from_avro(expr("substring(value, 6, length(value)-5)"), myschema, from_avro_options).alias("my_avro"), col("timestamp")).select("my_avro.*", "*").drop("my_avro")     
  )

# COMMAND ----------

from pyspark.sql.avro.functions import from_avro, to_avro
from pyspark.sql.functions import expr
from pyspark.sql.functions import col
from_avro_options= {"mode":"PERMISSIVE"}


@dlt.table(comment="Table for the symbol ZVZZT, all regions",
           temporary=False,
           table_properties={
           "delta.autoOptimize.optimizeWrite": "true",
           "delta.autoOptimize.autoCompact": "true",
           "pipelines.autoOptimize.managed": "true"
    }
        )


def gold_ticker_ZVZZT():
  return (
    dlt.read_stream("silver_ticker")
    .where(col("SYMBOL") == "ZVZZT")     
  )

# COMMAND ----------



from pyspark.sql.avro.functions import from_avro, to_avro
from pyspark.sql.functions import expr
from pyspark.sql.functions import col
from_avro_options= {"mode":"PERMISSIVE"}


@dlt.table(comment="Table for the symbol ZJZZT",
           temporary=False,
           table_properties={
           "delta.autoOptimize.optimizeWrite": "true",
           "delta.autoOptimize.autoCompact": "true",
           "pipelines.autoOptimize.managed": "true"
    }
        )


def gold_ticker_ZJZZT():
  return (
    dlt.read_stream("silver_ticker")
    .where(col("SYMBOL") == "ZJZZT")     
  )

# COMMAND ----------



from pyspark.sql.avro.functions import from_avro, to_avro
from pyspark.sql.functions import expr
from pyspark.sql.functions import col
from_avro_options= {"mode":"PERMISSIVE"}


@dlt.table(comment="Table for the symbol ZTEST",
           temporary=False,
           table_properties={
           "delta.autoOptimize.optimizeWrite": "true",
           "delta.autoOptimize.autoCompact": "true",
           "pipelines.autoOptimize.managed": "true"
    }
        )


def gold_ticker_ZTEST():
  return (
    dlt.read_stream("silver_ticker")
    .where(col("SYMBOL") == "ZTEST")     
  )

# COMMAND ----------

from pyspark.sql.avro.functions import from_avro, to_avro
from pyspark.sql.functions import expr
from pyspark.sql.functions import col
from_avro_options= {"mode":"PERMISSIVE"}


@dlt.table(comment="Table for the symbol ZXZZT",
           temporary=False,
           table_properties={
           "delta.autoOptimize.optimizeWrite": "true",
           "delta.autoOptimize.autoCompact": "true",
           "pipelines.autoOptimize.managed": "true"
    }
        )


def gold_ticker_ZXZZT():
  return (
    dlt.read_stream("silver_ticker")
    .where(col("SYMBOL") == "ZXZZT")     
  )

# COMMAND ----------

from pyspark.sql.avro.functions import from_avro, to_avro
from pyspark.sql.functions import expr
from pyspark.sql.functions import col
from_avro_options= {"mode":"PERMISSIVE"}


@dlt.table(comment="Table for the symbol ZWZZT",
           temporary=False,
           table_properties={
           "delta.autoOptimize.optimizeWrite": "true",
           "delta.autoOptimize.autoCompact": "true",
           "pipelines.autoOptimize.managed": "true"
    }
        )


def gold_ticker_ZWZZT():
  return (
    dlt.read_stream("silver_ticker")
    .where(col("SYMBOL") == "ZWZZT")     
  )
