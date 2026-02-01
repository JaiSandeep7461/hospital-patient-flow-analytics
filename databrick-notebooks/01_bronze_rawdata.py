from pyspark.sql.functions import *

event_hub_namespace = "hospital-analytics-namespace-latest.servicebus.windows.net"
event_hub_name="hospital-analytics-eh"  
event_hub_conn_str = dbutils.secrets.get(scope="hospitalanalyticsvaultscope",key="eventhub-connection")


kafka_options = {
    'kafka.bootstrap.servers': f"{event_hub_namespace}:9093",
    'subscribe': event_hub_name,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{event_hub_conn_str}";',
    'startingOffsets': 'latest',
    'failOnDataLoss': 'false'
}

# Read from eventhub

raw_df = (spark.readStream
          .format("kafka")
          .options(**kafka_options)
          .load())

# Cast data to json

json_df = raw_df.selectExpr("CAST(value AS STRING) as raw_json")

# # ADLS configuration 
# spark.conf.set(
#   "fs.azure.account.key.hospitalstorage01022026.dfs.core.windows.net",
#  dbutils.secrets.get(scope="hospitalanalyticsvaultscope",key="storage-account-key")
# )
# # bronze_path = "abfss://bronze@hospitalstorage01022026.core.windows.net/patient_flow"

# bronze_path = "abfss://bronze@hospitalstorage01022026.dfs.core.windows.net/patient_flow"


# checkpoint_path = "abfss://bronze@hospitalstorage01022026.dfs.core.windows.net/_checkpoints/patient_flow"

spark.conf.set(
  "fs.azure.account.key.hospitalstorage01022026.dfs.core.windows.net",
  dbutils.secrets.get(
      scope="hospitalanalyticsvaultscope",
      key="storage-account-key"
  )
)

bronze_path = "abfss://bronze@hospitalstorage01022026.dfs.core.windows.net/patient_flow"

checkpoint_path = "abfss://bronze@hospitalstorage01022026.dfs.core.windows.net/_checkpoints/patient_flow"


(
    json_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .start(bronze_path)
)
