from pyspark.sql.types import *
from pyspark.sql.functions import *


# Schema of dataset
schema = StructType([
    StructField('order_id', StringType()),
    StructField('timestamp', StringType()),
    StructField('customer_id', StringType()),
    StructField('product_id', StringType()),
    StructField('category', StringType()),
    StructField('price', DoubleType()),
    StructField('quantity', DoubleType()),
    StructField('total_amount', DoubleType()),
    StructField('city', StringType()),
    StructField('state', StringType()),
    StructField('country', StringType()),
    StructField('latitude', StringType()),
    StructField('longitude', StringType()),
    StructField('delivery_status', StringType())
])

# Azure Event Hub Configuration
event_hub_namespace = "<<NAMESPACE_HOST_NAME>>"
event_hub_name = "<<EVENT_HUB_NAME>>"
event_hub_conn_str = "<<YOUR_EVENT_HUB_CONNECTION_STRING>>"

eh_conf = {
    'kafka.bootstrap.servers': f"{event_hub_namespace}:9093",
    'subscribe': event_hub_name,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{event_hub_conn_str}";',
    'startingOffsets': 'latest',
    'failOnDataLoss': 'false'
}


# Stream from Event Hub
df_raw = (
    spark.readStream
    .format('kafka')
    .options(**eh_conf)
    .load()
)

# Parse JSON from kafka stream
# The binary payload is converted into one long string that follows the schema above
df_orders = (
    df_raw.selectExpr("CAST(value AS STRING) as json")
    .select(from_json('json',schema).alias('data'))
    .select('data.*')
)


bronze_path = 'abfss://ecommerce@rtecommercestrg.dfs.core.windows.net/bronze'

query = (
    df_orders.writeStream
    .format('delta')
    .outputMode('append')
    .trigger(availableNow=True)
    .option("checkpointLocation",bronze_path+"/_checkpoint")
    .start(bronze_path)
)

query.awaitTermination()

df_bronze=spark.read.format('delta').load(bronze_path)
display(df_bronze)