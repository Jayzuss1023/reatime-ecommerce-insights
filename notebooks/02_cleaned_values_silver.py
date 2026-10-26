from pyspark.sql.functions import *

bronze_path = 'abfss://ecommerce@rtecommercestrg.dfs.core.windows.net/bronze'
silver_path = 'abfss://ecommerce@rtecommercestrg.dfs.core.windows.net/silver'

# Reading raw data from bronze
df_bronze=(
    spark.readStream
    .format('delta')
    .load(bronze_path)
)

# Clean and enrich
df_clean=(
    df_bronze
    .withColumn('timestamp',to_timestamp('timestamp'))
    .withColumn('price',when(col('price').isNull(),0.0).otherwise(col('price')))
    .withColumn('quantity',when(col('quantity').isNull(),1).otherwise(col('quantity')))
    .withColumn('total_amount',col('price')*col('quantity'))
    .filter(col('country')=='USA')
    .filter(col('state').isNotNull())
    .withWatermark('timestamp','10 minutes')
    .dropDuplicatesWithinWatermark(['order_id'])
)

# Write to Silver Layer
query=(
    df_clean.writeStream
    .format('delta')
    .outputMode('append')
    .trigger(availableNow=True)
    .option('checkpointLocation',silver_path+'/_checkpoint')
    .start(silver_path)
)
query.awaitTermination()

display(spark.read.format('delta').load(silver_path))
