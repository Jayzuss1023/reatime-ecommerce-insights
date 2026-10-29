import pyspark.sql.functions as F

silver_path = 'abfss://ecommerce@rtecommercestrg.dfs.core.windows.net/silver'
gold_path = 'abfss://ecommerce@rtecommercestrg.dfs.core.windows.net/gold'

# Read from silver
df_silver=(
    spark.readStream
    .format('delta')
    .load(silver_path)
)

# Aggregation: Total sales and total items sold per state per minute
# Watermark tells Spark that an order can arive up to 1 minute late.
# Anything older than the latest timespamp minus 1 minute is dropped
# Grouped into 1 minute buckets 10:00 - 10:01, 10:01 - 10:02
# Example layout:
# window_start	window_end	state	total_sales	total_items
# 10:00:00        10:01:00       CA      30.00        3
# 10:01:00        10:02:00       TX      15.00        3

df_gold=(
    df_silver
    .withWatermark('timestamp','1 minute')
    .groupBy(
        F.window('timestamp','1 minute'),
        'state'
    )
    .agg(
        F.sum('total_amount').alias('total_sales'),
        F.sum('quantity').alias('total_items')
    )
    .select(
        F.col('window.start').alias('window_start'),
        F.col('window.end').alias('window_end'),
        'state',
        'total_sales',
        'total_items'
    )
)

# Write to gold layer
(
    df_gold.writeStream
    .format('delta')
    .outputMode('append')
    .trigger(availableNow=True)
    .option('checkpointLocation',gold_path+'/_checkpoint')
    .start(gold_path)
)