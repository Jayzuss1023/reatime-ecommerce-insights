from pyspark.sql.functions import *

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
    df.silver
    .withWatermark('timestamp','1 minute')
    .groupBy(
        window('timestamp','1 minute'),
        'state'
    )
    .agg(
        sum('total_amount').alias('total_sales'),
        sum('quantity').alias('total_items')
    )
    .select(
        col('window.start').alias('window_start'),
        col('window.end').alias('window_end'),
        'state',
        'total_sales',
        'total_items'
    )
)