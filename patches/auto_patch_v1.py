df = df.withColumn("user_id", F.col("user_id").cast("string")) 
df = df.select(*["user_id"])