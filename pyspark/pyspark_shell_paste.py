# ===================== REPLACE THESE 2 PATHS ONLY =====================
SOURCE = "hdfs://localhost:9000/payment/payment_gateway_raw.csv"   # 1) file inside HDFS (input)
DEST   = "file:///home/YOURNAME/payment"                           # 2) folder on your normal disk (output)
# ======================================================================
# Paste everything below into the pyspark shell (the 'spark' object already exists there)
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.shuffle.partitions", "50")
cols = ["transaction_id","merchant_id","merchant_category","customer_id","amount","currency","payment_method","gateway","bank","status","failure_reason","txn_timestamp","city","device_type","processing_time_ms"]
schema = StructType([StructField(c, StringType(), True) for c in cols])
# ---- 1. READ from HDFS
raw = spark.read.csv(SOURCE, header=True, schema=schema)
total_raw = raw.count()
print("Rows read from HDFS:", total_raw)
# ---- 2. CLEAN: trim spaces, empty -> NULL
df = raw.select([F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c))).alias(c) for c in cols])
df = (df.withColumn("payment_method", F.regexp_replace(F.upper("payment_method"), "_", " ")).withColumn("gateway", F.upper("gateway")).withColumn("status", F.upper("status")).withColumn("currency", F.upper("currency")).withColumn("bank", F.upper("bank")).withColumn("city", F.initcap("city")).withColumn("amount", F.col("amount").cast("double")).withColumn("processing_time_ms", F.col("processing_time_ms").cast("int")).withColumn("txn_timestamp", F.coalesce(F.to_timestamp("txn_timestamp", "yyyy-MM-dd HH:mm:ss"), F.to_timestamp("txn_timestamp", "dd/MM/yyyy HH:mm"))))
df = df.withColumn("failure_reason", F.when((F.col("status") == "FAILED") & F.col("failure_reason").isNull(), "UNKNOWN").otherwise(F.col("failure_reason")))
rows_before = df.count()
df = df.dropDuplicates(["transaction_id"])
duplicates_removed = rows_before - df.count()
# ---- 3. VALIDATE: every broken rule adds a reason
df = df.withColumn("reject_reason", F.concat_ws(", ", F.when(F.col("transaction_id").isNull(), F.lit("missing_txn_id")), F.when(F.col("amount").isNull() | (F.col("amount") <= 0), F.lit("bad_amount")), F.when(F.col("txn_timestamp").isNull(), F.lit("bad_timestamp")), F.when(F.col("txn_timestamp") > F.current_timestamp(), F.lit("future_timestamp")), F.when(~F.col("status").isin("SUCCESS","FAILED","PENDING","REFUNDED") | F.col("status").isNull(), F.lit("invalid_status")), F.when(~F.col("currency").isin("INR","USD") | F.col("currency").isNull(), F.lit("invalid_currency")), F.when(F.col("processing_time_ms").isNull() | (F.col("processing_time_ms") < 0), F.lit("bad_processing_time"))))
df = df.cache()
valid = df.filter(F.col("reject_reason") == "").drop("reject_reason")
rejected = df.filter(F.col("reject_reason") != "")
valid_count = valid.count()
rejected_count = rejected.count()
print("Valid:", valid_count, "| Rejected:", rejected_count, "| Duplicates removed:", duplicates_removed)
dq = rejected.select(F.explode(F.split("reject_reason", ", ")).alias("rule")).groupBy("rule").count().orderBy(F.desc("count"))
dq.show(truncate=False)
# ---- 4. ENRICH: new columns + broadcast join with small fee table
fees = spark.createDataFrame([("RAZORPAY", 2.0), ("PAYU", 1.8), ("PAYTM", 1.5), ("CCAVENUE", 2.2), ("STRIPE", 2.9)], ["gateway", "fee_pct"])
valid = (valid.withColumn("amount_inr", F.when(F.col("currency") == "USD", F.col("amount") * 83).otherwise(F.col("amount"))).withColumn("is_success", F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).withColumn("txn_date", F.to_date("txn_timestamp")).withColumn("hour", F.hour("txn_timestamp")).withColumn("year", F.year("txn_timestamp")).withColumn("month", F.month("txn_timestamp")).join(F.broadcast(fees), "gateway", "left").withColumn("fee", F.when(F.col("is_success") == 1, F.col("amount_inr") * F.col("fee_pct") / 100).otherwise(0.0)))
# ---- 5. SAVE clean + rejected to your normal disk
valid.write.mode("overwrite").partitionBy("year", "month").parquet(DEST + "/clean")
rejected.write.mode("overwrite").option("header", True).csv(DEST + "/rejected")
# ---- 6. SUMMARY TABLES for the dashboard  ->  DEST/output
tx = spark.read.parquet(DEST + "/clean")
metrics = lambda g: g.agg(F.count("*").alias("txns"), F.round(F.avg("is_success") * 100, 2).alias("success_rate"), F.round(F.avg("processing_time_ms"), 0).alias("avg_ms"), F.round(F.sum(F.when(F.col("is_success") == 1, F.col("amount_inr")).otherwise(0)), 2).alias("volume"), F.round(F.sum("fee"), 2).alias("fee"))
save = lambda frame, name: frame.coalesce(1).write.mode("overwrite").parquet(DEST + "/output/" + name)
save(metrics(tx.groupBy()), "kpi")
save(metrics(tx.groupBy("txn_date")).orderBy("txn_date"), "daily")
save(metrics(tx.groupBy("gateway")), "gateway")
save(metrics(tx.groupBy("payment_method")), "method")
save(metrics(tx.groupBy("hour")).orderBy("hour"), "hourly")
save(metrics(tx.filter(F.col("bank").isNotNull()).groupBy("bank")), "bank")
save(metrics(tx.filter(F.col("city").isNotNull()).groupBy("city")), "city")
save(metrics(tx.groupBy("merchant_category")), "category")
save(metrics(tx.groupBy("gateway", "payment_method")), "gateway_method")
save(tx.filter(F.col("status") == "FAILED").groupBy("failure_reason").count().orderBy(F.desc("count")), "failure_reasons")
save(tx.groupBy("status").count(), "status")
save(spark.createDataFrame([(total_raw, duplicates_removed, valid_count, rejected_count)], ["raw_rows", "duplicates_removed", "valid_rows", "rejected_rows"]), "summary")
save(dq, "dq_report")
print("DONE. Dashboard data saved in:", DEST + "/output")
