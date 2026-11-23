# Databricks notebook source
# MAGIC %scala
# MAGIC
# MAGIC // ==========================================
# MAGIC // PROJECT NEXUS
# MAGIC // INCREMENTAL BATCH LOADING
# MAGIC // STEP 1: CONFIGURATION
# MAGIC // ==========================================
# MAGIC
# MAGIC val basePath =
# MAGIC   "abfss://adlsdatabrics2c@adlsdatabricks2.dfs.core.windows.net/nexus/batch/"
# MAGIC
# MAGIC val incrementalFilePath =
# MAGIC   basePath + "incremental/orders_update.csv"
# MAGIC
# MAGIC val bronzeOrdersPath =
# MAGIC   basePath + "bronze/orders"
# MAGIC
# MAGIC println("PROJECT NEXUS - Incremental Batch Pipeline")
# MAGIC println("------------------------------------------")
# MAGIC println(s"Incoming File: $incrementalFilePath")
# MAGIC println(s"Bronze Path:   $bronzeOrdersPath")
# MAGIC println("Configuration initialized successfully")
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC val incrementalFolder =
# MAGIC   "abfss://adlsdatabrics2c@adlsdatabricks2.dfs.core.windows.net/nexus/batch/incremental/"
# MAGIC
# MAGIC display(dbutils.fs.ls(incrementalFolder))

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val basePath =
# MAGIC   "abfss://adlsdatabrics2c@adlsdatabricks2.dfs.core.windows.net/nexus/batch/"
# MAGIC
# MAGIC val incrementalPath =
# MAGIC   basePath + "incremental/orders_updates.csv"
# MAGIC
# MAGIC val incrementalDF = spark.read
# MAGIC   .option("header", "true")
# MAGIC   .option("inferSchema", "false")
# MAGIC   .option("mode", "PERMISSIVE")
# MAGIC   .csv(incrementalPath)
# MAGIC
# MAGIC println(s"Incoming record count: ${incrementalDF.count()}")
# MAGIC
# MAGIC display(incrementalDF)
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC val bronzeDeltaOrdersPath =
# MAGIC   "abfss://adlsdatabrics2c@adlsdatabricks2.dfs.core.windows.net/nexus/batch/bronze_delta/orders"
# MAGIC
# MAGIC val bronzeDeltaDF = spark.read
# MAGIC   .format("delta")
# MAGIC   .load(bronzeDeltaOrdersPath)
# MAGIC
# MAGIC println(s"Existing Delta orders: ${bronzeDeltaDF.count()}")
# MAGIC display(bronzeDeltaDF.limit(5))

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._
# MAGIC
# MAGIC val existingOrders = bronzeDeltaDF
# MAGIC   .select(
# MAGIC     col("order_id"),
# MAGIC     col("last_updated").cast("timestamp").as("existing_last_updated")
# MAGIC   )
# MAGIC
# MAGIC println(s"Existing Delta order count: ${existingOrders.count()}")

# COMMAND ----------

# MAGIC %scala
# MAGIC val incrementalFolder =
# MAGIC   "abfss://adlsdatabrics2c@adlsdatabricks2.dfs.core.windows.net/nexus/batch/incremental/"
# MAGIC
# MAGIC val incrementalFiles = dbutils.fs
# MAGIC   .ls(incrementalFolder)
# MAGIC   .filter(_.name.toLowerCase.endsWith(".csv"))
# MAGIC
# MAGIC require(incrementalFiles.nonEmpty, "No CSV files found in incremental folder")
# MAGIC
# MAGIC val incrementalDF = spark.read
# MAGIC   .option("header", "true")
# MAGIC   .option("inferSchema", "false")
# MAGIC   .csv(incrementalFiles.map(_.path): _*)
# MAGIC
# MAGIC println(s"Files found: ${incrementalFiles.size}")
# MAGIC println(s"Incoming records: ${incrementalDF.count()}")
# MAGIC
# MAGIC display(incrementalDF)

# COMMAND ----------

# MAGIC %scala
# MAGIC val incomingPrepared = incrementalDF
# MAGIC   .withColumn("quantity", col("quantity").cast("int"))
# MAGIC   .withColumn("incoming_last_updated",
# MAGIC     to_timestamp(col("last_updated"), "yyyy-MM-dd HH:mm:ss"))
# MAGIC   .withColumn("parsed_order_date", to_date(col("order_date"), "yyyy-MM-dd"))
# MAGIC
# MAGIC val joinedDF = incomingPrepared
# MAGIC   .join(existingOrders, Seq("order_id"), "left")
# MAGIC
# MAGIC val classifiedDF = joinedDF
# MAGIC   .withColumn(
# MAGIC     "operation",
# MAGIC     when(
# MAGIC       col("order_id").isNull ||
# MAGIC       col("customer_id").isNull ||
# MAGIC       col("product_id").isNull ||
# MAGIC       col("quantity").isNull ||
# MAGIC       (col("quantity") <= 0) ||
# MAGIC       col("parsed_order_date").isNull ||
# MAGIC       col("incoming_last_updated").isNull,
# MAGIC       "INVALID"
# MAGIC     )
# MAGIC     .when(col("existing_last_updated").isNull, "INSERT")
# MAGIC     .when(
# MAGIC       col("incoming_last_updated") <= col("existing_last_updated"),
# MAGIC       "STALE"
# MAGIC     )
# MAGIC     .otherwise("UPDATE")
# MAGIC   )
# MAGIC
# MAGIC display(
# MAGIC   classifiedDF.select(
# MAGIC     col("order_id"),
# MAGIC     col("quantity"),
# MAGIC     col("last_updated"),
# MAGIC     col("existing_last_updated"),
# MAGIC     col("operation")
# MAGIC   ).orderBy("order_id")
# MAGIC )

# COMMAND ----------

# MAGIC %scala
# MAGIC val insertDF = classifiedDF.filter(col("operation") === "INSERT")
# MAGIC val updateDF = classifiedDF.filter(col("operation") === "UPDATE")
# MAGIC val staleDF = classifiedDF.filter(col("operation") === "STALE")
# MAGIC val invalidDF = classifiedDF.filter(col("operation") === "INVALID")
# MAGIC
# MAGIC println(s"INSERT:  ${insertDF.count()}")
# MAGIC println(s"UPDATE:  ${updateDF.count()}")
# MAGIC println(s"STALE:   ${staleDF.count()}")
# MAGIC println(s"INVALID: ${invalidDF.count()}")

# COMMAND ----------

# MAGIC %scala
# MAGIC val silverIncrementalPath =
# MAGIC   "abfss://adlsdatabrics2c@adlsdatabricks2.dfs.core.windows.net/nexus/batch/silver/incremental/"
# MAGIC
# MAGIC insertDF.write.format("delta").mode("overwrite")
# MAGIC   .save(silverIncrementalPath + "inserts")
# MAGIC
# MAGIC updateDF.write.format("delta").mode("overwrite")
# MAGIC   .save(silverIncrementalPath + "updates")
# MAGIC
# MAGIC staleDF.write.format("delta").mode("overwrite")
# MAGIC   .save(silverIncrementalPath + "stale")
# MAGIC
# MAGIC invalidDF.write.format("delta").mode("overwrite")
# MAGIC   .save(silverIncrementalPath + "invalid")
# MAGIC
# MAGIC println("Incremental Silver datasets written successfully.")

# COMMAND ----------

# MAGIC %scala
# MAGIC val goldFactDF = spark.table(
# MAGIC   "adlsdatabricks2_workspace.gold.fact_order"
# MAGIC )
# MAGIC
# MAGIC println(s"Existing Gold orders: ${goldFactDF.count()}")
# MAGIC
# MAGIC goldFactDF.printSchema()
# MAGIC
# MAGIC display(goldFactDF.limit(10))

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("""
# MAGIC   SHOW TABLES IN adlsdatabricks2_workspace.gold
# MAGIC """).show(false)

# COMMAND ----------

# MAGIC %scala
# MAGIC val customerDim = spark.table(
# MAGIC   "adlsdatabricks2_workspace.gold.dim_customer"
# MAGIC )
# MAGIC
# MAGIC val productDim = spark.table(
# MAGIC   "adlsdatabricks2_workspace.gold.dim_product"
# MAGIC )
# MAGIC
# MAGIC val dateDim = spark.table(
# MAGIC   "adlsdatabricks2_workspace.gold.dim_date"
# MAGIC )
# MAGIC
# MAGIC customerDim.printSchema()
# MAGIC productDim.printSchema()
# MAGIC dateDim.printSchema()

# COMMAND ----------

# MAGIC
# MAGIC %scala
# MAGIC
# MAGIC val dimensionCheckDF = classifiedDF
# MAGIC   .filter(col("operation").isin("INSERT", "UPDATE"))
# MAGIC   .join(
# MAGIC     customerDim.select(
# MAGIC       col("customer_id"),
# MAGIC       col("customer_key")
# MAGIC     ),
# MAGIC     Seq("customer_id"),
# MAGIC     "left"
# MAGIC   )
# MAGIC   .join(
# MAGIC     productDim.select(
# MAGIC       col("product_id"),
# MAGIC       col("product_key")
# MAGIC     ),
# MAGIC     Seq("product_id"),
# MAGIC     "left"
# MAGIC   )
# MAGIC   .join(
# MAGIC     dateDim.select(
# MAGIC       col("full_date").as("parsed_order_date"),
# MAGIC       col("date_key")
# MAGIC     ),
# MAGIC     Seq("parsed_order_date"),
# MAGIC     "left"
# MAGIC   )
# MAGIC
# MAGIC // Check dimension mapping status
# MAGIC dimensionCheckDF
# MAGIC   .withColumn(
# MAGIC     "dimension_status",
# MAGIC     when(
# MAGIC       col("customer_key").isNull ||
# MAGIC       col("product_key").isNull ||
# MAGIC       col("date_key").isNull,
# MAGIC       "MISSING_DIMENSION"
# MAGIC     ).otherwise("MATCHED")
# MAGIC   )
# MAGIC   .groupBy("dimension_status")
# MAGIC   .count()
# MAGIC   .show(false)
# MAGIC
# MAGIC // Display records with missing dimension mappings
# MAGIC dimensionCheckDF
# MAGIC   .filter(
# MAGIC     col("customer_key").isNull ||
# MAGIC     col("product_key").isNull ||
# MAGIC     col("date_key").isNull
# MAGIC   )
# MAGIC   .select(
# MAGIC     col("order_id"),
# MAGIC     col("customer_id"),
# MAGIC     col("product_id"),
# MAGIC     col("parsed_order_date"),
# MAGIC     col("customer_key"),
# MAGIC     col("product_key"),
# MAGIC     col("date_key")
# MAGIC   )
# MAGIC   .show(false)
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %scala
# MAGIC
# MAGIC println("Missing customer IDs:")
# MAGIC dimensionCheckDF
# MAGIC   .filter(col("customer_key").isNull)
# MAGIC   .select("customer_id")
# MAGIC   .distinct()
# MAGIC   .show(false)
# MAGIC
# MAGIC println("Missing product IDs:")
# MAGIC dimensionCheckDF
# MAGIC   .filter(col("product_key").isNull)
# MAGIC   .select("product_id")
# MAGIC   .distinct()
# MAGIC   .show(false)
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %scala
# MAGIC
# MAGIC println("Customer IDs found in Bronze Delta:")
# MAGIC
# MAGIC bronzeDeltaDF
# MAGIC   .filter(col("customer_id").isin("CUST017", "CUST034"))
# MAGIC   .select("customer_id")
# MAGIC   .distinct()
# MAGIC   .show(false)
# MAGIC
# MAGIC println("Product IDs found in Bronze Delta:")
# MAGIC
# MAGIC bronzeDeltaDF
# MAGIC   .filter(col("product_id") === "PRD016")
# MAGIC   .select("product_id")
# MAGIC   .distinct()
# MAGIC   .show(false)
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %scala
# MAGIC
# MAGIC val basePath =
# MAGIC   "abfss://adlsdatabrics2c@adlsdatabricks2.dfs.core.windows.net/nexus/batch/"
# MAGIC
# MAGIC println("BRONZE FOLDERS")
# MAGIC dbutils.fs.ls(basePath + "bronze/")
# MAGIC   .foreach(x => println(x.path))
# MAGIC
# MAGIC println("\nSILVER FOLDERS")
# MAGIC dbutils.fs.ls(basePath + "silver/")
# MAGIC   .foreach(x => println(x.path))
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %scala
# MAGIC
# MAGIC println("SILVER CUSTOMERS")
# MAGIC dbutils.fs.ls(basePath + "silver/customers/")
# MAGIC   .foreach(x => println(x.name + " | " + x.path))
# MAGIC
# MAGIC println("\nSILVER PRODUCTS")
# MAGIC dbutils.fs.ls(basePath + "silver/products/")
# MAGIC   .foreach(x => println(x.name + " | " + x.path))
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %scala
# MAGIC
# MAGIC val silverCustomerDF = spark.read
# MAGIC   .format("delta")
# MAGIC   .load(basePath + "silver/customers/")
# MAGIC
# MAGIC val silverProductDF = spark.read
# MAGIC   .format("delta")
# MAGIC   .load(basePath + "silver/products/")
# MAGIC
# MAGIC println("CUSTOMER SCHEMA")
# MAGIC silverCustomerDF.printSchema()
# MAGIC
# MAGIC println("PRODUCT SCHEMA")
# MAGIC silverProductDF.printSchema()
# MAGIC
# MAGIC println("MISSING CUSTOMER RECORDS")
# MAGIC silverCustomerDF
# MAGIC   .filter(col("customer_id").isin("CUST017", "CUST034"))
# MAGIC   .show(false)
# MAGIC
# MAGIC println("MISSING PRODUCT RECORD")
# MAGIC silverProductDF
# MAGIC   .filter(col("product_id") === "PRD016")
# MAGIC   .show(false)
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %scala
# MAGIC
# MAGIC println("BRONZE CUSTOMER FILES")
# MAGIC dbutils.fs.ls(basePath + "bronze/customers/")
# MAGIC   .foreach(x => println(x.name + " | " + x.path))
# MAGIC
# MAGIC println("\nBRONZE PRODUCT FILES")
# MAGIC dbutils.fs.ls(basePath + "bronze/products/")
# MAGIC   .foreach(x => println(x.name + " | " + x.path))
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %scala
# MAGIC
# MAGIC val bronzeCustomerDF = spark.read
# MAGIC   .option("header", "true")
# MAGIC   .option("inferSchema", "true")
# MAGIC   .csv(basePath + "bronze/customers/customers.csv")
# MAGIC
# MAGIC val bronzeProductDF = spark.read
# MAGIC   .option("header", "true")
# MAGIC   .option("inferSchema", "true")
# MAGIC   .csv(basePath + "bronze/products/products.csv")
# MAGIC
# MAGIC println("BRONZE CUSTOMER RECORDS")
# MAGIC bronzeCustomerDF
# MAGIC   .filter(col("customer_id").isin("CUST017", "CUST034"))
# MAGIC   .show(false)
# MAGIC
# MAGIC println("BRONZE PRODUCT RECORD")
# MAGIC bronzeProductDF
# MAGIC   .filter(col("product_id") === "PRD016")
# MAGIC   .show(false)
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %scala
# MAGIC
# MAGIC import org.apache.spark.sql.expressions.Window
# MAGIC import org.apache.spark.sql.functions._
# MAGIC
# MAGIC // Find customer records missing from Gold
# MAGIC val missingCustomers = bronzeCustomerDF
# MAGIC   .join(
# MAGIC     customerDim.select("customer_id"),
# MAGIC     Seq("customer_id"),
# MAGIC     "left_anti"
# MAGIC   )
# MAGIC   .select(
# MAGIC     col("customer_id"),
# MAGIC     col("customer_name"),
# MAGIC     col("email"),
# MAGIC     col("city"),
# MAGIC     col("signup_date")
# MAGIC   )
# MAGIC
# MAGIC // Find product records missing from Gold
# MAGIC val missingProducts = bronzeProductDF
# MAGIC   .join(
# MAGIC     productDim.select("product_id"),
# MAGIC     Seq("product_id"),
# MAGIC     "left_anti"
# MAGIC   )
# MAGIC   .select(
# MAGIC     col("product_id"),
# MAGIC     col("product_name"),
# MAGIC     col("category"),
# MAGIC     col("unit_price").cast("decimal(10,2)").as("unit_price"),
# MAGIC     col("active")
# MAGIC   )
# MAGIC
# MAGIC // Generate new customer surrogate keys
# MAGIC val maxCustomerKey = customerDim
# MAGIC   .agg(coalesce(max("customer_key"), lit(0)))
# MAGIC   .first()
# MAGIC   .getInt(0)
# MAGIC
# MAGIC val customerWindow = Window.orderBy("customer_id")
# MAGIC
# MAGIC val newCustomerDim = missingCustomers
# MAGIC   .withColumn(
# MAGIC     "customer_key",
# MAGIC     (row_number().over(customerWindow) + lit(maxCustomerKey)).cast("int")
# MAGIC   )
# MAGIC   .select(
# MAGIC     "customer_key",
# MAGIC     "customer_id",
# MAGIC     "customer_name",
# MAGIC     "email",
# MAGIC     "city",
# MAGIC     "signup_date"
# MAGIC   )
# MAGIC
# MAGIC // Generate new product surrogate keys
# MAGIC val maxProductKey = productDim
# MAGIC   .agg(coalesce(max("product_key"), lit(0)))
# MAGIC   .first()
# MAGIC   .getInt(0)
# MAGIC
# MAGIC val productWindow = Window.orderBy("product_id")
# MAGIC
# MAGIC val newProductDim = missingProducts
# MAGIC   .withColumn(
# MAGIC     "product_key",
# MAGIC     (row_number().over(productWindow) + lit(maxProductKey)).cast("int")
# MAGIC   )
# MAGIC   .select(
# MAGIC     "product_key",
# MAGIC     "product_id",
# MAGIC     "product_name",
# MAGIC     "category",
# MAGIC     "unit_price",
# MAGIC     "active"
# MAGIC   )
# MAGIC
# MAGIC println("CUSTOMER DIMENSION RECORDS TO ADD")
# MAGIC newCustomerDim.show(false)
# MAGIC
# MAGIC println("PRODUCT DIMENSION RECORDS TO ADD")
# MAGIC newProductDim.show(false)
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %scala
# MAGIC
# MAGIC // Only customers required by the incremental orders
# MAGIC val requiredCustomers = bronzeCustomerDF
# MAGIC   .filter(col("customer_id").isin("CUST017", "CUST034"))
# MAGIC   .dropDuplicates(Seq("customer_id"))
# MAGIC   .select(
# MAGIC     col("customer_id"),
# MAGIC     col("customer_name"),
# MAGIC     col("email"),
# MAGIC     col("city"),
# MAGIC     col("signup_date")
# MAGIC   )
# MAGIC
# MAGIC // Only the product required by the incremental orders
# MAGIC val requiredProducts = bronzeProductDF
# MAGIC   .filter(col("product_id") === "PRD016")
# MAGIC   .dropDuplicates(Seq("product_id"))
# MAGIC   .select(
# MAGIC     col("product_id"),
# MAGIC     col("product_name"),
# MAGIC     col("category"),
# MAGIC     col("unit_price").cast("decimal(10,2)").as("unit_price"),
# MAGIC     col("active")
# MAGIC   )
# MAGIC
# MAGIC // Generate customer surrogate keys
# MAGIC val customerStartKey = customerDim
# MAGIC   .agg(coalesce(max("customer_key"), lit(0)))
# MAGIC   .first()
# MAGIC   .getInt(0)
# MAGIC
# MAGIC val requiredCustomerWindow = Window.orderBy("customer_id")
# MAGIC
# MAGIC val customersToAdd = requiredCustomers
# MAGIC   .withColumn(
# MAGIC     "customer_key",
# MAGIC     (row_number().over(requiredCustomerWindow) + lit(customerStartKey)).cast("int")
# MAGIC   )
# MAGIC   .select(
# MAGIC     "customer_key",
# MAGIC     "customer_id",
# MAGIC     "customer_name",
# MAGIC     "email",
# MAGIC     "city",
# MAGIC     "signup_date"
# MAGIC   )
# MAGIC
# MAGIC // Generate product surrogate key
# MAGIC val productStartKey = productDim
# MAGIC   .agg(coalesce(max("product_key"), lit(0)))
# MAGIC   .first()
# MAGIC   .getInt(0)
# MAGIC
# MAGIC val productsToAdd = requiredProducts
# MAGIC   .withColumn(
# MAGIC     "product_key",
# MAGIC     (row_number().over(Window.orderBy("product_id")) + lit(productStartKey)).cast("int")
# MAGIC   )
# MAGIC   .select(
# MAGIC     "product_key",
# MAGIC     "product_id",
# MAGIC     "product_name",
# MAGIC     "category",
# MAGIC     "unit_price",
# MAGIC     "active"
# MAGIC   )
# MAGIC
# MAGIC println("CUSTOMERS TO ADD")
# MAGIC customersToAdd.show(false)
# MAGIC
# MAGIC println("PRODUCTS TO ADD")
# MAGIC productsToAdd.show(false)
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %scala
# MAGIC
# MAGIC import io.delta.tables.DeltaTable
# MAGIC
# MAGIC // Add missing customers without duplicating existing customer IDs
# MAGIC DeltaTable.forName(
# MAGIC   "adlsdatabricks2_workspace.gold.dim_customer"
# MAGIC )
# MAGIC   .as("target")
# MAGIC   .merge(
# MAGIC     customersToAdd.as("source"),
# MAGIC     "target.customer_id = source.customer_id"
# MAGIC   )
# MAGIC   .whenNotMatched()
# MAGIC   .insertAll()
# MAGIC   .execute()
# MAGIC
# MAGIC // Add missing product without duplicating existing product IDs
# MAGIC DeltaTable.forName(
# MAGIC   "adlsdatabricks2_workspace.gold.dim_product"
# MAGIC )
# MAGIC   .as("target")
# MAGIC   .merge(
# MAGIC     productsToAdd.as("source"),
# MAGIC     "target.product_id = source.product_id"
# MAGIC   )
# MAGIC   .whenNotMatched()
# MAGIC   .insertAll()
# MAGIC   .execute()
# MAGIC
# MAGIC println("Gold dimension merge completed.")
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %scala
# MAGIC
# MAGIC val updatedCustomerDim = spark.table(
# MAGIC   "adlsdatabricks2_workspace.gold.dim_customer"
# MAGIC )
# MAGIC
# MAGIC val updatedProductDim = spark.table(
# MAGIC   "adlsdatabricks2_workspace.gold.dim_product"
# MAGIC )
# MAGIC
# MAGIC val updatedDateDim = spark.table(
# MAGIC   "adlsdatabricks2_workspace.gold.dim_date"
# MAGIC )
# MAGIC
# MAGIC val revalidatedDF = classifiedDF
# MAGIC   .filter(col("operation").isin("INSERT", "UPDATE"))
# MAGIC   .join(
# MAGIC     updatedCustomerDim.select("customer_id", "customer_key"),
# MAGIC     Seq("customer_id"),
# MAGIC     "left"
# MAGIC   )
# MAGIC   .join(
# MAGIC     updatedProductDim.select("product_id", "product_key"),
# MAGIC     Seq("product_id"),
# MAGIC     "left"
# MAGIC   )
# MAGIC   .join(
# MAGIC     updatedDateDim.select(
# MAGIC       col("full_date").as("parsed_order_date"),
# MAGIC       col("date_key")
# MAGIC     ),
# MAGIC     Seq("parsed_order_date"),
# MAGIC     "left"
# MAGIC   )
# MAGIC
# MAGIC revalidatedDF
# MAGIC   .withColumn(
# MAGIC     "dimension_status",
# MAGIC     when(
# MAGIC       col("customer_key").isNull ||
# MAGIC       col("product_key").isNull ||
# MAGIC       col("date_key").isNull,
# MAGIC       "MISSING_DIMENSION"
# MAGIC     ).otherwise("MATCHED")
# MAGIC   )
# MAGIC   .groupBy("dimension_status")
# MAGIC   .count()
# MAGIC   .show(false)
# MAGIC
# MAGIC revalidatedDF
# MAGIC   .filter(
# MAGIC     col("customer_key").isNull ||
# MAGIC     col("product_key").isNull ||
# MAGIC     col("date_key").isNull
# MAGIC   )
# MAGIC   .select("order_id", "customer_id", "product_id", "parsed_order_date")
# MAGIC   .show(false)
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %scala
# MAGIC
# MAGIC val goldFactDF = spark.table(
# MAGIC   "adlsdatabricks2_workspace.gold.fact_order"
# MAGIC )
# MAGIC
# MAGIC println("FACT ORDER SCHEMA")
# MAGIC goldFactDF.printSchema()
# MAGIC
# MAGIC println("CURRENT FACT TABLE COUNT")
# MAGIC println(goldFactDF.count())
# MAGIC
# MAGIC println("MAXIMUM SALES KEY")
# MAGIC goldFactDF
# MAGIC   .agg(max("sales_key").as("max_sales_key"))
# MAGIC   .show(false)
# MAGIC
# MAGIC println("INCOMING ORDERS ALREADY IN GOLD")
# MAGIC goldFactDF
# MAGIC   .join(
# MAGIC     revalidatedDF.select("order_id").distinct(),
# MAGIC     Seq("order_id"),
# MAGIC     "inner"
# MAGIC   )
# MAGIC   .select("order_id", "sales_key", "last_updated")
# MAGIC   .orderBy("order_id")
# MAGIC   .show(false)
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %scala
# MAGIC
# MAGIC val goldOrderKeys = goldFactDF
# MAGIC   .select("order_id", "sales_key")
# MAGIC
# MAGIC val goldMergeCheckDF = revalidatedDF
# MAGIC   .join(goldOrderKeys, Seq("order_id"), "left")
# MAGIC   .withColumn(
# MAGIC     "gold_operation",
# MAGIC     when(col("sales_key").isNull, "INSERT")
# MAGIC       .otherwise("UPDATE")
# MAGIC   )
# MAGIC
# MAGIC println("GOLD MERGE OPERATION COUNTS")
# MAGIC
# MAGIC goldMergeCheckDF
# MAGIC   .groupBy("gold_operation")
# MAGIC   .count()
# MAGIC   .show(false)
# MAGIC
# MAGIC println("ORDERS TO INSERT INTO GOLD")
# MAGIC
# MAGIC goldMergeCheckDF
# MAGIC   .filter(col("gold_operation") === "INSERT")
# MAGIC   .select("order_id", "operation", "customer_key", "product_key", "date_key")
# MAGIC   .orderBy("order_id")
# MAGIC   .show(false)
# MAGIC
# MAGIC println("ORDERS TO UPDATE IN GOLD")
# MAGIC
# MAGIC goldMergeCheckDF
# MAGIC   .filter(col("gold_operation") === "UPDATE")
# MAGIC   .select("order_id", "operation", "sales_key")
# MAGIC   .orderBy("order_id")
# MAGIC   .show(false)
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %scala
# MAGIC
# MAGIC import org.apache.spark.sql.expressions.Window
# MAGIC import org.apache.spark.sql.functions._
# MAGIC
# MAGIC // Remove duplicate incoming order IDs, retaining the latest record
# MAGIC val dedupWindow = Window
# MAGIC   .partitionBy("order_id")
# MAGIC   .orderBy(col("incoming_last_updated").desc)
# MAGIC
# MAGIC val deduplicatedDF = goldMergeCheckDF
# MAGIC   .withColumn("row_num", row_number().over(dedupWindow))
# MAGIC   .filter(col("row_num") === 1)
# MAGIC   .drop("row_num")
# MAGIC
# MAGIC // Get current maximum Gold sales key
# MAGIC val currentMaxSalesKey = goldFactDF
# MAGIC   .agg(max("sales_key").as("max_key"))
# MAGIC   .first()
# MAGIC   .getInt(0)
# MAGIC
# MAGIC // Assign new keys only to records missing from Gold
# MAGIC val keyWindow = Window.orderBy(
# MAGIC   col("sales_key").asc_nulls_first,
# MAGIC   col("order_id")
# MAGIC )
# MAGIC
# MAGIC val mergeSourceDF = deduplicatedDF
# MAGIC   .withColumn("key_rank", row_number().over(keyWindow))
# MAGIC   .withColumn(
# MAGIC     "final_sales_key",
# MAGIC     when(
# MAGIC       col("sales_key").isNull,
# MAGIC       lit(currentMaxSalesKey) + col("key_rank")
# MAGIC     ).otherwise(col("sales_key"))
# MAGIC   )
# MAGIC   .select(
# MAGIC     col("final_sales_key").cast("int").as("sales_key"),
# MAGIC     col("order_id"),
# MAGIC     col("customer_key"),
# MAGIC     col("product_key"),
# MAGIC     col("date_key"),
# MAGIC     col("quantity"),
# MAGIC     col("order_status"),
# MAGIC     lit(null).cast("string").as("sales_channel"),
# MAGIC     lit(null).cast("decimal(10,2)").as("discount_amount"),
# MAGIC     col("incoming_last_updated").as("last_updated")
# MAGIC   )
# MAGIC
# MAGIC println("MERGE SOURCE COUNT")
# MAGIC println(mergeSourceDF.count())
# MAGIC
# MAGIC println("PREPARED GOLD RECORDS")
# MAGIC mergeSourceDF
# MAGIC   .orderBy("sales_key")
# MAGIC   .show(20, false)
# MAGIC
# MAGIC println("DUPLICATE ORDER CHECK")
# MAGIC mergeSourceDF
# MAGIC   .groupBy("order_id")
# MAGIC   .count()
# MAGIC   .filter(col("count") > 1)
# MAGIC   .show(false)
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %scala
# MAGIC
# MAGIC import io.delta.tables.DeltaTable
# MAGIC
# MAGIC val goldFactTable = DeltaTable.forName(
# MAGIC   "adlsdatabricks2_workspace.gold.fact_order"
# MAGIC )
# MAGIC
# MAGIC goldFactTable
# MAGIC   .as("target")
# MAGIC   .merge(
# MAGIC     mergeSourceDF.as("source"),
# MAGIC     "target.order_id = source.order_id"
# MAGIC   )
# MAGIC   .whenMatched("source.last_updated > target.last_updated")
# MAGIC   .updateExpr(
# MAGIC     Map(
# MAGIC       "customer_key" -> "source.customer_key",
# MAGIC       "product_key" -> "source.product_key",
# MAGIC       "date_key" -> "source.date_key",
# MAGIC       "quantity" -> "source.quantity",
# MAGIC       "order_status" -> "source.order_status",
# MAGIC       "last_updated" -> "source.last_updated"
# MAGIC     )
# MAGIC   )
# MAGIC   .whenNotMatched()
# MAGIC   .insertExpr(
# MAGIC     Map(
# MAGIC       "sales_key" -> "source.sales_key",
# MAGIC       "order_id" -> "source.order_id",
# MAGIC       "customer_key" -> "source.customer_key",
# MAGIC       "product_key" -> "source.product_key",
# MAGIC       "date_key" -> "source.date_key",
# MAGIC       "quantity" -> "source.quantity",
# MAGIC       "order_status" -> "source.order_status",
# MAGIC       "sales_channel" -> "source.sales_channel",
# MAGIC       "discount_amount" -> "source.discount_amount",
# MAGIC       "last_updated" -> "source.last_updated"
# MAGIC     )
# MAGIC   )
# MAGIC   .execute()
# MAGIC
# MAGIC println("Incremental MERGE into Gold fact_order completed.")
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %scala
# MAGIC
# MAGIC val finalGoldFactDF = spark.table(
# MAGIC   "adlsdatabricks2_workspace.gold.fact_order"
# MAGIC )
# MAGIC
# MAGIC println("FINAL GOLD FACT COUNT")
# MAGIC println(finalGoldFactDF.count())
# MAGIC
# MAGIC println("NEWLY INSERTED ORDERS")
# MAGIC finalGoldFactDF
# MAGIC   .filter(col("order_id").isin(
# MAGIC     "ORD000013", "ORD000015",
# MAGIC     "ORD000151", "ORD000152", "ORD000153",
# MAGIC     "ORD000154", "ORD000155", "ORD000156",
# MAGIC     "ORD000157", "ORD000158"
# MAGIC   ))
# MAGIC   .select(
# MAGIC     "sales_key",
# MAGIC     "order_id",
# MAGIC     "customer_key",
# MAGIC     "product_key",
# MAGIC     "date_key",
# MAGIC     "quantity",
# MAGIC     "order_status",
# MAGIC     "last_updated"
# MAGIC   )
# MAGIC   .orderBy("sales_key")
# MAGIC   .show(false)
# MAGIC
# MAGIC println("UPDATED EXISTING ORDERS")
# MAGIC finalGoldFactDF
# MAGIC   .filter(col("order_id").isin(
# MAGIC     "ORD000011", "ORD000012", "ORD000014",
# MAGIC     "ORD000016", "ORD000021"
# MAGIC   ))
# MAGIC   .select("order_id", "sales_key", "last_updated")
# MAGIC   .orderBy("sales_key")
# MAGIC   .show(false)
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %scala
# MAGIC
# MAGIC val goldSchema = "adlsdatabricks2_workspace.gold"
# MAGIC
# MAGIC val dashboardViews = Seq(
# MAGIC   "vw_monthly_order_trend",
# MAGIC   "vw_order_status_summary",
# MAGIC   "vw_product_category_summary",
# MAGIC   "vw_sales_channel_summary",
# MAGIC   "vw_top_customers"
# MAGIC )
# MAGIC
# MAGIC dashboardViews.foreach { viewName =>
# MAGIC   println(s"\n========== $viewName ==========")
# MAGIC
# MAGIC   val viewDF = spark.table(s"$goldSchema.$viewName")
# MAGIC
# MAGIC   println(s"Row count: ${viewDF.count()}")
# MAGIC
# MAGIC   viewDF.show(20, false)
# MAGIC }
# MAGIC