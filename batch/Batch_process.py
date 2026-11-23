# Databricks notebook source
# MAGIC %scala
# MAGIC val basePath =
# MAGIC   "abfss://adlsdatabrics2c@adlsdatabricks2.dfs.core.windows.net/nexus/batch/bronze/"
# MAGIC
# MAGIC val folders = Seq(
# MAGIC   "batch/bronze/customers",
# MAGIC   "batch/bronze/products",
# MAGIC   "batch/bronze/orders",
# MAGIC   "batch/silver/customers",
# MAGIC   "batch/silver/products",
# MAGIC   "batch/silver/orders",
# MAGIC   "batch/rejected/customers",
# MAGIC   "batch/rejected/products",
# MAGIC   "batch/rejected/orders",
# MAGIC   "batch/gold/dim_customer",
# MAGIC   "batch/gold/dim_product",
# MAGIC   "batch/gold/dim_date",
# MAGIC   "batch/gold/fact_orders",
# MAGIC   "streaming",
# MAGIC   "checkpoints",
# MAGIC   "logs"
# MAGIC )
# MAGIC
# MAGIC folders.foreach { folder =>
# MAGIC   dbutils.fs.mkdirs(basePath + folder)
# MAGIC }
# MAGIC
# MAGIC println("PROJECT NEXUS folder structure created!")

# COMMAND ----------

# MAGIC %scala
# MAGIC val storagePath =
# MAGIC   "abfss://adlsdatabrics2c@adlsdatabricks2.dfs.core.windows.net/nexus/"
# MAGIC
# MAGIC display(dbutils.fs.ls(storagePath))

# COMMAND ----------

# MAGIC %scala
# MAGIC val basePath =
# MAGIC   "abfss://adlsdatabrics2c@adlsdatabricks2.dfs.core.windows.net/nexus/batch/bronze/"
# MAGIC
# MAGIC display(dbutils.fs.ls(basePath + "customers"))
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC display(dbutils.fs.ls(basePath + "products"))
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC display(dbutils.fs.ls(basePath + "orders"))

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.types._
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import org.apache.spark.sql.DataFrame

# COMMAND ----------

# MAGIC %scala
# MAGIC val basePath =
# MAGIC   "abfss://adlsdatabrics2c@adlsdatabricks2.dfs.core.windows.net/nexus/batch/"
# MAGIC
# MAGIC val customersSource = basePath + "bronze/customers/customers.csv"
# MAGIC val productsSource  = basePath + "bronze/products/products.csv"
# MAGIC val ordersSource    = basePath + "bronze/orders/orders.csv"
# MAGIC
# MAGIC val customersTarget = basePath + "bronze_delta/customers"
# MAGIC val productsTarget  = basePath + "bronze_delta/products"
# MAGIC val ordersTarget    = basePath + "bronze_delta/orders"

# COMMAND ----------

# MAGIC %scala
# MAGIC val customerSchema = StructType(Seq(
# MAGIC   StructField("customer_id", StringType, true),
# MAGIC   StructField("customer_name", StringType, true),
# MAGIC   StructField("email", StringType, true),
# MAGIC   StructField("city", StringType, true),
# MAGIC   StructField("signup_date", StringType, true)
# MAGIC ))
# MAGIC
# MAGIC val productSchema = StructType(Seq(
# MAGIC   StructField("product_id", StringType, true),
# MAGIC   StructField("product_name", StringType, true),
# MAGIC   StructField("category", StringType, true),
# MAGIC   StructField("unit_price", StringType, true),
# MAGIC   StructField("active", StringType, true)
# MAGIC ))
# MAGIC
# MAGIC val orderSchema = StructType(Seq(
# MAGIC   StructField("order_id", StringType, true),
# MAGIC   StructField("customer_id", StringType, true),
# MAGIC   StructField("product_id", StringType, true),
# MAGIC   StructField("quantity", StringType, true),
# MAGIC   StructField("order_date", StringType, true),
# MAGIC   StructField("order_status", StringType, true),
# MAGIC   StructField("last_updated", StringType, true)
# MAGIC ))

# COMMAND ----------

# MAGIC %scala
# MAGIC def readSource(
# MAGIC     path: String,
# MAGIC     schema: StructType
# MAGIC ): DataFrame = {
# MAGIC
# MAGIC   spark.read
# MAGIC     .format("csv")
# MAGIC     .option("header", "true")
# MAGIC     .option("mode", "PERMISSIVE")
# MAGIC     .schema(schema)
# MAGIC     .load(path)
# MAGIC     .withColumn("_source_file", input_file_name())
# MAGIC     .withColumn("_ingestion_timestamp", current_timestamp())
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC val customersDF =
# MAGIC   readSource(customersSource, customerSchema)
# MAGIC
# MAGIC val productsDF =
# MAGIC   readSource(productsSource, productSchema)
# MAGIC
# MAGIC val ordersDF =
# MAGIC   readSource(ordersSource, orderSchema)

# COMMAND ----------

# MAGIC %scala
# MAGIC customersDF.show(10, false)
# MAGIC
# MAGIC productsDF.show(10, false)
# MAGIC
# MAGIC ordersDF.show(10, false)

# COMMAND ----------

# MAGIC %scala
# MAGIC customersDF.write
# MAGIC   .format("delta")
# MAGIC   .mode("overwrite")
# MAGIC   .save(customersTarget)
# MAGIC
# MAGIC productsDF.write
# MAGIC   .format("delta")
# MAGIC   .mode("overwrite")
# MAGIC   .save(productsTarget)
# MAGIC
# MAGIC ordersDF.write
# MAGIC   .format("delta")
# MAGIC   .mode("overwrite")
# MAGIC   .save(ordersTarget)
# MAGIC
# MAGIC println("Bronze Delta ingestion completed!")

# COMMAND ----------

# MAGIC %scala
# MAGIC val bronzeCustomers =
# MAGIC   spark.read.format("delta").load(customersTarget)
# MAGIC
# MAGIC val bronzeProducts =
# MAGIC   spark.read.format("delta").load(productsTarget)
# MAGIC
# MAGIC val bronzeOrders =
# MAGIC   spark.read.format("delta").load(ordersTarget)
# MAGIC
# MAGIC println("Customers: " + bronzeCustomers.count())
# MAGIC println("Products: " + bronzeProducts.count())
# MAGIC println("Orders: " + bronzeOrders.count())

# COMMAND ----------

# MAGIC %scala
# MAGIC display(bronzeCustomers)

# COMMAND ----------

# MAGIC %scala
# MAGIC display(bronzeProducts)

# COMMAND ----------

# MAGIC %scala
# MAGIC display(bronzeOrders)

# COMMAND ----------

# MAGIC %md
# MAGIC Customer Silver transformation

# COMMAND ----------

# MAGIC %md
# MAGIC Read Bronze customers

# COMMAND ----------

# MAGIC %scala
# MAGIC val bronzeCustomers =
# MAGIC   spark.read
# MAGIC     .format("delta")
# MAGIC     .load(basePath + "bronze_delta/customers")
# MAGIC
# MAGIC display(bronzeCustomers)

# COMMAND ----------

# MAGIC %md
# MAGIC Clean and standardize columns

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("""
# MAGIC   SELECT
# MAGIC     try_to_timestamp('15/03/2024', 'dd/MM/yyyy') AS parsed_date
# MAGIC """).show(false)

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._
# MAGIC
# MAGIC val cleanedCustomersV2 = bronzeCustomers
# MAGIC   .withColumn("customer_id", upper(trim(col("customer_id"))))
# MAGIC   .withColumn("customer_name", trim(col("customer_name")))
# MAGIC   .withColumn("email", lower(trim(col("email"))))
# MAGIC   .withColumn("city", initcap(lower(trim(col("city")))))
# MAGIC   .withColumn(
# MAGIC     "signup_date_parsed",
# MAGIC     coalesce(
# MAGIC       to_date(try_to_timestamp(col("signup_date"), lit("yyyy-MM-dd"))),
# MAGIC       to_date(try_to_timestamp(col("signup_date"), lit("dd/MM/yyyy")))
# MAGIC     )
# MAGIC   )

# COMMAND ----------

# MAGIC %md
# MAGIC Apply validation rules

# COMMAND ----------

# MAGIC %scala
# MAGIC val validatedCustomersV2 = cleanedCustomersV2
# MAGIC   .withColumn(
# MAGIC     "rejection_reason",
# MAGIC     concat_ws(
# MAGIC       "; ",
# MAGIC       when(
# MAGIC         col("customer_id").isNull || col("customer_id") === "",
# MAGIC         lit("Missing customer ID")
# MAGIC       ),
# MAGIC       when(
# MAGIC         col("customer_name").isNull || col("customer_name") === "",
# MAGIC         lit("Missing customer name")
# MAGIC       ),
# MAGIC       when(
# MAGIC         col("city").isNull || col("city") === "",
# MAGIC         lit("Missing city")
# MAGIC       ),
# MAGIC       when(
# MAGIC         col("signup_date").isNull || trim(col("signup_date")) === "",
# MAGIC         lit("Missing signup date")
# MAGIC       ),
# MAGIC       when(
# MAGIC         col("signup_date").isNotNull &&
# MAGIC         trim(col("signup_date")) =!= "" &&
# MAGIC         col("signup_date_parsed").isNull,
# MAGIC         lit("Invalid signup date")
# MAGIC       ),
# MAGIC       when(
# MAGIC         col("signup_date_parsed") > current_date(),
# MAGIC         lit("Future signup date")
# MAGIC       ),
# MAGIC       when(
# MAGIC         col("email").isNotNull &&
# MAGIC         col("email") =!= "" &&
# MAGIC         !col("email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"),
# MAGIC         lit("Invalid email")
# MAGIC       )
# MAGIC     )
# MAGIC   )
# MAGIC
# MAGIC println("Customer validation completed")

# COMMAND ----------

# MAGIC %md
# MAGIC Separate valid and rejected records

# COMMAND ----------

# MAGIC %scala
# MAGIC val customerRejectedV2 = validatedCustomersV2
# MAGIC   .filter(col("rejection_reason") =!= "")
# MAGIC
# MAGIC val customerValidV2 = validatedCustomersV2
# MAGIC   .filter(col("rejection_reason") === "")

# COMMAND ----------

# MAGIC %scala
# MAGIC cleanedCustomersV2
# MAGIC   .filter(col("signup_date") === "15/03/2024")
# MAGIC   .select("customer_id", "signup_date", "signup_date_parsed")
# MAGIC   .show(false)

# COMMAND ----------

# MAGIC %scala
# MAGIC validatedCustomersV2
# MAGIC   .groupBy("rejection_reason")
# MAGIC   .count()
# MAGIC   .show(false)

# COMMAND ----------

# MAGIC %md
# MAGIC Inspect rejected records

# COMMAND ----------

# MAGIC %scala
# MAGIC customerRejectedV2.select(
# MAGIC   "customer_id",
# MAGIC   "customer_name",
# MAGIC   "email",
# MAGIC   "city",
# MAGIC   "signup_date",
# MAGIC   "rejection_reason"
# MAGIC ).show(50, false)

# COMMAND ----------

# MAGIC %scala
# MAGIC println("Bronze customers: " + bronzeCustomers.count())
# MAGIC println("Valid customers: " + customerValidV2.count())
# MAGIC println("Rejected customers: " + customerRejectedV2.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Customer deduplication

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.expressions.Window
# MAGIC
# MAGIC val customerWindowV2 = Window
# MAGIC   .partitionBy("customer_id")
# MAGIC   .orderBy(
# MAGIC     col("signup_date_parsed").desc_nulls_last,
# MAGIC     col("customer_name").asc,
# MAGIC     col("email").asc,
# MAGIC     col("city").asc
# MAGIC   )
# MAGIC
# MAGIC val rankedCustomersV2 = customerValidV2
# MAGIC   .withColumn("row_num", row_number().over(customerWindowV2))
# MAGIC
# MAGIC val silverCustomersV2 = rankedCustomersV2
# MAGIC   .filter(col("row_num") === 1)
# MAGIC   .drop("row_num", "rejection_reason")
# MAGIC
# MAGIC val duplicateCustomersV2 = rankedCustomersV2
# MAGIC   .filter(col("row_num") > 1)
# MAGIC   .withColumn("rejection_reason", lit("Duplicate customer ID"))
# MAGIC   .drop("row_num")

# COMMAND ----------

# MAGIC %scala
# MAGIC println("Valid before deduplication: " + customerValidV2.count())
# MAGIC println("Silver after deduplication: " + silverCustomersV2.count())
# MAGIC println("Duplicate records: " + duplicateCustomersV2.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Combine all rejected customers

# COMMAND ----------

# MAGIC %scala
# MAGIC val allRejectedCustomersV2 = customerRejectedV2
# MAGIC   .unionByName(duplicateCustomersV2)
# MAGIC
# MAGIC println("Total rejected customers: " + allRejectedCustomersV2.count())
# MAGIC
# MAGIC allRejectedCustomersV2
# MAGIC   .select(
# MAGIC     "customer_id",
# MAGIC     "customer_name",
# MAGIC     "rejection_reason"
# MAGIC   )
# MAGIC   .show(false)

# COMMAND ----------

# MAGIC %md
# MAGIC Prepare the silver DataFrame

# COMMAND ----------

# MAGIC %scala
# MAGIC val silverCustomersFinal = silverCustomersV2.select(
# MAGIC   col("customer_id"),
# MAGIC   col("customer_name"),
# MAGIC   col("email"),
# MAGIC   col("city"),
# MAGIC   col("signup_date_parsed").as("signup_date"),
# MAGIC   col("_source_file"),
# MAGIC   col("_ingestion_timestamp")
# MAGIC )
# MAGIC
# MAGIC silverCustomersFinal.printSchema()
# MAGIC
# MAGIC println("Final Silver count: " + silverCustomersFinal.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Write Silver and Rejected Delta datasets to ADLS Gen2

# COMMAND ----------

# MAGIC %scala
# MAGIC val silverCustomerPath =
# MAGIC   basePath + "silver/customers"
# MAGIC
# MAGIC silverCustomersFinal.write
# MAGIC   .format("delta")
# MAGIC   .mode("overwrite")
# MAGIC   .save(silverCustomerPath)
# MAGIC
# MAGIC println("Silver Customers Delta write successful")

# COMMAND ----------

# MAGIC %scala
# MAGIC val verifySilverCustomers = spark.read
# MAGIC   .format("delta")
# MAGIC   .load(silverCustomerPath)
# MAGIC
# MAGIC println("Silver customer records: " + verifySilverCustomers.count())
# MAGIC
# MAGIC verifySilverCustomers.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Write rejected customers

# COMMAND ----------

# MAGIC %scala
# MAGIC val rejectedCustomerPath =
# MAGIC   basePath + "rejected/customers"
# MAGIC
# MAGIC allRejectedCustomersV2.write
# MAGIC   .format("delta")
# MAGIC   .mode("overwrite")
# MAGIC   .save(rejectedCustomerPath)
# MAGIC
# MAGIC println("Rejected Customers Delta write successful")

# COMMAND ----------

# MAGIC %md
# MAGIC Verify rejected records

# COMMAND ----------

# MAGIC %scala
# MAGIC val verifyRejectedCustomers = spark.read
# MAGIC   .format("delta")
# MAGIC   .load(rejectedCustomerPath)
# MAGIC
# MAGIC println("Rejected customer records: " + verifyRejectedCustomers.count())
# MAGIC
# MAGIC verifyRejectedCustomers
# MAGIC   .select("customer_id", "customer_name", "rejection_reason")
# MAGIC   .show(false)

# COMMAND ----------

# MAGIC %md
# MAGIC Read Bronze Products Delta

# COMMAND ----------

# MAGIC %scala
# MAGIC val bronzeProductsV2 = spark.read
# MAGIC   .format("delta")
# MAGIC   .load(basePath + "bronze_delta/products")
# MAGIC
# MAGIC println("Total Bronze Products: " + bronzeProductsV2.count())
# MAGIC
# MAGIC bronzeProductsV2.printSchema()
# MAGIC
# MAGIC bronzeProductsV2.show(false)

# COMMAND ----------

# MAGIC %md
# MAGIC Inspect all 27 records

# COMMAND ----------

# MAGIC %scala
# MAGIC bronzeProductsV2
# MAGIC   .orderBy("product_id")
# MAGIC   .show(100, false)

# COMMAND ----------

# MAGIC %md
# MAGIC Check duplicate product IDs

# COMMAND ----------

# MAGIC %scala
# MAGIC bronzeProductsV2
# MAGIC   .groupBy("product_id")
# MAGIC   .count()
# MAGIC   .filter(col("count") > 1)
# MAGIC   .show(false)

# COMMAND ----------

# MAGIC %md
# MAGIC inspect all 27 product records

# COMMAND ----------

# MAGIC %scala
# MAGIC bronzeProductsV2
# MAGIC   .orderBy("product_id")
# MAGIC   .show(100, false)

# COMMAND ----------

# MAGIC %md
# MAGIC Clean and standardize product data

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._
# MAGIC
# MAGIC val cleanedProductsV2 = bronzeProductsV2
# MAGIC   .withColumn("product_id", upper(trim(col("product_id"))))
# MAGIC   .withColumn("product_name", trim(col("product_name")))
# MAGIC   .withColumn("category", initcap(lower(trim(col("category")))))
# MAGIC   .withColumn(
# MAGIC     "unit_price_parsed",
# MAGIC     expr("try_cast(trim(unit_price) as decimal(10,2))")
# MAGIC   )
# MAGIC   .withColumn("active", upper(trim(col("active"))))
# MAGIC
# MAGIC cleanedProductsV2.show(100, false)
# MAGIC cleanedProductsV2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Validate product records

# COMMAND ----------

# MAGIC %scala
# MAGIC val validatedProductsV2 = cleanedProductsV2
# MAGIC   .withColumn(
# MAGIC     "rejection_reason",
# MAGIC     concat_ws(
# MAGIC       "; ",
# MAGIC       when(
# MAGIC         col("product_id").isNull || col("product_id") === "",
# MAGIC         lit("Missing product ID")
# MAGIC       ),
# MAGIC       when(
# MAGIC         col("product_name").isNull || col("product_name") === "",
# MAGIC         lit("Missing product name")
# MAGIC       ),
# MAGIC       when(
# MAGIC         col("category").isNull || col("category") === "",
# MAGIC         lit("Missing category")
# MAGIC       ),
# MAGIC       when(
# MAGIC         col("unit_price").isNull || trim(col("unit_price")) === "",
# MAGIC         lit("Missing unit price")
# MAGIC       ),
# MAGIC       when(
# MAGIC         col("unit_price").isNotNull &&
# MAGIC         trim(col("unit_price")) =!= "" &&
# MAGIC         col("unit_price_parsed").isNull,
# MAGIC         lit("Invalid unit price")
# MAGIC       ),
# MAGIC       when(
# MAGIC         col("unit_price_parsed") <= 0,
# MAGIC         lit("Unit price must be greater than zero")
# MAGIC       ),
# MAGIC       when(
# MAGIC         col("active").isNull || !col("active").isin("Y", "N"),
# MAGIC         lit("Invalid active flag")
# MAGIC       )
# MAGIC     )
# MAGIC   )
# MAGIC
# MAGIC val productRejectedV2 = validatedProductsV2
# MAGIC   .filter(col("rejection_reason") =!= "")
# MAGIC
# MAGIC val productValidV2 = validatedProductsV2
# MAGIC   .filter(col("rejection_reason") === "")
# MAGIC
# MAGIC println("Total Bronze: " + bronzeProductsV2.count())
# MAGIC println("Validation Valid: " + productValidV2.count())
# MAGIC println("Validation Rejected: " + productRejectedV2.count())
# MAGIC
# MAGIC productRejectedV2
# MAGIC   .select("product_id", "product_name", "unit_price", "rejection_reason")
# MAGIC   .show(false)

# COMMAND ----------

# MAGIC %md
# MAGIC Detect duplicate and conflicting product IDs

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.expressions.Window
# MAGIC
# MAGIC // Identify product IDs that have multiple records
# MAGIC val duplicateProductIds = productValidV2
# MAGIC   .groupBy("product_id")
# MAGIC   .count()
# MAGIC   .filter(col("count") > 1)
# MAGIC   .select("product_id")
# MAGIC
# MAGIC // Identify IDs whose records have different business values
# MAGIC val conflictingProductIds = productValidV2
# MAGIC   .groupBy("product_id")
# MAGIC   .agg(
# MAGIC     countDistinct(
# MAGIC       struct(
# MAGIC         col("product_name"),
# MAGIC         col("category"),
# MAGIC         col("unit_price_parsed"),
# MAGIC         col("active")
# MAGIC       )
# MAGIC     ).as("distinct_versions")
# MAGIC   )
# MAGIC   .filter(col("distinct_versions") > 1)
# MAGIC   .select("product_id")
# MAGIC
# MAGIC println("Duplicate product IDs:")
# MAGIC duplicateProductIds.show(false)
# MAGIC
# MAGIC println("Conflicting product IDs:")
# MAGIC conflictingProductIds.show(false)

# COMMAND ----------

# MAGIC %md
# MAGIC Separate exact duplicates and conflicting records

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // Remove all conflicting product IDs from the valid dataset
# MAGIC val productsWithoutConflicts = productValidV2
# MAGIC   .join(conflictingProductIds, Seq("product_id"), "left_anti")
# MAGIC
# MAGIC // Rank remaining records by product ID
# MAGIC val productWindow = Window
# MAGIC   .partitionBy("product_id")
# MAGIC   .orderBy(
# MAGIC     col("product_name").asc,
# MAGIC     col("category").asc,
# MAGIC     col("unit_price_parsed").asc,
# MAGIC     col("active").asc
# MAGIC   )
# MAGIC
# MAGIC val rankedProducts = productsWithoutConflicts
# MAGIC   .withColumn("row_num", row_number().over(productWindow))
# MAGIC
# MAGIC // Keep one record per product ID
# MAGIC val silverProductsV2 = rankedProducts
# MAGIC   .filter(col("row_num") === 1)
# MAGIC   .drop("row_num", "rejection_reason")
# MAGIC
# MAGIC // Capture extra exact duplicate records
# MAGIC val duplicateProductsRejected = rankedProducts
# MAGIC   .filter(col("row_num") > 1)
# MAGIC   .withColumn("rejection_reason", lit("Duplicate product ID"))
# MAGIC   .drop("row_num")
# MAGIC
# MAGIC // Capture both records of conflicting product IDs
# MAGIC val conflictingProductsRejected = productValidV2
# MAGIC   .join(conflictingProductIds, Seq("product_id"), "inner")
# MAGIC   .withColumn(
# MAGIC     "rejection_reason",
# MAGIC     lit("Conflicting duplicate product ID")
# MAGIC   )
# MAGIC
# MAGIC println("Silver products: " + silverProductsV2.count())
# MAGIC println("Exact duplicate rejected: " + duplicateProductsRejected.count())
# MAGIC println("Conflicting records rejected: " + conflictingProductsRejected.count())
# MAGIC
# MAGIC duplicateProductsRejected
# MAGIC   .select("product_id", "product_name", "unit_price", "rejection_reason")
# MAGIC   .show(false)
# MAGIC
# MAGIC conflictingProductsRejected
# MAGIC   .select("product_id", "product_name", "unit_price", "rejection_reason")
# MAGIC   .show(false)

# COMMAND ----------

# MAGIC %md
# MAGIC Combine all rejected product records

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val allRejectedProductsV2 = productRejectedV2
# MAGIC   .unionByName(duplicateProductsRejected)
# MAGIC   .unionByName(conflictingProductsRejected)
# MAGIC
# MAGIC println("Total rejected products: " + allRejectedProductsV2.count())
# MAGIC
# MAGIC allRejectedProductsV2
# MAGIC   .select(
# MAGIC     "product_id",
# MAGIC     "product_name",
# MAGIC     "category",
# MAGIC     "unit_price",
# MAGIC     "rejection_reason"
# MAGIC   )
# MAGIC   .orderBy("product_id")
# MAGIC   .show(100, false)

# COMMAND ----------

# MAGIC %md
# MAGIC Prepare final Silver product schema

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val silverProductsFinal = silverProductsV2.select(
# MAGIC   col("product_id"),
# MAGIC   col("product_name"),
# MAGIC   col("category"),
# MAGIC   col("unit_price_parsed").as("unit_price"),
# MAGIC   col("active"),
# MAGIC   col("_source_file"),
# MAGIC   col("_ingestion_timestamp")
# MAGIC )
# MAGIC
# MAGIC println("Final Silver Product Count: " + silverProductsFinal.count())
# MAGIC
# MAGIC silverProductsFinal.printSchema()
# MAGIC
# MAGIC silverProductsFinal
# MAGIC   .orderBy("product_id")
# MAGIC   .show(100, false)

# COMMAND ----------

# MAGIC %md
# MAGIC Save Silver and Rejected products to ADLS Gen2

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // Define ADLS Gen2 paths
# MAGIC val silverProductPath = basePath + "silver/products"
# MAGIC val rejectedProductPath = basePath + "rejected/products"
# MAGIC
# MAGIC // Write Silver Products
# MAGIC silverProductsFinal.write
# MAGIC   .format("delta")
# MAGIC   .mode("overwrite")
# MAGIC   .save(silverProductPath)
# MAGIC
# MAGIC println("Silver Products written successfully")
# MAGIC
# MAGIC // Write Rejected Products
# MAGIC allRejectedProductsV2.write
# MAGIC   .format("delta")
# MAGIC   .mode("overwrite")
# MAGIC   .save(rejectedProductPath)
# MAGIC
# MAGIC println("Rejected Products written successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC Read back and verify persisted data

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val silverProductsCheck = spark.read
# MAGIC   .format("delta")
# MAGIC   .load(silverProductPath)
# MAGIC
# MAGIC val rejectedProductsCheck = spark.read
# MAGIC   .format("delta")
# MAGIC   .load(rejectedProductPath)
# MAGIC
# MAGIC println("========== PRODUCTS PIPELINE SUMMARY ==========")
# MAGIC println("Bronze Products   : " + bronzeProductsV2.count())
# MAGIC println("Silver Products   : " + silverProductsCheck.count())
# MAGIC println("Rejected Products : " + rejectedProductsCheck.count())
# MAGIC
# MAGIC println("========== SILVER VERIFICATION ==========")
# MAGIC silverProductsCheck.printSchema()
# MAGIC silverProductsCheck.orderBy("product_id").show(100, false)
# MAGIC
# MAGIC println("========== REJECTED VERIFICATION ==========")
# MAGIC rejectedProductsCheck
# MAGIC   .select("product_id", "rejection_reason")
# MAGIC   .orderBy("product_id")
# MAGIC   .show(100, false)

# COMMAND ----------

# MAGIC %md
# MAGIC Orders Bronze-to-Silver pipeline

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val bronzeOrdersV2 = spark.read
# MAGIC   .format("delta")
# MAGIC   .load(basePath + "bronze_delta/orders")
# MAGIC
# MAGIC println("Total Bronze Orders: " + bronzeOrdersV2.count())
# MAGIC
# MAGIC bronzeOrdersV2.printSchema()
# MAGIC
# MAGIC bronzeOrdersV2
# MAGIC   .orderBy("order_id")
# MAGIC   .show(200, false)

# COMMAND ----------

# MAGIC %md
# MAGIC Clean and parse Orders data

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._
# MAGIC
# MAGIC val cleanedOrdersV1 = bronzeOrdersV2
# MAGIC   .withColumn("order_id", upper(trim(col("order_id"))))
# MAGIC   .withColumn("customer_id", upper(trim(col("customer_id"))))
# MAGIC   .withColumn("product_id", upper(trim(col("product_id"))))
# MAGIC   .withColumn("order_status", upper(trim(col("order_status"))))
# MAGIC   .withColumn(
# MAGIC     "quantity_parsed",
# MAGIC     expr("try_cast(trim(quantity) AS INT)")
# MAGIC   )
# MAGIC   .withColumn(
# MAGIC     "order_date_parsed",
# MAGIC     coalesce(
# MAGIC       to_date(try_to_timestamp(col("order_date"), lit("yyyy-MM-dd"))),
# MAGIC       to_date(try_to_timestamp(col("order_date"), lit("dd-MM-yyyy")))
# MAGIC     )
# MAGIC   )
# MAGIC   .withColumn(
# MAGIC     "last_updated_parsed",
# MAGIC     try_to_timestamp(col("last_updated"), lit("yyyy-MM-dd HH:mm:ss"))
# MAGIC   )
# MAGIC
# MAGIC cleanedOrdersV1.select(
# MAGIC   "order_id",
# MAGIC   "customer_id",
# MAGIC   "product_id",
# MAGIC   "quantity",
# MAGIC   "quantity_parsed",
# MAGIC   "order_date",
# MAGIC   "order_date_parsed",
# MAGIC   "order_status",
# MAGIC   "last_updated_parsed"
# MAGIC ).show(30, false)

# COMMAND ----------

# MAGIC %md
# MAGIC Validate Orders

# COMMAND ----------

# MAGIC %scala
# MAGIC val silverCustomersRef = spark.read
# MAGIC   .format("delta")
# MAGIC   .load(basePath + "silver/customers")
# MAGIC   .select("customer_id")
# MAGIC   .distinct()
# MAGIC
# MAGIC val silverProductsRef = spark.read
# MAGIC   .format("delta")
# MAGIC   .load(basePath + "silver/products")
# MAGIC   .select("product_id")
# MAGIC   .distinct()
# MAGIC
# MAGIC println("Valid Customer IDs: " + silverCustomersRef.count())
# MAGIC println("Valid Product IDs : " + silverProductsRef.count())
# MAGIC
# MAGIC println("Customer reference sample:")
# MAGIC silverCustomersRef.show(10, false)
# MAGIC
# MAGIC println("Product reference sample:")
# MAGIC silverProductsRef.show(10, false)

# COMMAND ----------

# MAGIC %md
# MAGIC Apply validation rules

# COMMAND ----------

# MAGIC %scala
# MAGIC val ordersWithReferences = cleanedOrdersV1
# MAGIC   .join(
# MAGIC     silverCustomersRef
# MAGIC       .withColumnRenamed("customer_id", "_valid_customer_id"),
# MAGIC     cleanedOrdersV1("customer_id") === col("_valid_customer_id"),
# MAGIC     "left"
# MAGIC   )
# MAGIC   .join(
# MAGIC     silverProductsRef
# MAGIC       .withColumnRenamed("product_id", "_valid_product_id"),
# MAGIC     cleanedOrdersV1("product_id") === col("_valid_product_id"),
# MAGIC     "left"
# MAGIC   )
# MAGIC
# MAGIC val validatedOrdersV1 = ordersWithReferences
# MAGIC   .withColumn(
# MAGIC     "rejection_reason",
# MAGIC     concat_ws(
# MAGIC       "; ",
# MAGIC
# MAGIC       when(
# MAGIC         col("order_id").isNull || col("order_id") === "",
# MAGIC         lit("Missing order ID")
# MAGIC       ),
# MAGIC
# MAGIC       when(
# MAGIC         col("order_id").isNotNull &&
# MAGIC         col("order_id") =!= "" &&
# MAGIC         !col("order_id").rlike("^ORD[0-9]{6}$"),
# MAGIC         lit("Invalid order ID format")
# MAGIC       ),
# MAGIC
# MAGIC       when(
# MAGIC         col("customer_id").isNull || col("customer_id") === "",
# MAGIC         lit("Missing customer ID")
# MAGIC       ),
# MAGIC
# MAGIC       when(
# MAGIC         col("customer_id").isNotNull &&
# MAGIC         col("customer_id") =!= "" &&
# MAGIC         col("_valid_customer_id").isNull,
# MAGIC         lit("Customer ID not found in Silver")
# MAGIC       ),
# MAGIC
# MAGIC       when(
# MAGIC         col("product_id").isNull || col("product_id") === "",
# MAGIC         lit("Missing product ID")
# MAGIC       ),
# MAGIC
# MAGIC       when(
# MAGIC         col("product_id").isNotNull &&
# MAGIC         col("product_id") =!= "" &&
# MAGIC         col("_valid_product_id").isNull,
# MAGIC         lit("Product ID not found in Silver")
# MAGIC       ),
# MAGIC
# MAGIC       when(
# MAGIC         col("quantity").isNull || trim(col("quantity")) === "",
# MAGIC         lit("Missing quantity")
# MAGIC       ),
# MAGIC
# MAGIC       when(
# MAGIC         col("quantity").isNotNull &&
# MAGIC         trim(col("quantity")) =!= "" &&
# MAGIC         col("quantity_parsed").isNull,
# MAGIC         lit("Invalid quantity")
# MAGIC       ),
# MAGIC
# MAGIC       when(
# MAGIC         col("quantity_parsed") <= 0,
# MAGIC         lit("Quantity must be greater than zero")
# MAGIC       ),
# MAGIC
# MAGIC       when(
# MAGIC         col("order_date").isNull || trim(col("order_date")) === "",
# MAGIC         lit("Missing order date")
# MAGIC       ),
# MAGIC
# MAGIC       when(
# MAGIC         col("order_date").isNotNull &&
# MAGIC         trim(col("order_date")) =!= "" &&
# MAGIC         col("order_date_parsed").isNull,
# MAGIC         lit("Invalid order date")
# MAGIC       ),
# MAGIC
# MAGIC       when(
# MAGIC         col("order_date_parsed") > current_date(),
# MAGIC         lit("Future order date")
# MAGIC       ),
# MAGIC
# MAGIC       when(
# MAGIC         col("last_updated").isNull || trim(col("last_updated")) === "",
# MAGIC         lit("Missing last updated timestamp")
# MAGIC       ),
# MAGIC
# MAGIC       when(
# MAGIC         col("last_updated").isNotNull &&
# MAGIC         trim(col("last_updated")) =!= "" &&
# MAGIC         col("last_updated_parsed").isNull,
# MAGIC         lit("Invalid last updated timestamp")
# MAGIC       ),
# MAGIC
# MAGIC       when(
# MAGIC         col("order_status").isNull || col("order_status") === "",
# MAGIC         lit("Missing order status")
# MAGIC       ),
# MAGIC
# MAGIC       when(
# MAGIC         col("order_status").isNotNull &&
# MAGIC         col("order_status") =!= "" &&
# MAGIC         !col("order_status").isin(
# MAGIC           "PLACED",
# MAGIC           "SHIPPED",
# MAGIC           "DELIVERED",
# MAGIC           "CANCELLED",
# MAGIC           "RETURNED"
# MAGIC         ),
# MAGIC         lit("Unrecognized order status")
# MAGIC       )
# MAGIC     )
# MAGIC   )
# MAGIC
# MAGIC val orderValidationRejected = validatedOrdersV1
# MAGIC   .filter(col("rejection_reason") =!= "")
# MAGIC
# MAGIC val orderValidationPassed = validatedOrdersV1
# MAGIC   .filter(col("rejection_reason") === "")

# COMMAND ----------

# MAGIC %md
# MAGIC Check validation results

# COMMAND ----------

# MAGIC %scala
# MAGIC println("========== ORDER VALIDATION SUMMARY ==========")
# MAGIC
# MAGIC println("Total Bronze Orders     : " + cleanedOrdersV1.count())
# MAGIC println("Validation Passed       : " + orderValidationPassed.count())
# MAGIC println("Validation Rejected     : " + orderValidationRejected.count())
# MAGIC
# MAGIC println("========== REJECTION REASONS ==========")
# MAGIC
# MAGIC orderValidationRejected
# MAGIC   .select("order_id", "customer_id", "product_id", "quantity",
# MAGIC           "order_date", "order_status", "rejection_reason")
# MAGIC   .orderBy("order_id")
# MAGIC   .show(100, false)

# COMMAND ----------

# MAGIC %scala
# MAGIC duplicateOrderIds.show(50, false)

# COMMAND ----------

# MAGIC %md
# MAGIC Inspect duplicate order IDs

# COMMAND ----------

# MAGIC %scala
# MAGIC cleanedOrdersV1
# MAGIC   .filter(col("order_id") === "ORD000101")
# MAGIC   .select(
# MAGIC     "order_id",
# MAGIC     "customer_id",
# MAGIC     "product_id",
# MAGIC     "quantity",
# MAGIC     "order_status",
# MAGIC     "last_updated",
# MAGIC     "last_updated_parsed"
# MAGIC   )
# MAGIC   .show(20, false)

# COMMAND ----------

# MAGIC %md
# MAGIC Inspect all duplicate versions

# COMMAND ----------

# MAGIC %scala
# MAGIC cleanedOrdersV1
# MAGIC   .filter(col("order_id") === "ORD000091")
# MAGIC   .select("order_id", "order_status", "last_updated")
# MAGIC   .show(false)

# COMMAND ----------

# MAGIC %scala
# MAGIC cleanedOrdersV1
# MAGIC   .filter(
# MAGIC     col("order_id") === "ORD000092" ||
# MAGIC     col("order_id") === "ORD000093" ||
# MAGIC     col("order_id") === "ORD000094" ||
# MAGIC     col("order_id") === "ORD000095"
# MAGIC   )
# MAGIC   .select("order_id", "quantity", "order_status", "last_updated")
# MAGIC   .show(20, false)

# COMMAND ----------

# MAGIC %scala
# MAGIC cleanedOrdersV1
# MAGIC   .filter(
# MAGIC     col("order_id") === "ORD000101" ||
# MAGIC     col("order_id") === "ORD000102" ||
# MAGIC     col("order_id") === "ORD000103" ||
# MAGIC     col("order_id") === "ORD000104" ||
# MAGIC     col("order_id") === "ORD000105" ||
# MAGIC     col("order_id") === "ORD000106"
# MAGIC   )
# MAGIC   .select("order_id", "quantity", "order_status", "last_updated")
# MAGIC   .show(20, false)

# COMMAND ----------

# MAGIC %md
# MAGIC Deduplicate using the latest update

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.expressions.Window
# MAGIC import org.apache.spark.sql.functions._
# MAGIC
# MAGIC // Remove exact duplicate business records
# MAGIC val distinctOrdersV1 = cleanedOrdersV1.dropDuplicates(
# MAGIC   Seq(
# MAGIC     "order_id",
# MAGIC     "customer_id",
# MAGIC     "product_id",
# MAGIC     "quantity",
# MAGIC     "order_date",
# MAGIC     "order_status",
# MAGIC     "last_updated"
# MAGIC   )
# MAGIC )
# MAGIC
# MAGIC // Orders without a valid update timestamp cannot be ranked
# MAGIC val ordersWithValidTimestamp = distinctOrdersV1
# MAGIC   .filter(col("last_updated_parsed").isNotNull)
# MAGIC
# MAGIC // Find the latest timestamp for each order
# MAGIC val orderLatestWindow = Window.partitionBy("order_id")
# MAGIC
# MAGIC val latestOrderCandidates = ordersWithValidTimestamp
# MAGIC   .withColumn(
# MAGIC     "max_last_updated",
# MAGIC     max(col("last_updated_parsed")).over(orderLatestWindow)
# MAGIC   )
# MAGIC   .filter(col("last_updated_parsed") === col("max_last_updated"))
# MAGIC   .drop("max_last_updated")
# MAGIC
# MAGIC // Detect conflicting records with the same latest timestamp
# MAGIC val latestTimestampConflicts = latestOrderCandidates
# MAGIC   .groupBy("order_id")
# MAGIC   .count()
# MAGIC   .filter(col("count") > 1)
# MAGIC
# MAGIC println("========== LATEST VERSION CHECK ==========")
# MAGIC
# MAGIC println("Exact duplicate records removed: " +
# MAGIC   (cleanedOrdersV1.count() - distinctOrdersV1.count()))
# MAGIC
# MAGIC println("Orders with latest timestamp: " +
# MAGIC   latestOrderCandidates.select("order_id").distinct().count())
# MAGIC
# MAGIC println("Conflicting latest versions: " +
# MAGIC   latestTimestampConflicts.count())
# MAGIC
# MAGIC latestTimestampConflicts.show(50, false)

# COMMAND ----------

# MAGIC %md
# MAGIC Validate the latest order versions

# COMMAND ----------

# MAGIC %scala
# MAGIC val latestOrdersWithReferences = latestOrderCandidates
# MAGIC   .join(
# MAGIC     silverCustomersRef.withColumnRenamed(
# MAGIC       "customer_id", "_valid_customer_id"
# MAGIC     ),
# MAGIC     latestOrderCandidates("customer_id") === col("_valid_customer_id"),
# MAGIC     "left"
# MAGIC   )
# MAGIC   .join(
# MAGIC     silverProductsRef.withColumnRenamed(
# MAGIC       "product_id", "_valid_product_id"
# MAGIC     ),
# MAGIC     latestOrderCandidates("product_id") === col("_valid_product_id"),
# MAGIC     "left"
# MAGIC   )
# MAGIC
# MAGIC val latestOrdersValidated = latestOrdersWithReferences
# MAGIC   .withColumn(
# MAGIC     "rejection_reason",
# MAGIC     concat_ws(
# MAGIC       "; ",
# MAGIC       when(col("order_id").isNull || col("order_id") === "",
# MAGIC         lit("Missing order ID")),
# MAGIC       when(col("customer_id").isNull || col("customer_id") === "",
# MAGIC         lit("Missing customer ID")),
# MAGIC       when(col("_valid_customer_id").isNull,
# MAGIC         lit("Customer ID not found in Silver")),
# MAGIC       when(col("product_id").isNull || col("product_id") === "",
# MAGIC         lit("Missing product ID")),
# MAGIC       when(col("_valid_product_id").isNull,
# MAGIC         lit("Product ID not found in Silver")),
# MAGIC       when(col("quantity_parsed").isNull,
# MAGIC         lit("Invalid or missing quantity")),
# MAGIC       when(col("quantity_parsed") <= 0,
# MAGIC         lit("Quantity must be greater than zero")),
# MAGIC       when(col("order_date_parsed").isNull,
# MAGIC         lit("Missing or invalid order date")),
# MAGIC       when(col("order_date_parsed") > current_date(),
# MAGIC         lit("Future order date")),
# MAGIC       when(col("last_updated_parsed").isNull,
# MAGIC         lit("Missing or invalid update timestamp")),
# MAGIC       when(col("order_status").isNull || col("order_status") === "",
# MAGIC         lit("Missing order status")),
# MAGIC       when(
# MAGIC         !col("order_status").isin(
# MAGIC           "PLACED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"
# MAGIC         ),
# MAGIC         lit("Unrecognized order status")
# MAGIC       )
# MAGIC     )
# MAGIC   )
# MAGIC
# MAGIC val silverOrdersCandidates = latestOrdersValidated
# MAGIC   .filter(col("rejection_reason") === "")
# MAGIC
# MAGIC val rejectedLatestOrders = latestOrdersValidated
# MAGIC   .filter(col("rejection_reason") =!= "")
# MAGIC
# MAGIC println("========== LATEST ORDER VALIDATION ==========")
# MAGIC println("Latest Candidates : " + latestOrderCandidates.count())
# MAGIC println("Valid Orders      : " + silverOrdersCandidates.count())
# MAGIC println("Rejected Orders   : " + rejectedLatestOrders.count())
# MAGIC
# MAGIC rejectedLatestOrders
# MAGIC   .select("order_id", "customer_id", "product_id",
# MAGIC           "order_status", "last_updated", "rejection_reason")
# MAGIC   .orderBy("order_id")
# MAGIC   .show(100, false)

# COMMAND ----------

# MAGIC %md
# MAGIC Create the final Silver Orders DataFrame

# COMMAND ----------

# MAGIC %scala
# MAGIC val silverOrdersFinal = silverOrdersCandidates.select(
# MAGIC   col("order_id"),
# MAGIC   col("customer_id"),
# MAGIC   col("product_id"),
# MAGIC   col("quantity_parsed").cast("int").as("quantity"),
# MAGIC   col("order_date_parsed").cast("date").as("order_date"),
# MAGIC   col("order_status"),
# MAGIC   col("last_updated_parsed").cast("timestamp").as("last_updated"),
# MAGIC   col("_source_file"),
# MAGIC   col("_ingestion_timestamp")
# MAGIC )
# MAGIC
# MAGIC println("========== FINAL SILVER ORDERS ==========")
# MAGIC
# MAGIC println("Total Silver Orders: " + silverOrdersFinal.count())
# MAGIC
# MAGIC silverOrdersFinal.printSchema()
# MAGIC
# MAGIC silverOrdersFinal.orderBy("order_id").show(20, false)

# COMMAND ----------

# MAGIC %md
# MAGIC Write Silver Orders to Delta Lake

# COMMAND ----------

# MAGIC %scala
# MAGIC val silverOrdersPath = basePath + "silver/orders"
# MAGIC
# MAGIC silverOrdersFinal.write
# MAGIC   .format("delta")
# MAGIC   .mode("overwrite")
# MAGIC   .save(silverOrdersPath)
# MAGIC
# MAGIC println("Silver Orders Delta write successful")
# MAGIC println("Location: " + silverOrdersPath)

# COMMAND ----------

# MAGIC %md
# MAGIC Verify the saved Delta table

# COMMAND ----------

# MAGIC %scala
# MAGIC val silverOrdersCheck = spark.read
# MAGIC   .format("delta")
# MAGIC   .load(silverOrdersPath)
# MAGIC
# MAGIC println("========== SILVER ORDERS VERIFICATION ==========")
# MAGIC
# MAGIC println("Saved Orders Count: " + silverOrdersCheck.count())
# MAGIC
# MAGIC silverOrdersCheck.printSchema()
# MAGIC
# MAGIC silverOrdersCheck
# MAGIC   .orderBy("order_id")
# MAGIC   .show(10, false)

# COMMAND ----------

# MAGIC %md
# MAGIC Save rejected latest orders

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC val rejectedOrdersFinal = rejectedLatestOrders
# MAGIC   .drop("_valid_customer_id", "_valid_product_id")
# MAGIC   .select(
# MAGIC     col("order_id"),
# MAGIC     col("customer_id"),
# MAGIC     col("product_id"),
# MAGIC     col("quantity"),
# MAGIC     col("order_date"),
# MAGIC     col("order_status"),
# MAGIC     col("last_updated"),
# MAGIC     col("rejection_reason"),
# MAGIC     col("_source_file"),
# MAGIC     col("_ingestion_timestamp")
# MAGIC   )
# MAGIC
# MAGIC val rejectedOrdersPath = basePath + "rejected/orders"
# MAGIC
# MAGIC rejectedOrdersFinal.write
# MAGIC   .format("delta")
# MAGIC   .mode("overwrite")
# MAGIC   .save(rejectedOrdersPath)
# MAGIC
# MAGIC println("Rejected Orders Delta write successful")
# MAGIC println("Rejected Orders Count: " + rejectedOrdersFinal.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Verify both Delta tables and check for missing timestamps

# COMMAND ----------

# MAGIC %scala
# MAGIC val silverOrdersCheck = spark.read
# MAGIC   .format("delta")
# MAGIC   .load(basePath + "silver/orders")
# MAGIC
# MAGIC val rejectedOrdersCheck = spark.read
# MAGIC   .format("delta")
# MAGIC   .load(basePath + "rejected/orders")
# MAGIC
# MAGIC println("========== ORDERS PIPELINE VERIFICATION ==========")
# MAGIC
# MAGIC println("Bronze Orders       : " + bronzeOrdersV2.count())
# MAGIC println("Silver Orders       : " + silverOrdersCheck.count())
# MAGIC println("Rejected Orders     : " + rejectedOrdersCheck.count())
# MAGIC
# MAGIC println("========== INVALID UPDATE TIMESTAMP CHECK ==========")
# MAGIC
# MAGIC val invalidUpdateTimestampOrders = distinctOrdersV1
# MAGIC   .filter(col("last_updated_parsed").isNull)
# MAGIC
# MAGIC println(
# MAGIC   "Orders with missing/invalid last_updated: " +
# MAGIC   invalidUpdateTimestampOrders.count()
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC Read the new CSV in Databricks

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val ordersV2Path =
# MAGIC   basePath + "schema_evolution/orders_v2/orders_v2.csv"
# MAGIC
# MAGIC println(ordersV2Path)
# MAGIC
# MAGIC dbutils.fs.ls(
# MAGIC   basePath + "schema_evolution/orders_v2/"
# MAGIC ).foreach(println)

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import org.apache.spark.sql.types._
# MAGIC
# MAGIC val ordersV2Schema = StructType(Seq(
# MAGIC   StructField("order_id", StringType, true),
# MAGIC   StructField("customer_id", StringType, true),
# MAGIC   StructField("product_id", StringType, true),
# MAGIC   StructField("quantity", StringType, true),
# MAGIC   StructField("order_date", StringType, true),
# MAGIC   StructField("order_status", StringType, true),
# MAGIC   StructField("last_updated", StringType, true),
# MAGIC   StructField("sales_channel", StringType, true),
# MAGIC   StructField("discount_amount", StringType, true)
# MAGIC ))
# MAGIC
# MAGIC println("Updated Orders V2 schema created successfully")
# MAGIC ordersV2Schema.printTreeString()

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val ordersV2Path =
# MAGIC   "abfss://adlsdatabrics2c@adlsdatabricks2.dfs.core.windows.net/nexus/batch/schema_evolution/orders_v2/orders_v2.csv"
# MAGIC
# MAGIC val ordersV2Raw = spark.read
# MAGIC   .format("csv")
# MAGIC   .option("header", "true")
# MAGIC   .option("mode", "PERMISSIVE")
# MAGIC   .schema(ordersV2Schema)
# MAGIC   .load(ordersV2Path)
# MAGIC
# MAGIC println("Orders V2 DataFrame created successfully")

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC println("========== ORDERS V2 VERIFICATION ==========")
# MAGIC
# MAGIC println("Orders V2 Count: " + ordersV2Raw.count())
# MAGIC
# MAGIC ordersV2Raw.printSchema()
# MAGIC
# MAGIC ordersV2Raw.show(20, false)

# COMMAND ----------

# MAGIC %scala
# MAGIC val basePath =
# MAGIC   "abfss://adlsdatabrics2c@adlsdatabricks2.dfs.core.windows.net/nexus/batch/"
# MAGIC val existingBronzeOrders = spark.read
# MAGIC   .format("delta")
# MAGIC   .load(basePath + "bronze_delta/orders")
# MAGIC
# MAGIC println("Existing Bronze DataFrame loaded successfully")

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC existingBronzeOrders.printSchema()

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC println(existingBronzeOrders.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Check overlapping order IDs

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val overlappingOrders = ordersV2Raw
# MAGIC   .select("order_id")
# MAGIC   .distinct()
# MAGIC   .join(
# MAGIC     existingBronzeOrders.select("order_id").distinct(),
# MAGIC     Seq("order_id"),
# MAGIC     "inner"
# MAGIC   )
# MAGIC
# MAGIC println("Overlapping Order IDs: " + overlappingOrders.count())
# MAGIC overlappingOrders.show(false)

# COMMAND ----------

# MAGIC %md
# MAGIC Prepare V2 records for Bronze

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import org.apache.spark.sql.functions._
# MAGIC
# MAGIC val ordersV2ForBronze = ordersV2Raw
# MAGIC   .withColumn("_source_file", input_file_name())
# MAGIC   .withColumn("_ingestion_timestamp", current_timestamp())
# MAGIC
# MAGIC println("Prepared V2 records: " + ordersV2ForBronze.count())
# MAGIC ordersV2ForBronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Append V2 records with mergeSchema

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC ordersV2ForBronze.write
# MAGIC   .format("delta")
# MAGIC   .mode("append")
# MAGIC   .option("mergeSchema", "true")
# MAGIC   .save(basePath + "bronze_delta/orders")
# MAGIC
# MAGIC println("Orders V2 appended successfully with schema evolution")

# COMMAND ----------

# MAGIC %md
# MAGIC Reload the updated Bronze table

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val bronzeOrdersEvolved = spark.read
# MAGIC   .format("delta")
# MAGIC   .load(basePath + "bronze_delta/orders")
# MAGIC
# MAGIC println("Updated Bronze Orders loaded successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC Verify record count

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC println("Total Bronze Orders: " + bronzeOrdersEvolved.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Verify evolved schema

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC bronzeOrdersEvolved.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Verify old records have NULL values

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val oldOrdersNullCheck = bronzeOrdersEvolved
# MAGIC   .filter(col("sales_channel").isNull &&
# MAGIC           col("discount_amount").isNull)
# MAGIC
# MAGIC println(
# MAGIC   "Records with NULL sales_channel and discount_amount: " +
# MAGIC   oldOrdersNullCheck.count()
# MAGIC )

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC bronzeOrdersEvolved
# MAGIC   .filter(col("sales_channel").isNotNull)
# MAGIC   .select(
# MAGIC     "order_id",
# MAGIC     "customer_id",
# MAGIC     "product_id",
# MAGIC     "sales_channel",
# MAGIC     "discount_amount"
# MAGIC   )
# MAGIC   .orderBy("order_id")
# MAGIC   .show(20, false)

# COMMAND ----------

# MAGIC %md
# MAGIC Rebuild the Orders cleaning stage

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val cleanedOrdersEvolved = bronzeOrdersEvolved
# MAGIC   .withColumn("order_id", upper(trim(col("order_id"))))
# MAGIC   .withColumn("customer_id", upper(trim(col("customer_id"))))
# MAGIC   .withColumn("product_id", upper(trim(col("product_id"))))
# MAGIC   .withColumn("order_status", upper(trim(col("order_status"))))
# MAGIC   .withColumn(
# MAGIC     "sales_channel",
# MAGIC     when(
# MAGIC       col("sales_channel").isNotNull,
# MAGIC       upper(trim(col("sales_channel")))
# MAGIC     ).otherwise(lit(null).cast("string"))
# MAGIC   )
# MAGIC   .withColumn(
# MAGIC     "quantity_parsed",
# MAGIC     expr("try_cast(trim(quantity) AS INT)")
# MAGIC   )
# MAGIC   .withColumn(
# MAGIC     "discount_amount_parsed",
# MAGIC     expr("try_cast(trim(discount_amount) AS DECIMAL(10,2))")
# MAGIC   )
# MAGIC   .withColumn(
# MAGIC     "order_date_parsed",
# MAGIC     coalesce(
# MAGIC       to_date(try_to_timestamp(col("order_date"), lit("yyyy-MM-dd"))),
# MAGIC       to_date(try_to_timestamp(col("order_date"), lit("dd-MM-yyyy")))
# MAGIC     )
# MAGIC   )
# MAGIC   .withColumn(
# MAGIC     "last_updated_parsed",
# MAGIC     try_to_timestamp(col("last_updated"), lit("yyyy-MM-dd HH:mm:ss"))
# MAGIC   )
# MAGIC
# MAGIC println("Evolved Orders cleaning completed")

# COMMAND ----------

# MAGIC %md
# MAGIC Inspect the parsed V2 records

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC cleanedOrdersEvolved
# MAGIC   .filter(col("sales_channel").isNotNull)
# MAGIC   .select(
# MAGIC     "order_id",
# MAGIC     "quantity_parsed",
# MAGIC     "order_date_parsed",
# MAGIC     "order_status",
# MAGIC     "sales_channel",
# MAGIC     "discount_amount_parsed",
# MAGIC     "last_updated_parsed"
# MAGIC   )
# MAGIC   .orderBy("order_id")
# MAGIC   .show(20, false)

# COMMAND ----------

# MAGIC %md
# MAGIC Prepare reference data

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val silverCustomersRefEvolved = spark.read
# MAGIC   .format("delta")
# MAGIC   .load(basePath + "silver/customers")
# MAGIC   .select("customer_id")
# MAGIC   .distinct()
# MAGIC
# MAGIC val silverProductsRefEvolved = spark.read
# MAGIC   .format("delta")
# MAGIC   .load(basePath + "silver/products")
# MAGIC   .select("product_id")
# MAGIC   .distinct()
# MAGIC
# MAGIC println("Customer reference count: " + silverCustomersRefEvolved.count())
# MAGIC println("Product reference count: " + silverProductsRefEvolved.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Check valid sales channels

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC cleanedOrdersEvolved
# MAGIC   .filter(
# MAGIC     col("sales_channel").isNotNull &&
# MAGIC     !col("sales_channel").isin("STORE", "WEB", "MOBILE_APP")
# MAGIC   )
# MAGIC   .select("order_id", "sales_channel")
# MAGIC   .show(false)

# COMMAND ----------

# MAGIC %md
# MAGIC Build the complete Orders validation logic

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val ordersValidatedEvolved = cleanedOrdersEvolved
# MAGIC   .join(
# MAGIC     silverCustomersRefEvolved
# MAGIC       .withColumn("_valid_customer_id", lit(true)),
# MAGIC     Seq("customer_id"),
# MAGIC     "left"
# MAGIC   )
# MAGIC   .join(
# MAGIC     silverProductsRefEvolved
# MAGIC       .withColumn("_valid_product_id", lit(true)),
# MAGIC     Seq("product_id"),
# MAGIC     "left"
# MAGIC   )
# MAGIC   .withColumn(
# MAGIC     "rejection_reason",
# MAGIC     concat_ws(
# MAGIC       "; ",
# MAGIC       when(
# MAGIC         col("order_id").isNull ||
# MAGIC         !col("order_id").rlike("^ORD[0-9]{6}$"),
# MAGIC         lit("Invalid order_id")
# MAGIC       ),
# MAGIC       when(
# MAGIC         col("customer_id").isNull ||
# MAGIC         col("_valid_customer_id").isNull,
# MAGIC         lit("Invalid or unknown customer_id")
# MAGIC       ),
# MAGIC       when(
# MAGIC         col("product_id").isNull ||
# MAGIC         col("_valid_product_id").isNull,
# MAGIC         lit("Invalid or unknown product_id")
# MAGIC       ),
# MAGIC       when(
# MAGIC         col("quantity_parsed").isNull ||
# MAGIC         col("quantity_parsed") <= 0,
# MAGIC         lit("Invalid quantity")
# MAGIC       ),
# MAGIC       when(
# MAGIC         col("order_date_parsed").isNull ||
# MAGIC         col("order_date_parsed") > current_date(),
# MAGIC         lit("Invalid or future order_date")
# MAGIC       ),
# MAGIC       when(
# MAGIC         col("last_updated_parsed").isNull,
# MAGIC         lit("Invalid last_updated")
# MAGIC       ),
# MAGIC       when(
# MAGIC         col("order_status").isNull ||
# MAGIC         !col("order_status").isin(
# MAGIC           "PLACED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"
# MAGIC         ),
# MAGIC         lit("Invalid order_status")
# MAGIC       ),
# MAGIC       when(
# MAGIC         col("sales_channel").isNotNull &&
# MAGIC         !col("sales_channel").isin("STORE", "WEB", "MOBILE_APP"),
# MAGIC         lit("Invalid sales_channel")
# MAGIC       ),
# MAGIC       when(
# MAGIC         col("discount_amount").isNotNull &&
# MAGIC         (
# MAGIC           col("discount_amount_parsed").isNull ||
# MAGIC           col("discount_amount_parsed") < 0
# MAGIC         ),
# MAGIC         lit("Invalid discount_amount")
# MAGIC       )
# MAGIC     )
# MAGIC   )
# MAGIC
# MAGIC println("Orders validation rules applied successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC Check validation counts

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC println(
# MAGIC   "Total records: " + ordersValidatedEvolved.count()
# MAGIC )
# MAGIC
# MAGIC println(
# MAGIC   "Valid records before deduplication: " +
# MAGIC   ordersValidatedEvolved.filter(col("rejection_reason") === "").count()
# MAGIC )
# MAGIC
# MAGIC println(
# MAGIC   "Rejected records before deduplication: " +
# MAGIC   ordersValidatedEvolved.filter(col("rejection_reason") =!= "").count()
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC Remove exact duplicate records

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val distinctOrdersEvolved = ordersValidatedEvolved
# MAGIC   .dropDuplicates(Seq(
# MAGIC     "order_id",
# MAGIC     "customer_id",
# MAGIC     "product_id",
# MAGIC     "quantity",
# MAGIC     "order_date",
# MAGIC     "order_status",
# MAGIC     "last_updated",
# MAGIC     "sales_channel",
# MAGIC     "discount_amount"
# MAGIC   ))
# MAGIC
# MAGIC println(
# MAGIC   "Records after removing exact duplicates: " +
# MAGIC   distinctOrdersEvolved.count()
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC Select the latest version of each order

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import org.apache.spark.sql.expressions.Window
# MAGIC
# MAGIC val orderLatestWindowEvolved = Window
# MAGIC   .partitionBy("order_id")
# MAGIC
# MAGIC val latestOrderCandidatesEvolved = distinctOrdersEvolved
# MAGIC   .withColumn(
# MAGIC     "max_last_updated",
# MAGIC     max(col("last_updated_parsed")).over(orderLatestWindowEvolved)
# MAGIC   )
# MAGIC   .filter(
# MAGIC     col("last_updated_parsed") === col("max_last_updated")
# MAGIC   )
# MAGIC   .drop("max_last_updated")
# MAGIC
# MAGIC println(
# MAGIC   "Latest order candidates: " +
# MAGIC   latestOrderCandidatesEvolved.count()
# MAGIC )

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val latestTimestampConflictsEvolved = latestOrderCandidatesEvolved
# MAGIC   .groupBy("order_id")
# MAGIC   .count()
# MAGIC   .filter(col("count") > 1)
# MAGIC
# MAGIC println(
# MAGIC   "Latest timestamp conflicts: " +
# MAGIC   latestTimestampConflictsEvolved.count()
# MAGIC )
# MAGIC
# MAGIC latestTimestampConflictsEvolved.show(false)

# COMMAND ----------

# MAGIC %md
# MAGIC Separate valid and rejected latest orders

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val silverOrdersEvolved = latestOrderCandidatesEvolved
# MAGIC   .filter(col("rejection_reason") === "")
# MAGIC
# MAGIC val rejectedOrdersEvolved = latestOrderCandidatesEvolved
# MAGIC   .filter(col("rejection_reason") =!= "")
# MAGIC
# MAGIC println(
# MAGIC   "Latest valid Orders: " + silverOrdersEvolved.count()
# MAGIC )
# MAGIC
# MAGIC println(
# MAGIC   "Latest rejected Orders: " + rejectedOrdersEvolved.count()
# MAGIC )

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC rejectedOrdersEvolved
# MAGIC   .filter(col("order_id").between("ORD000159", "ORD000170"))
# MAGIC   .select(
# MAGIC     "order_id",
# MAGIC     "customer_id",
# MAGIC     "product_id",
# MAGIC     "sales_channel",
# MAGIC     "discount_amount",
# MAGIC     "rejection_reason"
# MAGIC   )
# MAGIC   .orderBy("order_id")
# MAGIC   .show(false)

# COMMAND ----------

# MAGIC %md
# MAGIC Prepare final Silver Orders schema

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val silverOrdersFinalEvolved = silverOrdersEvolved.select(
# MAGIC   col("order_id"),
# MAGIC   col("customer_id"),
# MAGIC   col("product_id"),
# MAGIC   col("quantity_parsed").cast("int").as("quantity"),
# MAGIC   col("order_date_parsed").cast("date").as("order_date"),
# MAGIC   col("order_status"),
# MAGIC   col("sales_channel"),
# MAGIC   col("discount_amount_parsed")
# MAGIC     .cast("decimal(10,2)")
# MAGIC     .as("discount_amount"),
# MAGIC   col("last_updated_parsed").cast("timestamp").as("last_updated"),
# MAGIC   col("_source_file"),
# MAGIC   col("_ingestion_timestamp")
# MAGIC )
# MAGIC
# MAGIC println("Final Silver Orders prepared")
# MAGIC silverOrdersFinalEvolved.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Prepare final Rejected Orders

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val rejectedOrdersFinalEvolved = rejectedOrdersEvolved
# MAGIC   .select(
# MAGIC     col("order_id"),
# MAGIC     col("customer_id"),
# MAGIC     col("product_id"),
# MAGIC     col("quantity"),
# MAGIC     col("order_date"),
# MAGIC     col("order_status"),
# MAGIC     col("last_updated"),
# MAGIC     col("sales_channel"),
# MAGIC     col("discount_amount"),
# MAGIC     col("rejection_reason"),
# MAGIC     col("_source_file"),
# MAGIC     col("_ingestion_timestamp")
# MAGIC   )
# MAGIC
# MAGIC println("Final Rejected Orders prepared")
# MAGIC rejectedOrdersFinalEvolved.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Verify final counts

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC println("Bronze Orders: " + bronzeOrdersEvolved.count())
# MAGIC println("Silver Orders: " + silverOrdersFinalEvolved.count())
# MAGIC println("Rejected Orders: " + rejectedOrdersFinalEvolved.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Write Silver Orders to ADLS Gen2

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC silverOrdersFinalEvolved.write
# MAGIC   .format("delta")
# MAGIC   .mode("overwrite")
# MAGIC   .option("overwriteSchema", "true")
# MAGIC   .save(basePath + "silver/orders")
# MAGIC
# MAGIC println("Silver Orders updated successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC Write Rejected Orders to ADLS Gen2

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC rejectedOrdersFinalEvolved.write
# MAGIC   .format("delta")
# MAGIC   .mode("overwrite")
# MAGIC   .option("overwriteSchema", "true")
# MAGIC   .save(basePath + "rejected/orders")
# MAGIC
# MAGIC println("Rejected Orders updated successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC Read back and verify persisted Delta tables

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val silverOrdersVerified = spark.read
# MAGIC   .format("delta")
# MAGIC   .load(basePath + "silver/orders")
# MAGIC
# MAGIC val rejectedOrdersVerified = spark.read
# MAGIC   .format("delta")
# MAGIC   .load(basePath + "rejected/orders")
# MAGIC
# MAGIC println("========== ORDERS PIPELINE VERIFICATION ==========")
# MAGIC
# MAGIC println("Silver Orders: " + silverOrdersVerified.count())
# MAGIC println("Rejected Orders: " + rejectedOrdersVerified.count())
# MAGIC
# MAGIC println("========== SILVER SCHEMA ==========")
# MAGIC silverOrdersVerified.printSchema()
# MAGIC
# MAGIC println("========== REJECTED SCHEMA ==========")
# MAGIC rejectedOrdersVerified.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Star Schema

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Silver source tables

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val goldCustomersSource = spark.read
# MAGIC   .format("delta")
# MAGIC   .load(basePath + "silver/customers")
# MAGIC
# MAGIC val goldProductsSource = spark.read
# MAGIC   .format("delta")
# MAGIC   .load(basePath + "silver/products")
# MAGIC
# MAGIC val goldOrdersSource = spark.read
# MAGIC   .format("delta")
# MAGIC   .load(basePath + "silver/orders")
# MAGIC
# MAGIC println("========== GOLD SOURCE VERIFICATION ==========")
# MAGIC println("Customers: " + goldCustomersSource.count())
# MAGIC println("Products: " + goldProductsSource.count())
# MAGIC println("Orders: " + goldOrdersSource.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Build DIM_CUSTOMER

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import org.apache.spark.sql.expressions.Window
# MAGIC
# MAGIC val customerKeyWindow = Window
# MAGIC   .orderBy(col("customer_id"))
# MAGIC
# MAGIC val dimCustomer = goldCustomersSource
# MAGIC   .select(
# MAGIC     col("customer_id"),
# MAGIC     col("customer_name"),
# MAGIC     col("email"),
# MAGIC     col("city"),
# MAGIC     col("signup_date")
# MAGIC   )
# MAGIC   .withColumn(
# MAGIC     "customer_key",
# MAGIC     row_number().over(customerKeyWindow)
# MAGIC   )
# MAGIC   .select(
# MAGIC     col("customer_key"),
# MAGIC     col("customer_id"),
# MAGIC     col("customer_name"),
# MAGIC     col("email"),
# MAGIC     col("city"),
# MAGIC     col("signup_date")
# MAGIC   )
# MAGIC
# MAGIC println("DIM_CUSTOMER prepared successfully")
# MAGIC println("DIM_CUSTOMER count: " + dimCustomer.count())
# MAGIC
# MAGIC dimCustomer.printSchema()
# MAGIC dimCustomer.show(10, false)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Build DIM_PRODUCT

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val productKeyWindow = Window
# MAGIC   .orderBy(col("product_id"))
# MAGIC
# MAGIC val dimProduct = goldProductsSource
# MAGIC   .select(
# MAGIC     col("product_id"),
# MAGIC     col("product_name"),
# MAGIC     col("category"),
# MAGIC     col("unit_price"),
# MAGIC     col("active")
# MAGIC   )
# MAGIC   .withColumn(
# MAGIC     "product_key",
# MAGIC     row_number().over(productKeyWindow)
# MAGIC   )
# MAGIC   .select(
# MAGIC     col("product_key"),
# MAGIC     col("product_id"),
# MAGIC     col("product_name"),
# MAGIC     col("category"),
# MAGIC     col("unit_price"),
# MAGIC     col("active")
# MAGIC   )
# MAGIC
# MAGIC println("DIM_PRODUCT prepared successfully")
# MAGIC println("DIM_PRODUCT count: " + dimProduct.count())
# MAGIC
# MAGIC dimProduct.printSchema()
# MAGIC dimProduct.show(10, false)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Build DIM_DATE

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val orderDateRange = goldOrdersSource
# MAGIC   .agg(
# MAGIC     min(col("order_date")).as("min_date"),
# MAGIC     max(col("order_date")).as("max_date")
# MAGIC   )
# MAGIC
# MAGIC orderDateRange.show(false)

# COMMAND ----------

# MAGIC %md
# MAGIC Generate DIM_DATE

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val dimDate = orderDateRange
# MAGIC   .select(
# MAGIC     explode(
# MAGIC       sequence(
# MAGIC         col("min_date"),
# MAGIC         col("max_date"),
# MAGIC         expr("interval 1 day")
# MAGIC       )
# MAGIC     ).as("full_date")
# MAGIC   )
# MAGIC   .withColumn(
# MAGIC     "date_key",
# MAGIC     date_format(col("full_date"), "yyyyMMdd").cast("int")
# MAGIC   )
# MAGIC   .withColumn("day", dayofmonth(col("full_date")))
# MAGIC   .withColumn("month", month(col("full_date")))
# MAGIC   .withColumn("month_name", date_format(col("full_date"), "MMMM"))
# MAGIC   .withColumn("quarter", quarter(col("full_date")))
# MAGIC   .withColumn("year", year(col("full_date")))
# MAGIC   .withColumn("day_of_week", date_format(col("full_date"), "EEEE"))
# MAGIC   .withColumn(
# MAGIC     "is_weekend",
# MAGIC     dayofweek(col("full_date")).isin(1, 7)
# MAGIC   )
# MAGIC   .select(
# MAGIC     col("date_key"),
# MAGIC     col("full_date"),
# MAGIC     col("day"),
# MAGIC     col("month"),
# MAGIC     col("month_name"),
# MAGIC     col("quarter"),
# MAGIC     col("year"),
# MAGIC     col("day_of_week"),
# MAGIC     col("is_weekend")
# MAGIC   )
# MAGIC
# MAGIC println("DIM_DATE prepared successfully")
# MAGIC println("DIM_DATE count: " + dimDate.count())
# MAGIC
# MAGIC dimDate.printSchema()
# MAGIC dimDate.show(10, false)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Verify all three dimensions

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC println("========== DIMENSION COUNTS ==========")
# MAGIC println("DIM_CUSTOMER: " + dimCustomer.count())
# MAGIC println("DIM_PRODUCT : " + dimProduct.count())
# MAGIC println("DIM_DATE    : " + dimDate.count())
# MAGIC
# MAGIC println("========== UNIQUE KEY CHECKS ==========")
# MAGIC
# MAGIC println(
# MAGIC   "Duplicate customer keys: " +
# MAGIC   dimCustomer.groupBy("customer_key").count()
# MAGIC     .filter(col("count") > 1).count()
# MAGIC )
# MAGIC
# MAGIC println(
# MAGIC   "Duplicate product keys: " +
# MAGIC   dimProduct.groupBy("product_key").count()
# MAGIC     .filter(col("count") > 1).count()
# MAGIC )
# MAGIC
# MAGIC println(
# MAGIC   "Duplicate date keys: " +
# MAGIC   dimDate.groupBy("date_key").count()
# MAGIC     .filter(col("count") > 1).count()
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Build FACT_ORDER

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val ordersWithDimensionKeys = goldOrdersSource
# MAGIC   .join(
# MAGIC     dimCustomer.select("customer_id", "customer_key"),
# MAGIC     Seq("customer_id"),
# MAGIC     "inner"
# MAGIC   )
# MAGIC   .join(
# MAGIC     dimProduct.select("product_id", "product_key"),
# MAGIC     Seq("product_id"),
# MAGIC     "inner"
# MAGIC   )
# MAGIC   .join(
# MAGIC     dimDate.select(
# MAGIC       col("full_date").as("date_join"),
# MAGIC       col("date_key")
# MAGIC     ),
# MAGIC     col("order_date") === col("date_join"),
# MAGIC     "inner"
# MAGIC   )
# MAGIC   .drop("date_join")
# MAGIC
# MAGIC println("Orders joined with dimensions: " + ordersWithDimensionKeys.count())
# MAGIC
# MAGIC ordersWithDimensionKeys.select(
# MAGIC   "order_id",
# MAGIC   "customer_id",
# MAGIC   "customer_key",
# MAGIC   "product_id",
# MAGIC   "product_key",
# MAGIC   "order_date",
# MAGIC   "date_key"
# MAGIC ).show(10, false)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Generate sales_key and finalize FACT_ORDER

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val salesKeyWindow = Window
# MAGIC   .orderBy(col("order_id"))
# MAGIC
# MAGIC val factOrder = ordersWithDimensionKeys
# MAGIC   .withColumn(
# MAGIC     "sales_key",
# MAGIC     row_number().over(salesKeyWindow)
# MAGIC   )
# MAGIC   .select(
# MAGIC     col("sales_key"),
# MAGIC     col("order_id"),
# MAGIC     col("customer_key"),
# MAGIC     col("product_key"),
# MAGIC     col("date_key"),
# MAGIC     col("quantity"),
# MAGIC     col("order_status"),
# MAGIC     col("sales_channel"),
# MAGIC     col("discount_amount"),
# MAGIC     col("last_updated")
# MAGIC   )
# MAGIC
# MAGIC println("FACT_ORDER prepared successfully")
# MAGIC println("FACT_ORDER count: " + factOrder.count())
# MAGIC
# MAGIC factOrder.printSchema()
# MAGIC factOrder.show(10, false)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validate FACT_ORDER

# COMMAND ----------

# MAGIC %scala
# MAGIC println("========== FACT_ORDER VALIDATION ==========")
# MAGIC
# MAGIC println("Total Records: " + factOrder.count())
# MAGIC
# MAGIC println(
# MAGIC   "Duplicate sales_key: " +
# MAGIC   factOrder.groupBy("sales_key")
# MAGIC     .count()
# MAGIC     .filter(col("count") > 1)
# MAGIC     .count()
# MAGIC )
# MAGIC
# MAGIC println(
# MAGIC   "Null sales_key: " +
# MAGIC   factOrder.filter(col("sales_key").isNull).count()
# MAGIC )
# MAGIC
# MAGIC println(
# MAGIC   "Null customer_key: " +
# MAGIC   factOrder.filter(col("customer_key").isNull).count()
# MAGIC )
# MAGIC
# MAGIC println(
# MAGIC   "Null product_key: " +
# MAGIC   factOrder.filter(col("product_key").isNull).count()
# MAGIC )
# MAGIC
# MAGIC println(
# MAGIC   "Null date_key: " +
# MAGIC   factOrder.filter(col("date_key").isNull).count()
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validate foreign-key relationships

# COMMAND ----------

# MAGIC %scala
# MAGIC println("========== FOREIGN KEY VALIDATION ==========")
# MAGIC
# MAGIC val invalidCustomerKeys = factOrder
# MAGIC   .join(
# MAGIC     dimCustomer.select("customer_key"),
# MAGIC     Seq("customer_key"),
# MAGIC     "left_anti"
# MAGIC   )
# MAGIC
# MAGIC val invalidProductKeys = factOrder
# MAGIC   .join(
# MAGIC     dimProduct.select("product_key"),
# MAGIC     Seq("product_key"),
# MAGIC     "left_anti"
# MAGIC   )
# MAGIC
# MAGIC val invalidDateKeys = factOrder
# MAGIC   .join(
# MAGIC     dimDate.select("date_key"),
# MAGIC     Seq("date_key"),
# MAGIC     "left_anti"
# MAGIC   )
# MAGIC
# MAGIC println("Invalid customer keys: " + invalidCustomerKeys.count())
# MAGIC println("Invalid product keys: " + invalidProductKeys.count())
# MAGIC println("Invalid date keys: " + invalidDateKeys.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write Gold tables to ADLS Gen2

# COMMAND ----------

# MAGIC %scala
# MAGIC // DIM_CUSTOMER
# MAGIC dimCustomer.write
# MAGIC   .format("delta")
# MAGIC   .mode("overwrite")
# MAGIC   .save(basePath + "gold/dim_customer")
# MAGIC
# MAGIC // DIM_PRODUCT
# MAGIC dimProduct.write
# MAGIC   .format("delta")
# MAGIC   .mode("overwrite")
# MAGIC   .save(basePath + "gold/dim_product")
# MAGIC
# MAGIC // DIM_DATE
# MAGIC dimDate.write
# MAGIC   .format("delta")
# MAGIC   .mode("overwrite")
# MAGIC   .save(basePath + "gold/dim_date")
# MAGIC
# MAGIC // FACT_ORDER
# MAGIC factOrder.write
# MAGIC   .format("delta")
# MAGIC   .mode("overwrite")
# MAGIC   .save(basePath + "gold/fact_order")
# MAGIC
# MAGIC println("========== GOLD LAYER WRITE SUMMARY ==========")
# MAGIC println("DIM_CUSTOMER written successfully")
# MAGIC println("DIM_PRODUCT written successfully")
# MAGIC println("DIM_DATE written successfully")
# MAGIC println("FACT_ORDER written successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC Reload and verify persisted Gold tables

# COMMAND ----------

# MAGIC %scala
# MAGIC val dimCustomerVerified = spark.read.format("delta")
# MAGIC   .load(basePath + "gold/dim_customer")
# MAGIC
# MAGIC val dimProductVerified = spark.read.format("delta")
# MAGIC   .load(basePath + "gold/dim_product")
# MAGIC
# MAGIC val dimDateVerified = spark.read.format("delta")
# MAGIC   .load(basePath + "gold/dim_date")
# MAGIC
# MAGIC val factOrderVerified = spark.read.format("delta")
# MAGIC   .load(basePath + "gold/fact_order")
# MAGIC
# MAGIC println("========== GOLD LAYER VERIFICATION ==========")
# MAGIC
# MAGIC println("DIM_CUSTOMER: " + dimCustomerVerified.count())
# MAGIC println("DIM_PRODUCT: " + dimProductVerified.count())
# MAGIC println("DIM_DATE: " + dimDateVerified.count())
# MAGIC println("FACT_ORDER: " + factOrderVerified.count())
# MAGIC
# MAGIC println("\n========== FACT_ORDER SCHEMA ==========")
# MAGIC factOrderVerified.printSchema()
# MAGIC
# MAGIC println("\n========== DIM_CUSTOMER SCHEMA ==========")
# MAGIC dimCustomerVerified.printSchema()
# MAGIC
# MAGIC println("\n========== DIM_PRODUCT SCHEMA ==========")
# MAGIC dimProductVerified.printSchema()
# MAGIC
# MAGIC println("\n========== DIM_DATE SCHEMA ==========")
# MAGIC dimDateVerified.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check available catalogs

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog(), current_schema();

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create the Gold schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS adlsdatabricks2_workspace.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN adlsdatabricks2_workspace;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Register DIM_CUSTOMER

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS
# MAGIC adlsdatabricks2_workspace.gold.dim_customer
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://adlsdatabrics2c@adlsdatabricks2.dfs.core.windows.net/nexus/batch/gold/dim_customer';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM adlsdatabricks2_workspace.gold.dim_customer
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Register DIM_PRODUCT

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS
# MAGIC adlsdatabricks2_workspace.gold.dim_product
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://adlsdatabrics2c@adlsdatabricks2.dfs.core.windows.net/nexus/batch/gold/dim_product';

# COMMAND ----------

# MAGIC %md
# MAGIC #### Register DIM_DATE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS
# MAGIC adlsdatabricks2_workspace.gold.dim_date
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://adlsdatabrics2c@adlsdatabricks2.dfs.core.windows.net/nexus/batch/gold/dim_date';

# COMMAND ----------

# MAGIC %md
# MAGIC #### Register FACT_ORDER

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS
# MAGIC adlsdatabricks2_workspace.gold.fact_order
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://adlsdatabrics2c@adlsdatabricks2.dfs.core.windows.net/nexus/batch/gold/fact_order';
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Verify all catalog tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN adlsdatabricks2_workspace.gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Verify catalog table counts

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'DIM_CUSTOMER' AS table_name, COUNT(*) AS total_records
# MAGIC FROM adlsdatabricks2_workspace.gold.dim_customer
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'DIM_PRODUCT', COUNT(*)
# MAGIC FROM adlsdatabricks2_workspace.gold.dim_product
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'DIM_DATE', COUNT(*)
# MAGIC FROM adlsdatabricks2_workspace.gold.dim_date
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'FACT_ORDER', COUNT(*)
# MAGIC FROM adlsdatabricks2_workspace.gold.fact_order;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dashboard consumption layer

# COMMAND ----------

# MAGIC %md
# MAGIC ### order_status_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW
# MAGIC adlsdatabricks2_workspace.gold.vw_order_status_summary
# MAGIC AS
# MAGIC SELECT
# MAGIC     order_status,
# MAGIC     COUNT(*) AS total_orders
# MAGIC FROM adlsdatabricks2_workspace.gold.fact_order
# MAGIC GROUP BY order_status;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM adlsdatabricks2_workspace.gold.vw_order_status_summary
# MAGIC ORDER BY total_orders DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Product category analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW
# MAGIC adlsdatabricks2_workspace.gold.vw_product_category_summary
# MAGIC AS
# MAGIC SELECT
# MAGIC     p.category,
# MAGIC     SUM(f.quantity) AS total_quantity,
# MAGIC     COUNT(DISTINCT f.order_id) AS total_orders
# MAGIC FROM adlsdatabricks2_workspace.gold.fact_order f
# MAGIC JOIN adlsdatabricks2_workspace.gold.dim_product p
# MAGIC     ON f.product_key = p.product_key
# MAGIC GROUP BY p.category;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM adlsdatabricks2_workspace.gold.vw_product_category_summary
# MAGIC ORDER BY total_quantity DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sales channel analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW
# MAGIC adlsdatabricks2_workspace.gold.vw_sales_channel_summary
# MAGIC AS
# MAGIC SELECT
# MAGIC     COALESCE(sales_channel, 'LEGACY/UNKNOWN') AS sales_channel,
# MAGIC     COUNT(*) AS total_orders,
# MAGIC     SUM(quantity) AS total_quantity
# MAGIC FROM adlsdatabricks2_workspace.gold.fact_order
# MAGIC GROUP BY COALESCE(sales_channel, 'LEGACY/UNKNOWN');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM adlsdatabricks2_workspace.gold.vw_sales_channel_summary
# MAGIC ORDER BY total_orders DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Monthly order trend

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW
# MAGIC adlsdatabricks2_workspace.gold.vw_monthly_order_trend
# MAGIC AS
# MAGIC SELECT
# MAGIC     d.year,
# MAGIC     d.month,
# MAGIC     d.month_name,
# MAGIC     COUNT(f.order_id) AS total_orders,
# MAGIC     SUM(f.quantity) AS total_quantity
# MAGIC FROM adlsdatabricks2_workspace.gold.fact_order f
# MAGIC JOIN adlsdatabricks2_workspace.gold.dim_date d
# MAGIC     ON f.date_key = d.date_key
# MAGIC GROUP BY
# MAGIC     d.year,
# MAGIC     d.month,
# MAGIC     d.month_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM adlsdatabricks2_workspace.gold.vw_monthly_order_trend
# MAGIC ORDER BY year, month;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Top 10 customers

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW
# MAGIC adlsdatabricks2_workspace.gold.vw_top_customers
# MAGIC AS
# MAGIC SELECT
# MAGIC     c.customer_key,
# MAGIC     c.customer_id,
# MAGIC     c.customer_name,
# MAGIC     c.city,
# MAGIC     COUNT(DISTINCT f.order_id) AS total_orders,
# MAGIC     SUM(f.quantity) AS total_quantity
# MAGIC FROM adlsdatabricks2_workspace.gold.fact_order f
# MAGIC JOIN adlsdatabricks2_workspace.gold.dim_customer c
# MAGIC     ON f.customer_key = c.customer_key
# MAGIC GROUP BY
# MAGIC     c.customer_key,
# MAGIC     c.customer_id,
# MAGIC     c.customer_name,
# MAGIC     c.city
# MAGIC ORDER BY
# MAGIC     total_quantity DESC,
# MAGIC     total_orders DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM adlsdatabricks2_workspace.gold.vw_top_customers
# MAGIC ORDER BY total_quantity DESC, total_orders DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Verify all five views

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VIEWS IN adlsdatabricks2_workspace.gold;