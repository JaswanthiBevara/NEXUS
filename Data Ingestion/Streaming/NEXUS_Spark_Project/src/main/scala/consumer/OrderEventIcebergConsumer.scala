
package consumer



import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

import java.sql.Timestamp

object OrderEventIcebergConsumer {

  def main(args: Array[String]): Unit = {

    // --------------------------------------------------
    // 1. CONFIGURATION
    // --------------------------------------------------

    val kafkaBootstrapServers =
      sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

    val kafkaTopic =
      sys.env.getOrElse("KAFKA_TOPIC", "nexus-order-events")

    val checkpointLocation =
      sys.env.getOrElse(
        "CHECKPOINT_LOCATION",
        "/data/checkpoint/order-events-iceberg"
      )

    val watermarkDelay =
      sys.env.getOrElse("WATERMARK_DELAY", "20 minutes")

   

    // --------------------------------------------------
    // 2. WATERMARK DELAY CONVERSION
    // --------------------------------------------------

    def parseDurationToMillis(value: String): Long = {

      val parts = value.trim.toLowerCase.split("\\s+")

      require(
        parts.length == 2,
        s"Invalid WATERMARK_DELAY: $value"
      )

      val amount = parts(0).toLong

      parts(1) match {
        case "second" | "seconds" =>
          amount * 1000L

        case "minute" | "minutes" =>
          amount * 60L * 1000L

        case "hour" | "hours" =>
          amount * 60L * 60L * 1000L

        case _ =>
          throw new IllegalArgumentException(
            s"Unsupported WATERMARK_DELAY unit: ${parts(1)}"
          )
      }
    }

    val watermarkDelayMillis =
      parseDurationToMillis(watermarkDelay)

    
    // --------------------------------------------------
    // 3. SPARK SESSION - ICEBERG CATALOG
    // --------------------------------------------------

    val catalogName = "nexus_catalog"

    val icebergWarehouse = sys.env.getOrElse(
      "ICEBERG_WAREHOUSE",
      "/data/iceberg/warehouse"
    )

    val spark = SparkSession.builder()
      .appName("NEXUS Order Event Iceberg Consumer")
      .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
      )
      .config(
        s"spark.sql.catalog.$catalogName",
        "org.apache.iceberg.spark.SparkCatalog"
      )
      .config(s"spark.sql.catalog.$catalogName.type", "hadoop")
      .config(s"spark.sql.catalog.$catalogName.warehouse", icebergWarehouse)
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println(s"Iceberg catalog: $catalogName")
    println(s"Iceberg warehouse: $icebergWarehouse")


    // --------------------------------------------------
    // 4. REFERENCE DATA: CUSTOMER AND PRODUCT MASTER
    // --------------------------------------------------

    val referencePath = sys.env.getOrElse(
      "REFERENCE_DATA_PATH",
      "/data/reference"
    )

    val customerIds = spark.read
      .option("header", "true")
      .option("mode", "FAILFAST")
      .csv(s"$referencePath/customers.csv")
      .select(trim(col("customer_id")).as("customer_id"))
      .filter(col("customer_id").isNotNull)
      .distinct()
      .collect()
      .map(_.getString(0))
      .toSet

    val productIds = spark.read
      .option("header", "true")
      .option("mode", "FAILFAST")
      .csv(s"$referencePath/products.csv")
      .select(trim(col("product_id")).as("product_id"))
      .filter(col("product_id").isNotNull)
      .distinct()
      .collect()
      .map(_.getString(0))
      .toSet

    require(
      customerIds.nonEmpty,
      "Customer reference data is empty"
    )

    require(
      productIds.nonEmpty,
      "Product reference data is empty"
    )

    println(s"Loaded ${customerIds.size} customer IDs")
    println(s"Loaded ${productIds.size} product IDs")

    // --------------------------------------------------
    // 5. EVENT SCHEMA
    // --------------------------------------------------

    val eventSchema = StructType(
      Seq(
        StructField("event_id", StringType, nullable = true),
        StructField("order_id", StringType, nullable = true),
        StructField("customer_id", StringType, nullable = true),
        StructField("product_id", StringType, nullable = true),
        StructField("quantity", IntegerType, nullable = true),
        StructField("unit_price", DoubleType, nullable = true),
        StructField("event_type", StringType, nullable = true),
        StructField("event_time", TimestampType, nullable = true),
        StructField("ingest_time", TimestampType, nullable = true),
        StructField("sales_channel", StringType, nullable = true),
        StructField("discount_amount", DoubleType, nullable = true),
        StructField("_corrupt_record", StringType, nullable = true)
      )
    )

    
    // --------------------------------------------------
    // 6. ICEBERG CONTROL STATE INITIALIZATION
    // --------------------------------------------------

    val stateTable = s"$catalogName.streaming.watermark_state"

    spark.sql(
      s"CREATE NAMESPACE IF NOT EXISTS $catalogName.streaming"
    )

    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS $stateTable (
         |  state_key INT,
         |  last_batch_id BIGINT,
         |  max_event_time TIMESTAMP
         |)
         |USING iceberg
         |""".stripMargin
    )

    val existingStateCount = spark.table(stateTable).count()

    if (existingStateCount == 0) {

      spark.sql(
        s"""
           |INSERT INTO $stateTable
           |VALUES (1, -1, CAST(NULL AS TIMESTAMP))
           |""".stripMargin
      )

      println("Initialized Iceberg watermark control state")
    }


    // --------------------------------------------------
    // 7. KAFKA STREAMING SOURCE
    // --------------------------------------------------

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

     val parsedEvents = kafkaDF
      .select(
        col("value").cast("string").as("raw_json"),
        col("topic").as("kafka_topic"),
        col("partition").as("kafka_partition"),
        col("offset").as("kafka_offset"),
        col("timestamp").as("kafka_timestamp")
      )
      .withColumn(
        "kafka_record_id",
        concat(
          col("kafka_topic"),
          lit(":"),
          col("kafka_partition").cast("string"),
          lit(":"),
          col("kafka_offset").cast("string")
        )
      )
      .withColumn(
        "parsed",
        from_json(
          col("raw_json"),
          eventSchema,
          Map(
            "mode" -> "PERMISSIVE",
            "columnNameOfCorruptRecord" -> "_corrupt_record",
            "timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]"
          )
        )
      )
      .select(
  col("raw_json"),
  col("kafka_topic"),
  col("kafka_partition"),
  col("kafka_offset"),
  col("kafka_timestamp"),
  col("kafka_record_id"),
  col("parsed.*")
)
      .withColumn(
        "parse_failed",
        col("event_id").isNull ||
        col("_corrupt_record").isNotNull
      )

    // --------------------------------------------------
    // 8. EVENT VALIDATION
    // --------------------------------------------------

    val validatedEvents = parsedEvents
      .withColumn(
        "reject_reason",
        concat_ws(
          "; ",

          when(
            col("parse_failed"),
            lit("Invalid JSON or missing event_id")
          ),

          when(
            col("event_id").isNull ||
            !col("event_id").rlike("^EVT\\d{6}$"),
            lit("Invalid event_id")
          ),

          when(
            col("order_id").isNull,
            lit("Missing order_id")
          ),

          when(
            col("order_id").isNotNull &&
            !col("order_id").rlike("^ORD\\d{6}$"),
            lit("Invalid order_id format")
          ),

          when(
            col("customer_id").isNotNull &&
            !col("customer_id").rlike("^CUST\\d{3}$"),
            lit("Invalid customer_id format")
          ),

          when(
            col("customer_id").isNotNull &&
            !col("customer_id").isin(customerIds.toSeq: _*),
            concat(
              lit("Unknown customer_id: "),
              col("customer_id")
            )
          ),

          when(
            col("product_id").isNotNull &&
            !col("product_id").rlike("^PRD\\d{3}$"),
            lit("Invalid product_id format")
          ),

          when(
            col("product_id").isNotNull &&
            !col("product_id").isin(productIds.toSeq: _*),
            concat(
              lit("Unknown product_id: "),
              col("product_id")
            )
          ),

          when(
            col("event_type").isNull ||
            !col("event_type").isin(
              "ORDER_CREATED",
              "ORDER_UPDATED",
              "ORDER_CANCELLED"
            ),
            lit("Invalid event_type")
          ),

          when(
            col("event_time").isNull,
            lit("Missing or invalid event_time")
          ),

          when(
            col("ingest_time").isNull,
            lit("Missing or invalid ingest_time")
          ),

          when(
            col("quantity").isNull ||
            col("quantity") < 1,
            lit("Quantity must be at least 1")
          ),

          when(
            col("unit_price").isNull ||
            col("unit_price") <= 0,
            lit("Unit price must be greater than 0")
          ),

          when(
            col("discount_amount") < 0,
            lit("Discount amount cannot be negative")
          ),

          when(
            col("event_time").isNotNull &&
            col("ingest_time").isNotNull &&
            col("event_time") >
              col("ingest_time") + expr("INTERVAL 5 MINUTES"),
            lit("Future event_time exceeds allowed clock skew")
          )
        )
      )

    // --------------------------------------------------
    // 9. MICRO-BATCH PROCESSING
    // --------------------------------------------------

    val query = validatedEvents.withWatermark("event_time", watermarkDelay).writeStream
      .queryName("nexus-order-events")
      .option("checkpointLocation", checkpointLocation)
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

        // Use the SparkSession associated with this micro-batch DataFrame.
        val batchSpark = batchDF.sparkSession

        println()
        println("==============================================")
        println(s"MICRO-BATCH $batchId")
        println("==============================================")

        if (!batchDF.isEmpty) {

          // ------------------------------------------
          // 9.1 READ PREVIOUS CONTROL STATE
          // ------------------------------------------

          
         val stateDF = batchSpark.table(stateTable)
           .filter(col("state_key") === 1)

         val stateRow = stateDF.first()


          val lastCompletedBatchId =
            stateRow.getAs[Long]("last_batch_id")

          val previousMaxEventTime =
            stateRow.getAs[Timestamp]("max_event_time")

          if (batchId <= lastCompletedBatchId) {

            println(
              s"Skipping already completed batch $batchId"
            )

          } else {

            // --------------------------------------
            // 9.2 PERSIST MICRO-BATCH
            // --------------------------------------

            batchDF.persist()

            try {

              val rawCount = batchDF.count()

              println(s"Raw events: $rawCount")

                         // ------------------------------------
            // 9.3 RAW ICEBERG WRITE
            // ------------------------------------

            val rawTable =
              s"$catalogName.streaming.raw_order_events"

            batchDF.createOrReplaceTempView("raw_events_source")

            batchSpark.sql(
              s"CREATE TABLE IF NOT EXISTS $rawTable (${batchDF.schema.toDDL}) USING iceberg"
            )

            batchSpark.sql(
              s"""
                 |MERGE INTO $rawTable AS target
                 |USING raw_events_source AS source
                 |ON target.kafka_record_id = source.kafka_record_id
                 |WHEN NOT MATCHED THEN INSERT *
                 |""".stripMargin
            )

              // ------------------------------------
              // 9.4 INVALID EVENTS
              // ------------------------------------

              val rejectedEvents = batchDF
                .filter(length(col("reject_reason")) > 0)

              val rejectedCount = rejectedEvents.count()

              println(
                s"Rejected events: $rejectedCount"
              )

              if (rejectedCount > 0) {

                val rejectedTable =
                  s"$catalogName.streaming.rejected_order_events"

                rejectedEvents.createOrReplaceTempView(
                  "rejected_events_source"
                )

                batchSpark.sql(
                  s"CREATE TABLE IF NOT EXISTS $rejectedTable (${rejectedEvents.schema.toDDL}) USING iceberg"
                )

                batchSpark.sql(
                  s"""
                     |MERGE INTO $rejectedTable AS target
                     |USING rejected_events_source AS source
                     |ON target.kafka_record_id = source.kafka_record_id
                     |WHEN NOT MATCHED THEN INSERT *
                     |""".stripMargin
                )
              }

              // ------------------------------------
              // 9.5 VALID EVENTS
              // ------------------------------------

              val validEvents = batchDF
                .filter(
                  col("reject_reason").isNull ||
                  length(col("reject_reason")) === 0
                )
                .drop("reject_reason")

              // ------------------------------------
              // 9.6 LATE EVENT CLASSIFICATION
              // ------------------------------------

              val eventsWithArrivalDelay =
                validEvents.withColumn(
                  "_arrival_delay_seconds",
                  col("ingest_time").cast("long") -
                  col("event_time").cast("long")
                )

              val lateEvents = eventsWithArrivalDelay
                .filter(
                  col("_arrival_delay_seconds") >
                  lit(watermarkDelayMillis / 1000L)
                )
                .drop("_arrival_delay_seconds")

              val lateCount = lateEvents.count()

              println(s"Late events: $lateCount")

              if (lateCount > 0) {

                val lateTable =
                  s"$catalogName.streaming.late_order_events"

                lateEvents.createOrReplaceTempView(
                  "late_events_source"
                )

                batchSpark.sql(
                  s"CREATE TABLE IF NOT EXISTS $lateTable (${lateEvents.schema.toDDL}) USING iceberg"
                )

                batchSpark.sql(
                  s"""
                     |MERGE INTO $lateTable AS target
                     |USING late_events_source AS source
                     |ON target.kafka_record_id = source.kafka_record_id
                     |WHEN NOT MATCHED THEN INSERT *
                     |""".stripMargin
                )
              }

              // ------------------------------------
              // 9.7 ON-TIME EVENTS
              // ------------------------------------

              val onTimeEvents = eventsWithArrivalDelay
                .filter(
                  col("_arrival_delay_seconds") <=
                  lit(watermarkDelayMillis / 1000L)
                )
                .drop("_arrival_delay_seconds")

              // ------------------------------------
              // 9.8 DUPLICATE EVENT HANDLING
              // ------------------------------------
		val dedupWindow = Window
  	.partitionBy("event_id")
  	.orderBy(
   	col("ingest_time").desc,
    	col("kafka_timestamp").desc,
    	col("kafka_partition").desc,
    	col("kafka_offset").desc
  	)

              val deduplicatedEvents = onTimeEvents
                .withColumn(
                  "_row_number",
                  row_number().over(dedupWindow)
                )
                .filter(col("_row_number") === 1)
                .drop("_row_number")

              // ------------------------------------
              // 9.9 PREPARE CURATED EVENTS
              // ------------------------------------

              val curatedEvents = deduplicatedEvents
                .drop(
                  "raw_json",
                  "parse_failed",
                  "_corrupt_record"
                )

              val curatedCount = curatedEvents.count()

              println(
                s"Curated candidates: $curatedCount"
              )

                           // ------------------------------------
             // 9.10 CURATED ICEBERG MERGE
             // ------------------------------------

             if (curatedCount > 0) {

               val curatedTable =
                 s"$catalogName.streaming.curated_order_events"

               curatedEvents.createOrReplaceTempView(
                 "curated_events_source"
               )

               batchSpark.sql(
                 s"CREATE TABLE IF NOT EXISTS $curatedTable (${curatedEvents.schema.toDDL}) USING iceberg"
               )

               
batchSpark.sql(
  s"""
     |MERGE INTO $curatedTable AS target
     |USING curated_events_source AS source
     |ON target.event_id = source.event_id
     |
     |WHEN MATCHED AND (
     |  source.ingest_time > target.ingest_time
     |  OR (
     |    source.ingest_time = target.ingest_time
     |    AND source.kafka_timestamp > target.kafka_timestamp
     |  )
     |  OR (
     |    source.ingest_time = target.ingest_time
     |    AND source.kafka_timestamp = target.kafka_timestamp
     |    AND source.kafka_partition > target.kafka_partition
     |  )
     |  OR (
     |    source.ingest_time = target.ingest_time
     |    AND source.kafka_timestamp = target.kafka_timestamp
     |    AND source.kafka_partition = target.kafka_partition
     |    AND source.kafka_offset > target.kafka_offset
     |  )
     |) THEN UPDATE SET *
     |
     |WHEN NOT MATCHED THEN INSERT *
     |""".stripMargin
)

             }

              // ------------------------------------
              // 9.11 UPDATE WATERMARK CONTROL STATE
              // ------------------------------------

              val batchMaxEventTime =
                validEvents
                  .agg(max(col("event_time")))
                  .first()
                  .getAs[Timestamp](0)

              val newMaxEventTime =
                if (batchMaxEventTime == null) {
                  previousMaxEventTime
                } else if (previousMaxEventTime == null) {
                  batchMaxEventTime
                } else if (
                  batchMaxEventTime.after(previousMaxEventTime)
                ) {
                  batchMaxEventTime
                } else {
                  previousMaxEventTime
                }

              println(
                s"Previous maximum event time: $previousMaxEventTime"
              )

              println(
                s"New maximum event time: $newMaxEventTime"
              )

                           // ------------------------------------
             // ICEBERG WATERMARK STATE MERGE
             // ------------------------------------

             val stateUpdateSchema = StructType(
               Seq(
                 StructField("state_key", IntegerType, nullable = false),
                 StructField("last_batch_id", LongType, nullable = false),
                 StructField("max_event_time", TimestampType, nullable = true)
               )
             )

             val updatedState = batchSpark.createDataFrame(
               batchSpark.sparkContext.parallelize(
                 Seq(
                   Row(
                     1,
                     batchId,
                     newMaxEventTime
                   )
                 )
               ),
               stateUpdateSchema
             )

             updatedState.createOrReplaceTempView(
               "watermark_state_source"
             )

             batchSpark.sql(
               s"""
                  |MERGE INTO $stateTable AS target
                  |USING watermark_state_source AS source
                  |ON target.state_key = source.state_key
                  |WHEN MATCHED THEN UPDATE SET
                  |  last_batch_id = source.last_batch_id,
                  |  max_event_time = source.max_event_time
                  |WHEN NOT MATCHED THEN INSERT (
                  |  state_key,
                  |  last_batch_id,
                  |  max_event_time
                  |)
                  |VALUES (
                  |  source.state_key,
                  |  source.last_batch_id,
                  |  source.max_event_time
                  |)
                  |""".stripMargin
             
             	)

               println(
               s"Completed batch: $batchId"
             )

            } finally {

              batchDF.unpersist()
            }
          }
        }
      }
      .start()

    // --------------------------------------------------
    // 10. KEEP STREAMING APPLICATION RUNNING
    // --------------------------------------------------

    query.awaitTermination()
  }
}

