package analytics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object NexusAnalytics {

  def main(args: Array[String]): Unit = {

    val catalogName = "nexus_catalog"

    val warehouse = sys.env.getOrElse(
      "ICEBERG_WAREHOUSE",
      "/data/iceberg/warehouse"
    )

    val curatedTable =
      s"$catalogName.streaming.curated_order_events"

    val spark = SparkSession.builder()
      .appName("NEXUS Business Analytics")
      .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
      )
      .config(
        s"spark.sql.catalog.$catalogName",
        "org.apache.iceberg.spark.SparkCatalog"
      )
      .config(
        s"spark.sql.catalog.$catalogName.type",
        "hadoop"
      )
      .config(
        s"spark.sql.catalog.$catalogName.warehouse",
        warehouse
      )
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    def section(name: String): Unit = {
      println()
      println("=" * 75)
      println(s"  $name")
      println("=" * 75)
    }

    try {

      println()
      println("=" * 75)
      println("             NEXUS BUSINESS ANALYTICS")
      println("=" * 75)
      println("Data source: Curated Iceberg streaming table")
      println("Mode       : READ ONLY")

      // 1. LOAD CURATED DATA

      val curated = spark.table(curatedTable)

      val requiredColumns = Seq(
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "unit_price",
        "discount_amount",
        "event_type",
        "event_time",
        "ingest_time"
      )

      val missingColumns =
        requiredColumns.diff(curated.columns.toSeq)

      require(
        missingColumns.isEmpty,
        s"Missing curated columns: ${missingColumns.mkString(", ")}"
      )

      // 2. PREPARE CURRENT ORDER STATE
      // Keep the latest event for each order so updates
      // are not counted as separate orders.

      val orderWindow = Window
        .partitionBy("order_id")
        .orderBy(
          col("ingest_time").desc,
          col("event_time").desc
        )

      val currentOrders = curated
        .filter(col("order_id").isNotNull)
        .withColumn(
          "order_rank",
          row_number().over(orderWindow)
        )
        .filter(col("order_rank") === 1)
        .filter(col("event_type") =!= "ORDER_CANCELLED")
        .withColumn(
          "net_revenue",
          (
            col("quantity") * col("unit_price") -
            coalesce(col("discount_amount"), lit(0.0))
          ).cast("double")
        )

      // 3. TOTAL SALES REVENUE

      section("1. TOTAL SALES REVENUE")

      currentOrders
        .agg(
          round(sum("net_revenue"), 2)
            .as("total_sales_revenue")
        )
        .show(false)

      // 4. TOTAL ORDERS

      section("2. TOTAL ORDERS")

      currentOrders
        .agg(
          countDistinct("order_id")
            .as("total_orders")
        )
        .show(false)

      // 5. TOP 10 BEST-SELLING PRODUCTS

      section("3. TOP 10 BEST-SELLING PRODUCTS")

      currentOrders
        .groupBy("product_id")
        .agg(
          sum("quantity").as("total_quantity_sold"),
          round(sum("net_revenue"), 2)
            .as("revenue_contribution")
        )
        .orderBy(
          col("total_quantity_sold").desc,
          col("revenue_contribution").desc
        )
        .limit(10)
        .show(false)

      // 6. MONTHLY SALES TREND

      section("4. MONTHLY SALES TREND")

      currentOrders
        .withColumn(
          "sales_month",
          date_format(col("event_time"), "yyyy-MM")
        )
        .groupBy("sales_month")
        .agg(
          round(sum("net_revenue"), 2)
            .as("monthly_revenue")
        )
        .orderBy("sales_month")
        .show(false)

      // 7. TOP 10 CUSTOMERS

      section("5. TOP 10 CUSTOMERS")

      currentOrders
        .groupBy("customer_id")
        .agg(
          countDistinct("order_id")
            .as("total_orders"),
          round(sum("net_revenue"), 2)
            .as("total_revenue")
        )
        .orderBy(col("total_revenue").desc)
        .limit(10)
        .show(false)

      // COMPLETION

      section("BUSINESS ANALYTICS COMPLETED")

      println("All five business analytics reports generated.")
      println("Source: Curated streaming data in Apache Iceberg.")
      println("No production table was modified.")

      println()
      println(
        "Note: Revenue assumes discount_amount is the total " +
        "discount for an order, not a per-unit discount."
      )

    } finally {
      spark.stop()
    }
  }
}