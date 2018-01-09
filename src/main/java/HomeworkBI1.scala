import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object HomeworkBI1 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark FP-Growth Implementation")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._

    val customSchema = StructType(Array(
      StructField("InvoiceNo", StringType, true),
      StructField("StockCode", StringType, true),
      StructField("Description", StringType, true),
      StructField("Quantity", IntegerType, true),
      StructField("InvoiceDate", TimestampType, true),
      StructField("UnitPrice", DoubleType, true),
      StructField("CustomerID", IntegerType, true),
      StructField("Country", StringType, true)))

    def readExcel(file: String): DataFrame = spark.read
      .format("com.crealytics.spark.excel")
      .option("useHeader", "true") // Required
      .option("treatEmptyValuesAsNulls", "true") // Optional, default: true
      .option("inferSchema", "false") // Optional, default: false
      .option("addColorColumns", "false") // Optional, default: false
      .option("maxRowsInMemory", 20) // Optional, default None. If set, uses a streaming reader which can help with big files
      .option("excerptSize", 10) // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
      .option("path", file)
      .schema(customSchema)
      .load()

    val data = readExcel("src/main/resources/Online Retail.xlsx")

    // Preprocessing
    // TODO: make set of stockcodes for every transaction-number

    val transactions = data
      .select($"InvoiceNo", $"StockCode")
      .groupBy($"InvoiceNo")
      .agg(
        collect_list("StockCode") as "StockCode"
      )
      .show()

    // Mining
    // TODO: Frequent itemset algorithm

    // Frequent 1-itemset
    val groupedDataByItem = data
      .select("InvoiceNo", "StockCode")
      .groupBy("StockCode")
      .count()
      .filter($"count" >= 2)
      .show()

//    val fpg = new FPGrowth()
//      .setMinSupport(0.2)
//      .setNumPartitions(10)
//    val model = fpg.run()
//
//    model.freqItemsets.collect().foreach { itemset =>
//      println(s"${itemset.items.mkString("[", ",", "]")},${itemset.freq}")
//    }

    // Postprocessing
    // TODO: stockcodes to description & print description of frequent products
  }
}
