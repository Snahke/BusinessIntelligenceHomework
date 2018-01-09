import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object HomeworkBI1 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark FP-Growth Implementation")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._

    def readExcel(file: String): DataFrame = spark.read
      .format("com.crealytics.spark.excel")
      .option("useHeader", "true") // Required
      .option("treatEmptyValuesAsNulls", "true") // Optional, default: true
      .option("inferSchema", "true") // Optional, default: false
      .option("addColorColumns", "false") // Optional, default: false
      .option("maxRowsInMemory", 20) // Optional, default None. If set, uses a streaming reader which can help with big files
      .option("excerptSize", 10) // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
      .option("path", file)
      .load()

    val data = readExcel("src/main/resources/Online Retail.xlsx")
      .selectExpr("cast(InvoiceNo as int) InvoiceNo",
      "StockCode",
      "Description",
      "cast(Quantity as int) Quantity",
      "InvoiceDate",
      "UnitPrice",
      "cast(CustomerID as int) CustomerID",
      "Country")

    val transactions: Dataset[(Int, String)] = data.map(s => (s.getAs[Int]("InvoiceNo"), s.getAs[String]("StockCode")))
    transactions.show()

    // Preprocessing
    // TODO: make set of stockcodes for every transaction-number

    // val transactions: RDD[Array[String]] = data.map(s => s.trim.split(' '))

    // Mining
    // TODO: Frequent itemset algorithm

    // Postprocessing
    // TODO: stockcodes to description & print description of frequent products
  }
}
