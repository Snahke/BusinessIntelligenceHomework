
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.broadcast.Broadcast


object HomeworkBI1 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("Spark FP-Growth Implementation")
      .master("local[2]")
      .getOrCreate()

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
      .option("maxRowsInMemory", 20000) // Optional, default None. If set, uses a streaming reader which can help with big files
      .option("excerptSize", 10) // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
      .option("path", file)
      .schema(customSchema)
      .load()

    val data = readExcel("src/main/resources/OnlineRetail2.xlsx")

    import spark.implicits._

    // Preprocessing
    // TODO: make set of stockcodes for every transaction-number

    val transactions = data
      .select($"InvoiceNo", $"StockCode")
      .groupBy($"InvoiceNo")
      .agg(
        collect_list("StockCode") as "StockCode"
      ).select("StockCode").as[Seq[String]].rdd
    val broadcastestBaskets = spark.sparkContext.broadcast(transactions.collect())

    transactions.toDF().show()

    // Mining
    // TODO: Frequent itemset algorithm
    // Frequent 1-itemset
    val minsup = 3

    val it1 = data
      .select("InvoiceNo", "StockCode")
      .groupBy("StockCode")
      .count()
      .filter($"count" >= 2)


    def iteration(baskets: RDD[Seq[String]], k: Int, minsup: Int, prevRules: Broadcast[Array[Seq[String]]]) = {
      baskets
        .filter(itemset => {
          if (k == 1) {
            true
          }
          else {
            prevRules.value.exists(prevItemSet => prevItemSet.forall(itemset.contains(_)))
          }
        })
        .flatMap(itemset => itemset.combinations(k))
        .map(candidate => Tuple2(candidate, 1))
        .reduceByKey(_ + _)
        .filter(_._2 >= minsup)
    }

    val empty: Broadcast[Array[Seq[String]]] = spark.sparkContext.broadcast(Array(Seq()))

    val test1 = iteration(transactions, 1, minsup, empty)
    val testi1 = spark.sparkContext.broadcast(test1.map(_._1).collect())

    val test2 = iteration(transactions, 2, minsup, testi1)
    val testi2 = spark.sparkContext.broadcast(test2.map(_._1).collect())


    val times1 = System.currentTimeMillis()
    System.out.println(times1)
    val test3 = iteration(transactions, 3, minsup, testi2)
    test3.toDF().show()
    val testi3 = test3.map(_._1).collect()
    val times2 = System.currentTimeMillis()
    System.out.println(times2 - times1)

    // Postprocessing
    // TODO: stockcodes to description & print description of frequent products

    def getDescriptions(itemset: Array[Seq[String]], database: DataFrame) = {
      val relevantData = data
        .select($"StockCode", $"Description")
        .distinct()

      println("These items are frequently bought together:")

      itemset.foreach(frequentItems => {
        val stockCode1 = frequentItems(0)
        val stockCode2 = frequentItems(1)
        val stockCode3 = frequentItems(2)

        val description1 = relevantData.filter(row => row(0) == stockCode1).first()
        val description2 = relevantData.filter(row => row(0) == stockCode2).first()
        val description3 = relevantData.filter(row => row(0) == stockCode3).first()

        println("[" + Console.BLUE + description1(1) + Console.WHITE + ", " + Console.BLUE + description2(1) + Console.WHITE + " and " + Console.BLUE + description3(1) + Console.WHITE + "]")
      })
    }

    getDescriptions(testi3, data)

    //------------------------------------------------------------------------------------------------
    //old code
    //------------------------------------------------------------------------------------------------
//    def generateCandidates(oneItemSet: DataFrame, kItemSet: DataFrame, c: Int): Dataset[String] = {
//      val i = kItemSet.crossJoin(oneItemSet).map(r => r(0).toString + ";" + r(2).toString)
//      i.filter(s => s.split(";").distinct.length == c)
//    }
//
//
//    def validateCandidates(kItemSet: Dataset[String], minsup: Long): Dataset[(String, Int)] = {
//      kItemSet
//        .map(kitem => (kitem,
//          broadcastestBaskets.value.filter(s => kitem.split(";").forall(s.contains))
//            .count(p => true)
//        ))
//        .filter(tup => tup._2 >= minsup)
//    }
//
//    def validateCandidates2(kItemSet: RDD[Seq[Int]], minsup: Long): RDD[(Seq[Int], Int)] = {
//      kItemSet
//        .map(kitem => (kitem,
//          broadcastestBaskets.value.filter(s => kitem.forall(s.contains))
//            .count(p => true)
//        ))
//        .filter(tup => tup._2 >= minsup)
//    }
  }

  case class Row2(s: String)

}