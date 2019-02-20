package dataReader
import org.apache.spark.sql.SparkSession

object csvReader {
  def flight_dataframe(summaryFile: String): Unit = {
    System.setProperty("hadoop.home.dir", "E:/Downloads/winutils-master/winutils-master/hadoop-2.7.1")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()


    import spark.implicits._
    val accessKeyId = ""
    val secretAccessKey = ""

    spark.sparkContext.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.s3.awsAccessKeyId", accessKeyId)
    spark.sparkContext.hadoopConfiguration.set("fs.s3.awsSecretAccessKey", secretAccessKey)

    // used to create dataframe with encoding to windows-1252 and delimiter and reading textfile as csv file
    // since headers are provided this can be read as a csv file adding heads as true.
//    val summaryDf = spark.read.option("header", "true").option("charset", "windows-1252").option("delimiter", "¦").csv(summaryFile)

    val flightData2010 = spark.read.option("header", "true").option("inferSchema", "true").csv(summaryFile)
    //shows the dataframe
//    flightData2010.show(5)

    flightData2010.createOrReplaceTempView("flight_data_2010")


    // query data in SQL with explain for showing the planning for each sql and dataframe
    val sqlWay = spark.sql(
      """SELECT DEST_COUNTRY_NAME,count(1) FROM flight_data_2010
        |GROUP BY DEST_COUNTRY_NAME
      """.stripMargin)

    val dataFrameWay = flightData2010.groupBy('DEST_COUNTRY_NAME).count()
//    sqlWay.explain()
//    dataFrameWay.explain()

    //checks whether it's greater than the previous values that have been seen.
    spark.sql("SELECT max(count) from flight_data_2010").take(1)

    import org.apache.spark.sql.functions.max
    flightData2010.select(max("count")).take(1)

    //find top five destination countries in the data
    val maxSql = spark.sql(
      """SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
        |FROM flight_data_2010
        |GROUP BY DEST_COUNTRY_NAME
        |ORDER BY sum(count) DESC
        |LIMIT 5
      """.stripMargin)
//    maxSql.show()

    //find top five destination countries in the data using dataframes
    import org.apache.spark.sql.functions.desc
    flightData2010
      .groupBy("DEST_COUNTRY_NAME")
      .sum("count")
      .withColumnRenamed("sum(count)", "destination_total")
      .sort(desc("destination_total"))
      .limit(5)
      .show()
  }
}
//    used to save data back to s3
//    consumerDf.write.mode("append").format("csv").save("s3a://sidm-spark-processed-data/testData")