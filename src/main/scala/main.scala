import dataReader.csvReader

object main {
  def main(args: Array[String]): Unit = {
    val flightData = "s3://flight-data101/2010-summary.csv"
    val bmiDf = csvReader.flight_dataframe(flightData)
  }
}

