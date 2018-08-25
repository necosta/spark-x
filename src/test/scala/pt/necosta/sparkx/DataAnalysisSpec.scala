package pt.necosta.sparkx

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

class DataAnalysisSpec extends TestConfig {

  "DataAnalysis" should "correctly aggregate airlines by delay percentages" in {

    val ds = importOutputDs()

    val output = ds.transform(DataAnalysis.getDelaysByAirline)

    val orderedRecords = output.orderBy(asc("DeparturesWithDelayPerc"))
    //orderedRecords.show()
    orderedRecords.head.AirlineDesc should be("ExpressJet Airlines Inc.: EV   ")
  }

  "DataAnalysis" should "correctly aggregate airlines by flights to city" in {

    val ds = importOutputDs()

    val city = "Phoenix"
    val output = ds.transform(DataAnalysis.getFlightsByAirlineToCity(city))

    val orderedRecords = output.orderBy(desc("FlightsCount"))
    //orderedRecords.show()
    orderedRecords.head.AirlineDesc should be("Cochise Airlines Inc.: COC     ")
  }

  private def importOutputDs(): Dataset[OutputRecord] = {
    implicit val spark: SparkSession = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val outputFilePath = this.getClass.getResource("/outputData.csv").getPath

    spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", "|") // Necessary hack given the way output is created
      .option("ignoreLeadingWhiteSpace", value = true) // Necessary hack given the way output is created
      .csv(outputFilePath)
      .withColumn("DepartureDelay", col("DepartureDelay").cast(IntegerType))
      .withColumn("ArrivalDelay", col("ArrivalDelay").cast(IntegerType))
      .as[OutputRecord]
  }
}
