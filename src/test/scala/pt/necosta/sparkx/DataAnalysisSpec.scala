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

  "DataAnalysis" should "correctly aggregate airlines+airports by delay time" in {

    val ds = importOutputDs()

    val output = ds.transform(DataAnalysis.getDelaysByAirlineAndByAirport)

    val orderedRecords = output.orderBy(desc("AvgDelayTime"))
    //orderedRecords.show()
    orderedRecords.head.AirlineDesc should be("Munz Northern Airlines Inc.: XY")
    orderedRecords.head.DestAirportDesc should be(
      "Kahului, HI: Kahului Airport                     ")
    orderedRecords.head.AvgDelayTime should be(72.0)
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
