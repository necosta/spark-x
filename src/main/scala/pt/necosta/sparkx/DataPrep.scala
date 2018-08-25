package pt.necosta.sparkx

import java.io.PrintWriter

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, IntegerType}

object DataPrep {

  def init(sourceFolder: String): DataPrep = {
    new DataPrep(sourceFolder)
  }
}

class DataPrep(sourceFolder: String) extends WithSpark {
  import spark.implicits._

  private val SOURCE_URL = "https://www.transtats.bts.gov/Download_Lookup.asp"

  val baseTableName = "BASE"
  val airlineTableName = "L_AIRLINE_ID"
  val airportTableName = "L_AIRPORT_ID"
  val tables = Map(baseTableName -> "sourceData.csv",
                   airlineTableName -> "airlineData.csv",
                   airportTableName -> "airportData.csv")

  def getSourceDs: Dataset[InputRecord] = {
    val sourceFilePath = s"$sourceFolder/${tables(baseTableName)}"
    csvToDataFrame(sourceFilePath)
      .as[InputRecord]
  }

  def getLookup(sourceFilePath: String): Dataset[LookupRecord] = {
    csvToDataFrame(sourceFilePath)
      .as[LookupRecord]
  }

  def buildFinalDs(): Dataset[InputRecord] => Dataset[OutputRecord] = {
    val JOIN_BROADCAST_HINT = "broadcast"

    val airlineFilePath = s"$sourceFolder/${tables(airlineTableName)}"
    val airportFilePath = s"$sourceFolder/${tables(airportTableName)}"

    val airlineDataset = getLookup(airlineFilePath)
      .withColumnRenamed("Code", "AirlineCode")
      .withColumnRenamed("Description", "AirlineDesc")
    val airportDataset = getLookup(airportFilePath)
      .withColumnRenamed("Code", "AirportCode")
      .withColumnRenamed("Description", "AirportDesc")

    // ToDo: Assume all lookups have a match vs apply left join
    ds =>
      {
        ds.join(airlineDataset.hint(JOIN_BROADCAST_HINT))
          .where($"AIRLINE_ID" === $"AirlineCode")
          .join(
            airportDataset
              .hint(JOIN_BROADCAST_HINT)
              .withColumnRenamed("AirportCode", "OriginAirportCode")
              .withColumnRenamed("AirportDesc", "OriginAirportDesc"))
          .where($"ORIGIN_AIRPORT_ID" === $"OriginAirportCode")
          .join(
            airportDataset
              .hint(JOIN_BROADCAST_HINT)
              .withColumnRenamed("AirportCode", "DestAirportCode")
              .withColumnRenamed("AirportDesc", "DestAirportDesc"))
          .where($"DEST_AIRPORT_ID" === $"DestAirportCode")
          .withColumn("DepartureDelay", col("DEP_DELAY").cast(IntegerType))
          .withColumn("ArrivalDelay", col("ARR_DELAY").cast(IntegerType))
          .withColumn("IsCancelled",
                      col("CANCELLED").cast(IntegerType).cast(BooleanType))
          .select("FL_DATE",
                  "AirlineDesc",
                  "FL_NUM",
                  "OriginAirportDesc",
                  "DestAirportDesc",
                  "DEST_CITY_NAME",
                  "DepartureDelay",
                  "ArrivalDelay",
                  "IsCancelled")
          .as[OutputRecord]
      }
  }
  def importLookupTables(): Unit = {
    tables.filter(t => t._1 != baseTableName).foreach(importTable)
  }

  private def importTable(lookupMap: (String, String)): Unit = {
    val content = scala.io.Source
      .fromURL(s"$SOURCE_URL?Lookup=${lookupMap._1}")
      .mkString
    new PrintWriter(s"$sourceFolder/${lookupMap._2}") {
      write(content); close()
    }
  }

  private def csvToDataFrame(sourceFilePath: String): DataFrame = {
    spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(sourceFilePath)
  }
}
