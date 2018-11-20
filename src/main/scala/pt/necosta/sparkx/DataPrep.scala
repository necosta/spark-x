package pt.necosta.sparkx

import java.io.PrintWriter

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, IntegerType}

object DataPrep {

  def init(sourceFolder: String): DataPrep = {
    new DataPrep(sourceFolder)
  }
}

class DataPrep(sourceFolder: String) extends WithSpark {
  import spark.implicits._

  private val sourceUrl = "https://www.transtats.bts.gov/Download_Lookup.asp"

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

  def getLookupDs(sourceFilePath: String): Dataset[LookupRecord] = {
    csvToDataFrame(sourceFilePath)
      .as[LookupRecord]
  }

  def buildFinalDs(): Dataset[InputRecord] => Dataset[OutputRecord] = {

    val joinHint = "broadcast"
    val joinType = "left_outer"
    val airlineKey = "OP_CARRIER_AIRLINE_ID"
    val originAirportKey = "ORIGIN_AIRPORT_ID"
    val destAirportKey = "DEST_AIRPORT_ID"

    val airlineFilePath = s"$sourceFolder/${tables(airlineTableName)}"
    val airportFilePath = s"$sourceFolder/${tables(airportTableName)}"

    val airlineDataset = getLookupDs(airlineFilePath)
      .withColumnRenamed("Code", airlineKey)
      .withColumnRenamed("Description", "AirlineDesc")
    val airportDataset = getLookupDs(airportFilePath)
      .withColumnRenamed("Code", "AirportCode")
      .withColumnRenamed("Description", "AirportDesc")

    ds =>
      {
        ds.join(airlineDataset.hint(joinHint), Seq(airlineKey), joinType)
          .join(
            airportDataset
              .hint(joinHint)
              .withColumnRenamed("AirportCode", originAirportKey)
              .withColumnRenamed("AirportDesc", "OriginAirportDesc"),
            Seq(originAirportKey),
            joinType
          )
          .join(
            airportDataset
              .hint(joinHint)
              .withColumnRenamed("AirportCode", destAirportKey)
              .withColumnRenamed("AirportDesc", "DestAirportDesc"),
            Seq(destAirportKey),
            joinType
          )
          .transform[OutputRecord](applyDatasetTransform())
      }
  }

  def importLookupTables(): Unit = {
    tables
      .filter(t => t._1 != baseTableName)
      .foreach(importTable)
  }

  def applyDatasetTransform(): Dataset[Row] => Dataset[OutputRecord] = { ds =>
    ds.withColumn("DepartureDelay", col("DEP_DELAY").cast(IntegerType))
      .withColumn("ArrivalDelay", col("ARR_DELAY").cast(IntegerType))
      .withColumn("IsCancelled",
                  col("CANCELLED").cast(IntegerType).cast(BooleanType))
      .select("FL_DATE",
              "AirlineDesc",
              "OP_CARRIER_FL_NUM",
              "OriginAirportDesc",
              "DestAirportDesc",
              "DEST_CITY_NAME",
              "DepartureDelay",
              "ArrivalDelay",
              "IsCancelled")
      .as[OutputRecord]
  }

  private def importTable(lookupMap: (String, String)): Unit = {
    val content = scala.io.Source
      .fromURL(s"$sourceUrl?Lookup=${lookupMap._1}")
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
