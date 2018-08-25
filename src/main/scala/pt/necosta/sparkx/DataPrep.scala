package pt.necosta.sparkx

import java.io.PrintWriter

import org.apache.spark.sql.{DataFrame, Dataset}

object DataPrep {

  def init(sourceFolder: String): DataPrep = {
    new DataPrep(sourceFolder)
  }
}

class DataPrep(sourceFolder: String) extends WithSpark {
  import spark.implicits._

  val sourceURL = "https://www.transtats.bts.gov/Download_Lookup.asp"

  val baseTableName = "BASE"
  val airlineTableName = "L_AIRLINE_ID"
  val tables = Map(baseTableName -> "sourceData.csv",
                   airlineTableName -> "airlineData.csv")

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
    val airlineFilePath = s"$sourceFolder/${tables(airlineTableName)}"
    val airlineDataset = getLookup(airlineFilePath)
    ds =>
      {
        ds.join(airlineDataset.hint("broadcast"))
          .where($"AIRLINE_ID" === $"Code")
          .withColumnRenamed("Description", "AirlineDescription")
          .select($"FL_DATE", $"AIRLINE_ID", $"AirlineDescription")
          .as[OutputRecord]
      }
  }
  def importLookupTables(): Unit = {
    tables.filter(t => t._1 != baseTableName).foreach(importTable)
  }

  private def importTable(lookupMap: (String, String)): Unit = {
    val content = scala.io.Source
      .fromURL(s"$sourceURL?Lookup=${lookupMap._1}")
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
