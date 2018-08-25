package pt.necosta.sparkx

import org.apache.spark.sql.{DataFrame, Dataset}

object DataPrep {

  def init(sourceFolder: String): DataPrep = {
    new DataPrep(sourceFolder)
  }
}

class DataPrep(sourceFolder: String) extends WithSpark {
  import spark.implicits._

  def getSource(): Dataset[InputRecord] = {
    val sourceFilePath = s"$sourceFolder/sourceData.csv"
    csvToDataFrame(sourceFilePath)
      .as[InputRecord]
  }

  def getLookup(sourceFilePath: String): Dataset[LookupRecord] = {
    csvToDataFrame(sourceFilePath)
      .as[LookupRecord]
  }

  def buildFinalDs(): Dataset[InputRecord] => Dataset[OutputRecord] = {
    val airlineFilePath = s"$sourceFolder/airlineData.csv"
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

  private def csvToDataFrame(sourceFilePath: String): DataFrame = {
    spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(sourceFilePath)
  }
}
