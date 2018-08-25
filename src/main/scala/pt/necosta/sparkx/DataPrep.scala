package pt.necosta.sparkx

import org.apache.spark.sql.{DataFrame, Dataset}
import java.sql.Timestamp

object DataPrep {

  def init(): DataPrep = {
    new DataPrep()
  }
}

class DataPrep extends WithSpark {
  import spark.implicits._

  def getSource(sourceFilePath: String): Dataset[InputRecord] = {
    csvToDataFrame(sourceFilePath)
      .as[InputRecord]
  }

  def getLookup(sourceFilePath: String): Dataset[LookupRecord] = {
    csvToDataFrame(sourceFilePath)
      .as[LookupRecord]
  }

  def getOutput(airlineFilePath: String)
    : Dataset[InputRecord] => Dataset[OutputRecord] = {
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
