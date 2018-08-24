package pt.necosta.sparkx

import org.apache.spark.sql.SparkSession

object SparkX {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("SparkX")
      .getOrCreate()

    val sourceFolder = sys.env.getOrElse("SPARKX_SOURCE_FOLDER", "/sparkx")
    val targetFolder = sys.env.getOrElse("SPARKX_TARGET_FOLDER", "/sparkx")

    val sourceFilePath = s"$sourceFolder/sourceData.csv"
    val airlineFilePath = s"$sourceFolder/airlineData.csv"
    val targetFilePath = s"$targetFolder/output.parquet"

    Dataflow.withConfig(sourceFilePath, airlineFilePath, targetFilePath)

    spark.stop()
  }
}
