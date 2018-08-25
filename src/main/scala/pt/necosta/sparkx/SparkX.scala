package pt.necosta.sparkx

import org.apache.spark.sql.SparkSession

object SparkX {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("SparkX")
      .getOrCreate()

    val sourceFolder = sys.env.getOrElse("SPARKX_SOURCE_FOLDER", "/sparkx")

    val dataflow = Dataflow.withConfig(sourceFolder)

    println("Starting data import")
    dataflow.runImport()

    println("Starting data analysis")
    dataflow.runAnalysis()

    spark.stop()
  }
}
