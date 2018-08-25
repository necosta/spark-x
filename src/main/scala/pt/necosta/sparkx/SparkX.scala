package pt.necosta.sparkx

import org.apache.spark.sql.SparkSession

object SparkX {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("SparkX")
      .getOrCreate()

    val sourceFolder = sys.env.getOrElse("SPARKX_SOURCE_FOLDER", "/sparkx")

    Dataflow
      .withConfig(sourceFolder)
      .start()

    spark.stop()
  }
}
