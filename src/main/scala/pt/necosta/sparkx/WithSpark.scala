package pt.necosta.sparkx

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

trait WithSpark {
  implicit val spark: SparkSession = SparkSession.builder().getOrCreate()

  implicit val sc: SparkContext = spark.sparkContext
}
