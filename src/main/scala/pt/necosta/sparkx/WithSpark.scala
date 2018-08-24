package pt.necosta.sparkx

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

trait WithSpark {
  implicit lazy val spark: SparkSession = SparkSession.builder().getOrCreate()

  implicit lazy val sc: SparkContext = spark.sparkContext
}
