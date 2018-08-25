package pt.necosta.sparkx

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

case class AirlineDelays(AirlineDesc: String,
                         DeparturesWithDelayPerc: Option[Double])

object DataAnalysis extends WithSpark {

  def getDelaysByAirline: Dataset[OutputRecord] => Dataset[AirlineDelays] = {
    import spark.implicits._

    ds =>
      ds.withColumn("IsDelayed", isDelayedUdf(col("DepartureDelay")))
        .groupBy($"AirlineDesc")
        .agg(
          count("DepartureDelay").alias("DeparturesCount"),
          sum("IsDelayed").alias("DeparturesWithDelay"),
          (sum("IsDelayed") / count("DepartureDelay")).alias(
            "DeparturesWithDelayPerc")
        )
        .select("AirlineDesc", "DeparturesWithDelayPerc")
        .as[AirlineDelays]
  }

  private val isDelayedUdf = udf((delay: Int) => if (delay > 0) 1 else 0)
}
