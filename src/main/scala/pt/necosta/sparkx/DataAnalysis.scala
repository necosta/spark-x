package pt.necosta.sparkx

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

case class AirlineDelays(AirlineDesc: String,
                         DeparturesWithDelayPerc: Option[Double])

case class AirlineFlights(AirlineDesc: String, FlightsCount: BigInt)

case class AirlineAirportDelays(AirlineDesc: String,
                                DestAirportDesc: String,
                                AvgDelayTime: Option[Double])

object DataAnalysis extends WithSpark {

  def getDelaysByAirline: Dataset[OutputRecord] => Dataset[AirlineDelays] = {
    import spark.implicits._

    val isDelayedCol = "IsDelayed"
    ds =>
      ds.transform(ignoreCancelled())
        .withColumn(isDelayedCol,
                    when(col("DepartureDelay").isNull, 0)
                      .otherwise(isDelayedUdf(col("DepartureDelay"))))
        // ToDo: AirlineID would be more efficient to group by
        .groupBy($"AirlineDesc")
        .agg(
          (sum(isDelayedCol) / count(isDelayedCol)).alias(
            "DeparturesWithDelayPerc")
        )
        .select("AirlineDesc", "DeparturesWithDelayPerc")
        .as[AirlineDelays]
  }

  def getFlightsByAirlineToCity(
      city: String): Dataset[OutputRecord] => Dataset[AirlineFlights] = {
    import spark.implicits._

    ds =>
      ds.transform(ignoreCancelled())
        .filter(r => r.DestAirportDesc.contains(city))
        .groupBy($"AirlineDesc")
        .agg(
          count("OP_CARRIER_FL_NUM").alias("FlightsCount")
        )
        .select("AirlineDesc", "FlightsCount")
        .as[AirlineFlights]
  }

  def getDelaysByAirlineAndByAirport
    : Dataset[OutputRecord] => Dataset[AirlineAirportDelays] = {
    import spark.implicits._

    ds =>
      ds.transform(ignoreCancelled())
        .withColumn("DelayTime", delayMinUdf(col("ArrivalDelay")))
        .groupBy($"AirlineDesc", $"DestAirportDesc")
        .agg(
          avg("DelayTime").alias("AvgDelayTime")
        )
        .select("AirlineDesc", "DestAirportDesc", "AvgDelayTime")
        .as[AirlineAirportDelays]
  }

  private val isDelayedUdf = udf((delay: Int) => if (delay > 0) 1 else 0)

  private val delayMinUdf = udf((delay: Int) => if (delay > 0) delay else 0)

  def ignoreCancelled(): Dataset[OutputRecord] => Dataset[OutputRecord] = {
    // Assume no data is a non-cancelled flight!
    ds =>
      ds.filter(r => !r.IsCancelled.getOrElse(false))
  }
}
