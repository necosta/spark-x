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
  import DataPrep._

  def getDelaysByAirline: Dataset[OutputRecord] => Dataset[AirlineDelays] = {
    import spark.implicits._

    val isDelayedCol = "IsDelayed"
    ds =>
      ds.transform(ignoreCancelled())
        .withColumn(isDelayedCol,
                    when(col("DepartureDelay").isNull, 0)
                      .otherwise(isDelayedUdf(col("DepartureDelay"))))
        .groupBy(airlineKey, airlineDesc)
        .agg(
          (sum(isDelayedCol) / count(isDelayedCol)).alias(
            "DeparturesWithDelayPerc")
        )
        .select(airlineDesc, "DeparturesWithDelayPerc")
        .as[AirlineDelays]
  }

  def getFlightsByAirlineToCity(
      city: String): Dataset[OutputRecord] => Dataset[AirlineFlights] = {
    import spark.implicits._

    ds =>
      ds.transform(ignoreCancelled())
        .filter(_.DestAirportDesc.contains(city))
        .groupBy(airlineKey, airlineDesc)
        .agg(
          count("OP_CARRIER_FL_NUM").alias("FlightsCount")
        )
        .select(airlineDesc, "FlightsCount")
        .as[AirlineFlights]
  }

  def getDelaysByAirlineAndByAirport
    : Dataset[OutputRecord] => Dataset[AirlineAirportDelays] = {
    import spark.implicits._

    ds =>
      ds.transform(ignoreCancelled())
        .withColumn("DelayTime", delayMinUdf(col("ArrivalDelay")))
        .groupBy(col(airlineKey),
                 col(airlineDesc),
                 col(destAirportKey),
                 col(destAirportDesc))
        .agg(
          avg("DelayTime").alias("AvgDelayTime")
        )
        .select(airlineDesc, destAirportDesc, "AvgDelayTime")
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
