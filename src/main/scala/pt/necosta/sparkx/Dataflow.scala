package pt.necosta.sparkx

import org.apache.spark.sql.functions._

object Dataflow {

  def withConfig(sourceFolder: String): Dataflow = {
    new Dataflow(sourceFolder)
  }
}

class Dataflow(sourceFolder: String) extends WithSpark {

  private val SAVE_FORMAT = "parquet"
  val dataPrep: DataPrep = DataPrep.init(sourceFolder)
  val outputFile = s"$sourceFolder/output.parquet"

  def runImport(): Unit = {
    // 1 - Import all lookup tables into source folder as csv files
    dataPrep.importLookupTables()
    // 2 - Transform base csv file into dataset
    val sourceDs = dataPrep.getSourceDs
    // 3 - Build final denormalized dataset
    val transformedDs =
      sourceDs.transform(DataPrep.init(sourceFolder).buildFinalDs())
    println(s"Imported file has ${transformedDs.count()} rows")
    // 4 - Persist final dataset as parquet for future Spark jobs
    transformedDs.write.format(SAVE_FORMAT).save(outputFile)
  }

  def runAnalysis(): Unit = {
    import spark.implicits._

    val NUMBER_RECORDS = 3

    val ds = spark.read.parquet(outputFile).as[OutputRecord]

    // Find the Airports with the least delay
    val delaysByAirlineDs = ds.transform(DataAnalysis.getDelaysByAirline)

    val delaysByAirlineRecords = delaysByAirlineDs
      .orderBy(asc("DeparturesWithDelayPerc"))
      .take(NUMBER_RECORDS)
    println(s"\nThe top $NUMBER_RECORDS airports with the least delay are:\n")
    delaysByAirlineRecords.foreach(r =>
      println(
        s"${r.AirlineDesc}: ${"%.2f".format(r.DeparturesWithDelayPerc.get * 100)}% delays"))

    // Which Airlines have the most flights to New York
    // ToDo: Review this logic of looking up by name
    val city = "New York"
    val flightsByAirlineToCityDs =
      ds.transform(DataAnalysis.getFlightsByAirlineToCity(city))

    val flightsByAirlineRecords = flightsByAirlineToCityDs
      .orderBy(desc("FlightsCount"))
      .take(NUMBER_RECORDS)
    println(s"\nThe top $NUMBER_RECORDS airlines that fly to $city are:\n")
    flightsByAirlineRecords.foreach(r =>
      println(s"${r.AirlineDesc}: ${r.FlightsCount} flights"))

    // Which airlines arrive the worst on which airport and by what delay
    val delaysByAirlineAndByAirportDs =
      ds.transform(DataAnalysis.getDelaysByAirlineAndByAirport)
    val delaysByAirlineAndByAirportRecords = delaysByAirlineAndByAirportDs
      .orderBy(desc("AvgDelayTime"))
      .take(NUMBER_RECORDS)
    println(
      s"\nThe top $NUMBER_RECORDS airlines/airports combinations by delay time:\n")
    delaysByAirlineAndByAirportRecords.foreach(r =>
      println(s"${r.AirlineDesc} to ${r.DestAirportDesc}: ${"%.2f".format(
        r.AvgDelayTime.get)} min (avg)"))

    // ToDo: Any other interesting insights
  }
}
