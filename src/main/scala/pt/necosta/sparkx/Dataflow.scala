package pt.necosta.sparkx

import org.apache.spark.sql.functions._

object Dataflow {

  def withConfig(sourceFolder: String): Dataflow = {
    new Dataflow(sourceFolder)
  }
}

class Dataflow(sourceFolder: String) extends WithSpark {

  private val saveFormat = "parquet"
  val dataPrep = DataPrep.init(sourceFolder)
  val outputFolder = s"$sourceFolder/output.parquet"

  def runImport(): Unit = {
    // 1 - Import all lookup tables into source folder as csv files
    dataPrep.importLookupTables()
    // 2 - Transform base csv file into dataset
    val sourceDs = dataPrep.getSourceDs
    // 3 - Build final denormalized dataset
    val transformedDs =
      sourceDs.transform(DataPrep.init(sourceFolder).buildFinalDs())
    // 4 - Persist final dataset as parquet for future Spark jobs
    transformedDs.write.format(saveFormat).save(outputFolder)
  }

  def runAnalysis(): Unit = {
    import spark.implicits._

    val ds = spark.read.parquet(outputFolder).as[OutputRecord]

    // Find the Airports with the least delay
    val delaysByAirlineDs = ds.transform(DataAnalysis.getDelaysByAirline)
    val NUMBER_RECORDS = 3
    val delaysByAirlineRecord = delaysByAirlineDs
      .orderBy(asc("DeparturesWithDelayPerc"))
      .take(NUMBER_RECORDS)
    println(s"The top $NUMBER_RECORDS airports with the least delay are:")
    delaysByAirlineRecord.foreach(r =>
      println(s"${r.AirlineDesc}: ${r.DeparturesWithDelayPerc} delays"))

    // Which Airline has most flights to New York

    // Which airlines arrive the worst on which airport and by what delay

    // Any other interesting insights
  }

}
