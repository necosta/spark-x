package pt.necosta.sparkx

object Dataflow {

  def withConfig(sourceFolder: String): Dataflow = {
    new Dataflow(sourceFolder)
  }
}

class Dataflow(sourceFolder: String) extends WithSpark {

  private val saveFormat = "parquet"
  val dataPrep = DataPrep.init(sourceFolder)
  val outputFolder = s"$sourceFolder/output.parquet"

  def start(): Unit = {
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
}
