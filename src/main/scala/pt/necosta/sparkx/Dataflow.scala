package pt.necosta.sparkx

object Dataflow {

  def withConfig(sourceFilePath: String,
                 airlineFilePath: String,
                 outputFolder: String): Dataflow = {
    new Dataflow(sourceFilePath, airlineFilePath, outputFolder)
  }
}

class Dataflow(sourceFilePath: String,
               airlineFilePath: String,
               outputFolder: String)
    extends WithSpark {

  private val saveFormat = "parquet"
  private val dataObject = DataPrep.init()

  def start(): Unit = {
    // 1 - Import file into dataset
    val sourceDs = dataObject.getSource(sourceFilePath)
    // 2 - Build final table
    val transformedDs = sourceDs
      .transform(dataObject.getOutput(airlineFilePath))
    // 3 - Save dataset for future Spark jobs
    transformedDs.write.format(saveFormat).save(outputFolder)
  }
}
