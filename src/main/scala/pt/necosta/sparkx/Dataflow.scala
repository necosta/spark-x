package pt.necosta.sparkx

object Dataflow {

  def withConfig(sourceFolder: String): Dataflow = {
    new Dataflow(sourceFolder)
  }
}

class Dataflow(sourceFolder: String) extends WithSpark {

  private val saveFormat = "parquet"
  private val dataPrep = DataPrep.init(sourceFolder)
  val outputFolder = s"$sourceFolder/output.parquet"

  def start(): Unit = {
    // 1 - Import file into dataset
    val sourceDs = dataPrep.getSource()
    // 2 - Build final table
    val transformedDs =
      sourceDs.transform(DataPrep.init(sourceFolder).buildFinalDs())
    // 3 - Save dataset for future Spark jobs
    transformedDs.write.format(saveFormat).save(outputFolder)
  }
}
