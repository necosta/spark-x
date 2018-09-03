package pt.necosta.sparkx

import java.io.File

class DataflowSpec extends TestConfig {

  override def beforeAll(): Unit = {
    super.beforeAll()
    tryCopyResourcesToTestDir()
  }

  "Dataflow" should "correctly run import pipeline" in {
    val dataflow = Dataflow.withConfig(testFolderPath)
    dataflow.runImport()

    new File(dataflow.outputFile).exists() should be(true)

    val airlineFileName =
      dataflow.dataPrep.tables(dataflow.dataPrep.airlineTableName)
    val airlineFile = new File(s"$testFolderPath/$airlineFileName")
    airlineFile.exists() should be(true)
    // just check file size is greater than 60k...
    airlineFile.length().toInt should be > (60 * 1024)
  }
}
