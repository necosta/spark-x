package pt.necosta.sparkx

import java.io.File

class DataflowSpec extends TestConfig {

  override def beforeAll(): Unit = {
    super.beforeAll()
    tryCopyResourcesToTestDir()
  }

  "Dataflow" should "correctly run pipeline" in {
    val dataflow = Dataflow.withConfig(testFolderPath)
    dataflow.start()

    new File(dataflow.outputFolder).exists() should be(true)

    val airlineFileName =
      dataflow.dataPrep.tables(dataflow.dataPrep.airlineTableName)
    val airlineFile = new File(s"$testFolderPath/$airlineFileName")
    airlineFile.exists() should be(true)
    airlineFile.length() should be(61643)
  }
}
