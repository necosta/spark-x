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
  }
}
