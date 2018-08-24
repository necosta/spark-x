package pt.necosta.sparkx

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class DataflowSpec
    extends FlatSpec
    with Matchers
    with BeforeAndAfterAll
    with SharedSparkContext {

  val folder = new java.io.File("output.parquet")

  override def afterAll() = {
    folder.listFiles().foreach(f => f.delete())
    folder.delete()
    super.afterAll()
  }
  "Dataflow" should "correctly run pipeline" in {
    val sourceFilePath = this.getClass.getResource("/sourceData.csv").getPath
    val airlineFilePath = this.getClass.getResource("/airlineData.csv").getPath
    val targetFolder = folder.getName
    Dataflow.withConfig(sourceFilePath, airlineFilePath, targetFolder).start()
    folder.exists() should be(true)
  }
}
