package pt.necosta.sparkx

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

class DataObjectSpec extends FlatSpec with Matchers with SharedSparkContext {

  "DataObject" should "correctly import source csv file into dataset" in {
    implicit val spark: SparkSession = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val filePath = this.getClass.getResource("/sourceData.csv").getPath
    val ds = DataObject.init().getSource(filePath)

    ds.count() should be(9)
    ds.columns.length should be(11)

    // ToDo: Replace deprecated code
    ds.map(c => (c.FL_DATE.getYear, c.FL_DATE.getMonth)).head() should be(
      (118, 0))
  }

  "DataObject" should "correctly import lookup csv file into dataset" in {
    val filePath = this.getClass.getResource("/airlineData.csv").getPath
    val ds = DataObject.init().getLookup(filePath)

    ds.count() should be(9)
    ds.columns.length should be(2)
  }

  "DataObject" should "correctly join source and lookup datasets" in {
    val sourceFilePath = this.getClass.getResource("/sourceData.csv").getPath
    val airlineFilePath = this.getClass.getResource("/airlineData.csv").getPath
    val out = DataObject
      .init()
      .getSource(sourceFilePath)
      .transform(DataObject.init().getOutput(airlineFilePath))

    out.count() should be(9)
    out.columns.length should be(3)
  }
}
