package pt.necosta.sparkx

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

class DataObjectSpec extends FlatSpec with Matchers with SharedSparkContext {

  "DataObject" should "correctly import csv file into dataset" in {
    implicit val spark: SparkSession = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val filePath = this.getClass.getResource("/sourceData.csv").getPath
    val ds = DataObject.csvToDataset(filePath)

    ds.count() should be(9)
    ds.columns.length should be(11)

    ds.map(c => (c.FL_DATE.getYear, c.FL_DATE.getMonth)).head() should be(
      (118, 0))
  }
}
