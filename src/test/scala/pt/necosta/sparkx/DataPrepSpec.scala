package pt.necosta.sparkx

import org.apache.spark.sql.SparkSession

class DataPrepSpec extends TestConfig {

  override def beforeAll(): Unit = {
    super.beforeAll()
    tryCopyResourcesToTestDir()
  }

  "DataPrep" should "correctly import source csv file into dataset" in {
    implicit val spark: SparkSession = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val ds = DataPrep.init(testFolderPath).getSourceDs

    ds.count() should be(9)
    ds.columns.length should be(11)

    // ToDo: Replace deprecated code
    ds.map(c => (c.FL_DATE.getYear, c.FL_DATE.getMonth)).head() should be(
      (118, 0))
  }

  "DataPrep" should "correctly import lookup csv file into dataset" in {
    val filePath = this.getClass.getResource("/airlineData.csv").getPath
    val ds = DataPrep.init(testFolderPath).getLookup(filePath)

    ds.count() should be(9)
    ds.columns.length should be(2)
  }

  "DataPrep" should "correctly join source and lookup datasets" in {
    val out = DataPrep
      .init(testFolderPath)
      .getSourceDs
      .transform(DataPrep.init(testFolderPath).buildFinalDs())

    out.count() should be(9)
    out.columns.length should be(3)
  }
}
