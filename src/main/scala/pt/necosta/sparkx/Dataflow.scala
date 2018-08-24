package pt.necosta.sparkx

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Dataflow {

  val idColumnName = "id"

  def addIdColumn(): DataFrame => DataFrame = { df =>
    {
      require(!df.columns.contains(idColumnName))
      df.withColumn(idColumnName, monotonically_increasing_id)
    }
  }

  def withConfig(sourceFilePath: String): Dataflow = {
    new Dataflow(sourceFilePath)
  }
}

class Dataflow(sourceFilePath: String) extends WithSpark {
  def start() = {

    // 1 - Import file into dataset
    val ds = DataObject.csvToDataset(sourceFilePath)

    // ToDo: 2 - Import lookup tables
    // ToDo: 3 - Build final table

  }
}
