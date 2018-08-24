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

  def withConfig(sourceFilePath: String, airlineFilePath: String): Dataflow = {
    new Dataflow(sourceFilePath,airlineFilePath)
  }
}

class Dataflow(sourceFilePath: String, airlineFilePath: String) extends WithSpark {
  def start() = {

    // 1 - Import file into dataset
    val sourceDataset = DataObject.getSource(sourceFilePath)
    // 2 - Build final table
    val outDs = sourceDataset.transform(DataObject.getOutput(airlineFilePath))
    // ToDo: 3 - Save dataset for future Spark jobs?
  }
}
