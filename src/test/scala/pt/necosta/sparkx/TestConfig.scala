package pt.necosta.sparkx

import java.io.File
import java.nio.file.{Files, Paths}

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

trait TestConfig
    extends FlatSpec
    with Matchers
    with BeforeAndAfterAll
    with SharedSparkContext {

  val testFolderPath: String = Files
    .createTempDirectory("sparkx")
    .toString

  def tryCopyResourcesToTestDir(): Unit = {
    if (new File(testFolderPath).list().nonEmpty)
      return

    val folder = new File(this.getClass.getResource("/").getPath)
    if (folder.exists && folder.isDirectory)
      folder.listFiles
        .filter(f => f.getName.endsWith(".csv"))
        .toList
        .foreach(file =>
          Files.copy(this.getClass.getResourceAsStream(s"/${file.getName}"),
                     Paths.get(s"$testFolderPath/${file.getName}")))
  }
}
