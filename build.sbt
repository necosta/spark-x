lazy val root = (project in file("."))
  .settings(
    name := "sparkx",
    organization := "pt.necosta",
    scalaVersion := "2.11.12",
    version := "0.0.2-SNAPSHOT"
  )

val sparkVersion = "2.3.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.holdenkarau" %% "spark-testing-base" % (sparkVersion + "_0.10.0") % "test",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)

scalafmtOnCompile in ThisBuild := true
scalafmtTestOnCompile in ThisBuild := true
scalafmtFailTest in ThisBuild := true

// Spark test class does not seem to handle well parallelization...
parallelExecution in Test := false
