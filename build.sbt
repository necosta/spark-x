lazy val root = (project in file("."))
  .settings(
    name := "sparkx",
    organization := "pt.necosta",
    scalaVersion := "2.12.6",
    version := "0.0.1-SNAPSHOT"
  )

  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.0.5" % "test")
