name := "dj_challenge_spark"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.2",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)