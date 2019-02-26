name := "dj_challenge_scio"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "com.spotify" %% "scio-core" % "0.7.1",
  "com.spotify" %% "scio-test" % "0.7.1" % Test,
  "org.apache.beam" % "beam-runners-direct-java" % "2.9.0",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.apache.beam" % "beam-runners-spark" % "2.9.0",
  "org.apache.spark" %% "spark-core" % "2.3.2",
  "org.apache.spark" %% "spark-streaming" % "2.3.2",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"

)

