ThisBuild / organization := "com.nexus"
ThisBuild / version := "1.0.0"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "nexus-streaming",

    libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.9" % Provided,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.9",
  "io.delta" %% "delta-spark" % "3.3.2",
  "org.apache.iceberg" % "iceberg-spark-runtime-3.5_2.12" % "1.5.2"
),

    assembly / assemblyJarName := "nexus-streaming-assembly.jar",

    assembly / mainClass := Some("producer.OrderEventProducer"),

    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case PathList("META-INF", _*) => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case _ => MergeStrategy.first
    }
  )

