ThisBuild / resolvers ++= Seq(
    "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
    "Maven Central" at "https://repo1.maven.org/maven2/",
    Resolver.mavenLocal
)

name := "sensor-digestion-flink"

version := "0.1-SNAPSHOT"

organization := "org.digitalpanda.wordcount"

ThisBuild / scalaVersion := "2.11.12"

val flinkVersion = "1.9.1"
val avroVersion = "1.9.1"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",

  "org.apache.flink" %% "flink-connector-kafka" % flinkVersion ,

  "org.apache.flink" % "flink-avro" % flinkVersion,
  "org.apache.flink" % "flink-avro-confluent-registry" % flinkVersion,
  "org.apache.avro" % "avro" % avroVersion
)

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies
  )

assembly / mainClass := Some("org.digitalpanda.flink.sensor.digestion.DigestionJob")

// make run command include the provided dependencies
Compile / run  := Defaults.runTask(Compile / fullClasspath,
                                   Compile / run / mainClass,
                                   Compile / run / runner
                                  ).evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true

// exclude Scala library from assembly
assembly / assemblyOption  := (assembly / assemblyOption).value.copy(includeScala = false)
