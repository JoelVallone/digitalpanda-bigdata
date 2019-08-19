name := "sensor-digestion"

version := "0.1"

scalaVersion := "2.11.12"

resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.3",
  "org.apache.spark" %% "spark-sql" % "2.4.3",

  "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.1",

  "org.digitalpanda" % "backend.api" % "0.1.0",

  "com.datastax.spark"  %% "spark-cassandra-connector-embedded" % "2.4.1" % "test",
  "junit" % "junit" % "4.10" %  Test,
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)