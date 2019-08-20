name := "sensor-digestion"

version := "0.1"

scalaVersion := "2.11.12"

resolvers += Resolver.mavenLocal
//resolvers += "DataStax Repo" at "https://repo.datastax.com/public-repos/"


// Test Dependencies
// The 'test/resources' Directory in should match the resources directory in the `it` directory
// for the version of the Spark Cassandra Connector in use.
val scalaTestVersion = "2.2.4"
val connectorVersion = "2.4.1"
val jUnitVersion = "4.12"
val cassandraVersion = "3.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.3",
  "org.apache.spark" %% "spark-sql" % "2.4.3",
  "com.datastax.spark" %% "spark-cassandra-connector" % connectorVersion,

  "org.digitalpanda" % "backend.api" % "0.1.0")

libraryDependencies ++= Seq(
  "com.datastax.spark"  %% "spark-cassandra-connector-embedded" % "2.0.10" % "test",
  "org.apache.cassandra" % "cassandra-all" % cassandraVersion % "test",
  "junit" % "junit" % jUnitVersion %  "test",
  "org.scalatest" %% "scalatest" % scalaTestVersion % "test"

).map(_.excludeAll(
  ExclusionRule("org.slf4j","log4j-over-slf4j"),
  ExclusionRule("org.slf4j","slf4j-log4j12"))
) // Excluded to allow for Cassandra to run embedded

//Forking is required for the Embedded Cassandra
fork in Test := true


//assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)