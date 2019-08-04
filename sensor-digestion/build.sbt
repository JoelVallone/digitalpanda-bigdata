name := "sensor-digestion"

version := "0.1"

scalaVersion := "2.12.8"

resolvers += Resolver.mavenLocal

libraryDependencies += "junit" % "junit" % "4.10" % Test
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.3",
  "org.apache.spark" %% "spark-sql" % "2.4.3",

  "org.digitalpanda" % "backend.api" % "0.1.0"
)