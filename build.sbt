ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.16"

lazy val root = (project in file("."))
  .settings(
    name := "Spark_practice"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.1"