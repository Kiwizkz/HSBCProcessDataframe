ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.13"

lazy val root = (project in file("."))
  .settings(
    name := "hsbcprocessdataframeapp",
    idePackagePrefix := Some("com.kiwi.app")
  )
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0" % "test"
libraryDependencies += "junit" % "junit" % "4.13.2" % "test"