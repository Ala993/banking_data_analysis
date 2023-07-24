ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "Banking"
  )
val sparkVersion = "3.4.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.1"
libraryDependencies += "org.scalanlp" %% "breeze-viz" % "1.0"
libraryDependencies += "org.jfree" % "jfreechart" % "1.5.3"