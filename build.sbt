name := "simba"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-catalyst" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0"

libraryDependencies += "com.vividsolutions" % "jts-core" % "1.14.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"