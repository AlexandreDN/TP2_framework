name := "p2"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.2.0"
libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.0-RC2"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.1.0"
)
libraryDependencies += "io.spray" %% "spray-json" % "1.3.2"