name := "checker"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
    // Spark dependency
    "org.apache.spark" %% "spark-sql" % "2.0.2" % "provided",
    "org.apache.hadoop" % "hadoop-hdfs" % "2.5.2"

)

