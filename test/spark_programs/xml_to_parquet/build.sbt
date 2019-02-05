name := "XmlToParquet"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
    // Spark dependency
    "org.apache.spark" %% "spark-sql" % "2.0.2" % "provided",
    "com.databricks" % "spark-xml_2.11" %  "0.4.1"
)

//    "com.databricks" % "spark-xml_2.11" %  "0.3.3"

