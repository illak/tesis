name := "WikidataBuilder"

version := "1.0"

scalaVersion := "2.11.8"

sparkVersion := "2.3.1"

sparkComponents ++= Seq("sql")


libraryDependencies ++= Seq(
    // Spark dependency
    //"org.apache.spark" %% "spark-sql" % "2.0.2" % "provided",
    "com.databricks" % "spark-xml_2.11" %  "0.4.1"
)

// META-INF discarding
assemblyMergeStrategy in assembly := {
       case PathList("META-INF", xs @ _*) => MergeStrategy.discard
       case x => MergeStrategy.first
   }
