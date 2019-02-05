name := "PathComputing"

version := "1.0"

scalaVersion := "2.11.8"

sparkVersion := "2.3.1"

// Add Spark components this package depends on, e.g, "mllib", ....
sparkComponents ++= Seq("sql", "mllib")


libraryDependencies ++= Seq(
    "graphframes" % "graphframes" % "0.5.0-spark2.1-s_2.11"
)


//    "org.apache.spark" %% "spark-sql" % "2.1.1" % "provided",

//spDependencies += "graphframes/graphframes:0.5.0-spark2.1-s_2.11"

//resolvers += Resolver.url("SparkPackages", url("https://dl.bintray.com/spark-packages/maven/"))


// META-INF discarding
//assemblyMergeStrategy in assembly := {
//       case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//       case x => MergeStrategy.first
//   }
