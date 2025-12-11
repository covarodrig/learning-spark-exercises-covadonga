name := "learning-spark-exercises"
version := "1.0"
scalaVersion := "2.12.17"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.0",
  "org.apache.spark" %% "spark-sql" % "3.3.0",
  "org.apache.spark" %% "spark-mllib" % "3.3.0",
  "io.delta" %% "delta-core" % "2.3.0"
)
