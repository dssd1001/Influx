name := "Influx"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.1.0"

libraryDependencies += "org.scalanlp" % "breeze_2.11" % "0.12"
resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"