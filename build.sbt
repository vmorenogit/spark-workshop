name := "spark-workshop"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.5.1"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "1.5.1"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "1.5.1"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.3.0"

resolvers += Resolver.sonatypeRepo("public")