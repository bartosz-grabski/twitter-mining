name := "tagger"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.mongodb" %% "casbah" % "2.5.0"

libraryDependencies += "net.minidev" % "json-smart" % "1.0.9"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.0.0"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.0.0"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"