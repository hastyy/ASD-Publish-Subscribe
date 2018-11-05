name := "Publish Subscribe"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.4.10"
libraryDependencies += "com.typesafe.akka" %% "akka-remote" % "2.5.17"
libraryDependencies += "com.typesafe" % "config" % "1.2.1"

resolvers += "Akka Snapshot Repository" at "http://repo.akka.io/snapshots/"
