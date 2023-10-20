import sbt.util

name := "ExtremeComputing"

scalaVersion := "2.12.15"

scalacOptions ++= Seq("-deprecation")

//resolvers += Resolver.sonatypeRepo("releases")
resolvers ++= Resolver.sonatypeOssRepos("releases")

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.3"

// grading libraries
libraryDependencies += "junit" % "junit" % "4.10" % Test
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.9" % Test

logLevel := util.Level.Error
