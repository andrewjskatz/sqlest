import sbt._
import sbt.Keys._
import spray.boilerplate.BoilerplatePlugin.Boilerplate.{ settings => boilerplateSettings }

object SqlestBuild extends Build {

  lazy val sqlest = Project(
    id = "sqlest",
    base = file("."),

    settings = Seq(
      organization := "jhc",
      name := "sqlest",
      version := "0.0.1-SNAPSHOT",

      scalaVersion := "2.11.1",
      scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature"),

      libraryDependencies ++= Seq(
        "joda-time" % "joda-time" % "2.2",
        "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
        "org.scalatest" %% "scalatest" % "2.1.7" % "test"
      )
    ) ++ boilerplateSettings
  )
}
