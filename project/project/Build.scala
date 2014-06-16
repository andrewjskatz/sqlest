import sbt._
import sbt.Keys._

/**
* Make vertx plugin project available to the root sbt project
*/
object VertxBuild extends Build {
  lazy val root = Project(
    "root",
    file("."),
    settings = Seq(
        scalacOptions := Seq("-unchecked", "-deprecation", "-feature")
    )
  )
  
}