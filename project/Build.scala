package sample

import sbt._
import sbt.Keys._
import com.typesafe.sbt.SbtAspectj.{ Aspectj, aspectjSettings, useInstrumentedClasses }
import com.typesafe.sbt.SbtAspectj.AspectjKeys.inputs

object ConcurrencyBuild extends Build {
  lazy val concurrency = Project(
    id = "concurrency",
    base = file("."),
    settings = Defaults.defaultSettings ++ aspectjSettings ++ Seq(
      organization := "com.typesafe.sbt.aspectj",
      version := "0.1",
      scalaVersion := "2.11.2",
      libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.6",

      // add akka-actor as an aspectj input (find it in the update report)
      inputs in Aspectj <++= update map { report =>
        report.matching(moduleFilter(organization = "com.typesafe.akka", name = "akka-actor*"))
      },

      // replace the original akka-actor jar with the instrumented classes in runtime
      fullClasspath in Runtime <<= useInstrumentedClasses(Runtime)
    )
  )
}
