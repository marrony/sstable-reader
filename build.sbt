ThisBuild / scalaVersion := "2.13.8"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "marrony"
ThisBuild / organizationName := "marrony"

val nettyVersion = "4.1.77.Final"

val root = (project in file("."))
  .settings(
    libraryDependencies ++= Seq(
      "io.netty" % "netty-common" % nettyVersion,
      "io.netty" % "netty-buffer" % nettyVersion,
      "org.lz4" % "lz4-java" % "1.8.0"
    ),

    scalacOptions ++= Seq(
      "-target:jvm-1.11",
      "-Xfatal-warnings",

      "-unchecked",
      "-deprecation",
      "-feature",
      "-Ymacro-annotations",
      "-opt-warnings:at-inline-failed",

      "-Xlint:_",
      "-Xlint:-implicit-recursion",
      "-Xlint:-type-parameter-shadow",
      "-Xlint:-valpattern",
      "-Ywarn-dead-code",
      "-Ywarn-macros:after",
      "-Wunused",
      "-Ypatmat-exhaust-depth", "200",

      "-language:postfixOps"
    )
  )
