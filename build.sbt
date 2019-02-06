name := "CCCInFreeMonad"

version := "0.1"

scalaVersion := "2.12.8"

addCompilerPlugin("org.scalameta" % "paradise" % "3.0.0-M11" cross CrossVersion.full)

libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-core" % "1.6.0",
  "org.typelevel" %% "cats-free" % "1.6.0",
  "org.typelevel" %% "cats-effect" % "1.2.0",
  "io.frees" %% "frees-core" % "0.8.2"
)