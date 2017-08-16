name := "Scala Common"

version := "0.9"

scalaVersion := "2.11.8"

javacOptions ++= Seq("-encoding", "UTF-8")

compileOrder := CompileOrder.JavaThenScala

exportJars := true

libraryDependencies += "commons-lang" % "commons-lang" % "2.6"
