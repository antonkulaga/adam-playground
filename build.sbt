import com.typesafe.sbt.SbtNativePackager.autoImport._

import sbt.Keys._

import sbt._

name := "adam-playground"

organization := "comp.bio.aging"

scalaVersion :=  "2.11.8"

isSnapshot := true

scalacOptions ++= Seq( "-target:jvm-1.8", "-feature", "-language:_" )

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint", "-J-Xss5M", "-encoding", "UTF-8")

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

mainClass in Compile := Some("comp.bio.aging.extractor.Main")

resourceDirectory in Test := baseDirectory { _ / "files" }.value

unmanagedClasspath in Compile ++= (unmanagedResources in Compile).value

updateOptions := updateOptions.value.withCachedResolution(true) //to speed up dependency resolution

resolvers += sbt.Resolver.bintrayRepo("comp-bio-aging", "main")

resolvers += sbt.Resolver.bintrayRepo("denigma", "denigma-releases")

resolvers += "ICM repository" at "http://maven.icm.edu.pl/artifactory/repo"

libraryDependencies ++= Seq(
  
  "org.apache.spark" %% "spark-core" % "2.0.2",

  "org.bdgenomics.adam" %% "adam-core-spark2" % "0.21.0",

  "org.bdgenomics.utils" %% "utils-misc" % "0.2.11",

  "org.bdgenomics.utils" %% "utils-misc-spark2" % "0.2.11",

  "org.bdgenomics.utils" %% "utils-intervalrdd-spark2" % "0.2.11",

  "org.scalatest" %% "scalatest" % "3.0.1" % Test,

  "com.holdenkarau" %% "spark-testing-base" % "2.1.0_0.6.0" % Test,

  "org.scalacheck" %% "scalacheck" % "1.13.4" % Test
)

libraryDependencies += "com.lihaoyi" % "ammonite" % "0.8.2" % Test cross CrossVersion.full

initialCommands in (Test, console) := """ammonite.Main().run()"""

exportJars := true

fork in run := true

parallelExecution in Test := false

maintainer := "Anton Kulaga <antonkulaga@gmail.com>"

packageSummary := "adam-playground"

packageDescription := """Adam"""

bintrayRepository := "main"

bintrayOrganization := Some("comp-bio-aging")

licenses += ("MPL-2.0", url("http://opensource.org/licenses/MPL-2.0"))

enablePlugins(JavaAppPackaging)
