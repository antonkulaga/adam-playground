import com.typesafe.sbt.SbtNativePackager.autoImport._

import sbt.Keys._

import sbt._

name := "adam-playground"

organization := "comp.bio.aging"

scalaVersion :=  "2.11.11"

coursierMaxIterations := 200

isSnapshot := true

scalacOptions ++= Seq( "-target:jvm-1.8", "-feature", "-language:_" )

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint", "-J-Xss5M", "-encoding", "UTF-8")

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

mainClass in Compile := Some("comp.bio.aging.extractor.Main")

resourceDirectory in Test := baseDirectory { _ / "files" }.value

unmanagedClasspath in Compile ++= (unmanagedResources in Compile).value

updateOptions := updateOptions.value.withCachedResolution(true) //to speed up dependency resolution

resolvers += Resolver.mavenLocal

resolvers += sbt.Resolver.bintrayRepo("comp-bio-aging", "main")

resolvers += sbt.Resolver.bintrayRepo("denigma", "denigma-releases")

resolvers += "ICM repository" at "http://maven.icm.edu.pl/artifactory/repo"

lazy val sparkVersion = "2.2.0"

lazy val adamVersion = "0.23.0-SNAPSHOT"//"0.22.0"

lazy val utilsVersion = "0.2.13"

libraryDependencies ++= Seq(
  
  "org.apache.spark" %% "spark-core" % sparkVersion,

  "org.apache.spark" %% "spark-sql" % sparkVersion,

  "org.bdgenomics.adam" %% "adam-core-spark2" % adamVersion,

  "org.bdgenomics.utils" %% "utils-misc" % utilsVersion,

  "org.bdgenomics.utils" %% "utils-misc-spark2" % utilsVersion,

  "org.bdgenomics.utils" %% "utils-intervalrdd-spark2" % utilsVersion,

  "org.bdgenomics.utils" %% "utils-misc" % utilsVersion % Test,

  "org.scalatest" %% "scalatest" % "3.0.3" % Test,

  "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.7.2" % Test,

  "org.scalacheck" %% "scalacheck" % "1.13.5" % Test
)

libraryDependencies += "com.lihaoyi" % "ammonite" % "1.0.0" % Test cross CrossVersion.full

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
