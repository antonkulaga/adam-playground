import sbt.Keys._

import sbt._

name := "adam-playground"

organization := "comp.bio.aging"

scalaVersion :=  "2.11.12"

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

lazy val sparkVersion = "2.2.1"

lazy val adamVersion = "0.23.0-SNAPSHOT"//"0.22.0"

lazy val utilsVersion = "0.2.13"

lazy val enumeratumVersion = "1.5.12"

lazy val pprintVersion = "0.5.3"

libraryDependencies ++= Seq(
  
  "org.apache.spark" %% "spark-core" % sparkVersion,

  "org.apache.spark" %% "spark-sql" % sparkVersion,

  "org.apache.spark" %% "spark-hive" % sparkVersion,

  "org.bdgenomics.adam" %% "adam-core-spark2" % adamVersion,

  "org.bdgenomics.utils" %% "utils-misc" % utilsVersion,

  "org.bdgenomics.utils" %% "utils-misc-spark2" % utilsVersion,

  "org.bdgenomics.utils" %% "utils-intervalrdd-spark2" % utilsVersion,

  "com.beachape" %% "enumeratum" % enumeratumVersion,

  "com.lihaoyi" %% "pprint" % pprintVersion,

  "org.bdgenomics.utils" %% "utils-misc" % utilsVersion % Test,

  "org.scalatest" %% "scalatest" % "3.0.4" % Test,

  "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.8.0" % Test,

  "org.scalacheck" %% "scalacheck" % "1.13.5" % Test

)

initialCommands in (Test, console) := """ammonite.Main().run()"""

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oF")

exportJars := true

fork in run := true

parallelExecution in Test := false

bintrayRepository := "main"

bintrayOrganization := Some("comp-bio-aging")

licenses += ("MPL-2.0", url("http://opensource.org/licenses/MPL-2.0"))