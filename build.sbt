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

resolvers += Resolver.sonatypeRepo("releases")

resolvers += sbt.Resolver.bintrayRepo("comp-bio-aging", "main")

resolvers += sbt.Resolver.bintrayRepo("denigma", "denigma-releases")

resolvers += "ICM repository" at "http://maven.icm.edu.pl/artifactory/repo"

resolvers += "jitpack.io" at "https://jitpack.io"

lazy val sparkVersion = "2.3.0"

lazy val adamVersion = "0.24.0"

lazy val utilsVersion = "0.2.13"

lazy val enumeratumVersion = "1.5.13"

lazy val pprintVersion = "0.5.3"

val framelessVersion = "0.6.1"

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % sparkVersion,

  "org.apache.spark" %% "spark-sql" % sparkVersion,

  "org.apache.spark" %% "spark-mllib" % sparkVersion,

  "org.bdgenomics.adam" %% "adam-core-spark2" % adamVersion,

  "org.bdgenomics.utils" %% "utils-misc" % utilsVersion,

  "org.bdgenomics.utils" %% "utils-misc-spark2" % utilsVersion,

  "org.bdgenomics.utils" %% "utils-intervalrdd-spark2" % utilsVersion,

  "com.beachape" %% "enumeratum" % enumeratumVersion,

  "com.lihaoyi" %% "pprint" % pprintVersion,

  "com.github.guillaumedd" %% "gstlib" % "0.1.2",

  "org.bdgenomics.utils" %% "utils-misc" % utilsVersion % Test,

  "org.scalatest" %% "scalatest" % "3.0.5" % Test,

  "com.holdenkarau" %% "spark-testing-base" % "2.3.0_0.9.0" % Test,

  "org.scalacheck" %% "scalacheck" % "1.14.0" % Test,

  "org.typelevel" %% "frameless-cats"      % framelessVersion % Test,

  "org.typelevel" %% "frameless-dataset"   % framelessVersion % Test,

  "org.typelevel" %% "frameless-ml"      % framelessVersion % Test,

  "org.apache.hadoop" % "hadoop-azure" % "2.7.6" % Test,

  "com.microsoft.azure" % "azure-storage" % "7.0.0" % Test
)

initialCommands in (Test, console) := """ammonite.Main().run()"""

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oF")

exportJars := true

fork in run := true

parallelExecution in Test := false

bintrayRepository := "main"

bintrayOrganization := Some("comp-bio-aging")

licenses += ("MPL-2.0", url("http://opensource.org/licenses/MPL-2.0"))

libraryDependencies += "com.lihaoyi" % "ammonite" % "1.1.2" % Test cross CrossVersion.full

sourceGenerators in Test += Def.task {
  val file = (sourceManaged in Test).value / "amm.scala"
  IO.write(file, """object amm extends App { ammonite.Main.main(args) }""")
  Seq(file)
}.taskValue

libraryDependencies += "com.github.pathikrit" %% "better-files" % "3.4.0" % Test