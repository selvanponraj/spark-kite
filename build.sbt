/*
 * Copyright 2015 David Greco
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import de.heikoseeberger.sbtheader.HeaderPattern
import de.heikoseeberger.sbtheader.license.Apache2_0
import sbt._

name := "spark-kite"

version in ThisBuild := "1.0"

scalaVersion := "2.10.5"

ivyScala := ivyScala.value map {
  _.copy(overrideScalaVersion = true)
}

scalariformSettings

scalastyleFailOnError := true

dependencyUpdatesExclusions := moduleFilter(organization = "org.scala-lang")

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8", // yes, this is 2 args
  "-feature",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Xfuture"
)

wartremoverErrors ++= Seq(
  Wart.Any2StringAdd,
  Wart.EitherProjectionPartial,
  Wart.OptionPartial,
  Wart.Product,
  Wart.Serializable,
  Wart.ListOps,
  Wart.Nothing
)

val sparkVersion = "1.3.0-cdh5.4.0"

val hadoopVersion = "2.6.0-cdh5.4.0"

val sparkAvroVersion = "1.0.0"

val avroVersion = "1.7.6-cdh5.4.0"

val kiteVersion = "1.0.0-cdh5.4.0"

val scalaTestVersion = "2.2.4"

resolvers in ThisBuild ++= Seq(
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "compile" excludeAll ExclusionRule(organization = "org.apache.hadoop"),
  "org.apache.spark" %% "spark-sql" % sparkVersion % "compile" excludeAll ExclusionRule(organization = "org.apache.hadoop"),
  "org.apache.spark" %% "spark-yarn" % sparkVersion % "compile" excludeAll ExclusionRule(organization = "org.apache.hadoop"),
  "org.kitesdk" % "kite-data-core" % kiteVersion % "provided",
  "org.kitesdk" % "kite-data-mapreduce" % kiteVersion % "provided",
  "com.databricks" %% "spark-avro" % sparkAvroVersion % "provided" excludeAll ExclusionRule(organization = "org.apache.avro"),
  "org.apache.avro" % "avro" % avroVersion % "provided" exclude("org.mortbay.jetty", "servlet-api") exclude("io.netty", "netty") exclude("org.apache.avro", "avro-ipc") exclude("org.mortbay.jetty", "jetty"),
  "org.apache.avro" % "avro-mapred" % avroVersion % "provided" classifier "hadoop2" exclude("org.mortbay.jetty", "servlet-api") exclude("io.netty", "netty") exclude("org.apache.avro", "avro-ipc") exclude("org.mortbay.jetty", "jetty"),
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided" exclude("org.slf4j", "slf4j-api") excludeAll ExclusionRule("javax.servlet")
)

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run)) //http://stackoverflow.com/questions/18838944/how-to-add-provided-dependencies-back-to-run-test-tasks-classpath/21803413#21803413

fork := true //http://stackoverflow.com/questions/27824281/sparksql-missingrequirementerror-when-registering-table

parallelExecution in Test := false

headers := Map(
  "scala" ->(HeaderPattern.cStyleBlockComment, Apache2_0("2015", "David Greco")._2),
  "conf" ->(HeaderPattern.hashLineComment, Apache2_0("2015", "David Greco")._2)
)

lazy val root = (project in file(".")).
  configs(IntegrationTest).
  settings(Defaults.itSettings: _*).
  settings(
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % scalaTestVersion % "it,test"
    )
  ).enablePlugins(AutomateHeaderPlugin).disablePlugins(AssemblyPlugin)

lazy val assembly_ = (project in file("assembly")).
  settings(
    ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) },
    assemblyJarName in assembly := s"spark-kite-assembly-${version.value}.jar",
    libraryDependencies ++= Seq(
      "org.kitesdk" % "kite-data-core" % kiteVersion % "compile",
      "org.kitesdk" % "kite-data-mapreduce" % kiteVersion % "compile",
      "com.databricks" %% "spark-avro" % sparkAvroVersion % "compile" excludeAll ExclusionRule(organization = "org.apache.avro"),
      "org.apache.avro" % "avro" % avroVersion % "compile" exclude("org.mortbay.jetty", "servlet-api") exclude("io.netty", "netty") exclude("org.apache.avro", "avro-ipc") exclude("org.mortbay.jetty", "jetty"),
      "org.apache.avro" % "avro-mapred" % avroVersion % "compile" classifier "hadoop2" exclude("org.mortbay.jetty", "servlet-api") exclude("io.netty", "netty") exclude("org.apache.avro", "avro-ipc") exclude("org.mortbay.jetty", "jetty")
    )
  ) dependsOn root settings (
  projectDependencies := {
    Seq(
      (projectID in root).value.excludeAll(ExclusionRule(organization = "org.apache.spark"))
    )
  })

AutomateHeaderPlugin.automateFor(IntegrationTest)

net.virtualvoid.sbt.graph.Plugin.graphSettings