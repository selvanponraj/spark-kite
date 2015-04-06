name := "spark-cdh5-template"

version := "1.0"

scalaVersion := "2.10.5"

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

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

resolvers ++= Seq(
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "compile" excludeAll ExclusionRule(organization = "org.apache.hadoop"),
  "org.apache.spark" %% "spark-sql" % sparkVersion % "compile" excludeAll ExclusionRule(organization = "org.apache.hadoop"),
  "org.apache.spark" %% "spark-yarn" % sparkVersion % "compile" excludeAll ExclusionRule(organization = "org.apache.hadoop"),
  "org.kitesdk" % "kite-data-core" % kiteVersion % "compile",
  "org.kitesdk" % "kite-data-mapreduce" % kiteVersion % "compile",
  "com.databricks" % "spark-avro_2.10" % sparkAvroVersion % "compile" excludeAll ExclusionRule(organization = "org.apache.avro"),
  "org.apache.avro" % "avro" % avroVersion % "compile" exclude("org.mortbay.jetty", "servlet-api") exclude("io.netty", "netty") exclude("org.apache.avro", "avro-ipc") exclude("org.mortbay.jetty", "jetty"),
  "org.apache.avro" % "avro-mapred" % avroVersion % "compile" classifier("hadoop2") exclude("org.mortbay.jetty", "servlet-api") exclude("io.netty", "netty") exclude("org.apache.avro", "avro-ipc") exclude("org.mortbay.jetty", "jetty"),
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "compile" excludeAll ExclusionRule("javax.servlet")
)

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))

fork := true //http://stackoverflow.com/questions/27824281/sparksql-missingrequirementerror-when-registering-table

parallelExecution in Test := false

lazy val root = (project in file(".")).
  configs(IntegrationTest).
  settings(Defaults.itSettings: _*).
  settings(
    libraryDependencies += "org.scalatest" % "scalatest_2.10" % scalaTestVersion % "it,test"
  )
