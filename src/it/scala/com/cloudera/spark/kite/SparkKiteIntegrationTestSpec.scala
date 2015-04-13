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

package com.cloudera.spark.kite

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.kitesdk.data._
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpec}

import scala.util.Random

case class ITPerson(name: String, age: Int) {
  def this() {
    this(null, 0)
  }
}

class SparkKiteIntegrationTestSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {
  var sparkContext: SparkContext = _

  override def beforeAll() = {

    val fs = FileSystem.get(new Configuration())
    val assembly = s"/user/${System.getProperty("user.name")}/spark-assembly.jar"
    if (fs.exists(new Path(assembly)))
      fs.delete(new Path(assembly), true)
    fs.copyFromLocalFile(
      false,
      true,
      new Path(s"file://${System.getProperty("user.dir")}/spark_assembly/spark-assembly_2.10-1.3.0-cdh5.4.0.jar"),
      new Path(s"/user/${System.getProperty("user.name")}/spark-assembly.jar")
    )

    val conf = new SparkConf().
      setAppName("spark-kite-integration-test").
      set("executor-memory", "128m").
      setJars(List(s"${System.getProperty("user.dir")}/assembly/target/scala-2.10/spark-kite-assembly-1.0.jar")).
      set("spark.yarn.jar", s"hdfs:///user/${System.getProperty("user.name")}/spark-assembly.jar").
      setMaster("yarn-client")
    sparkContext = new SparkContext(conf)
  }

  "Spark" must {
    "be able to create a SchemaRDD/Dataframe from a kite avro dataset using a reflection based schema" in {

      val conf = new Configuration()
      val dir = new Path(s"/user/${System.getProperty("user.name")}/tmp")
      val fileSystem = dir.getFileSystem(conf)
      if (fileSystem.exists(dir))
        fileSystem.delete(dir, true)

      val sqlContext = new SQLContext(sparkContext)

      val datasetURI = URIBuilder.build(s"repo:hdfs:///user/${System.getProperty("user.name")}/tmp", "test", "persons")

      val descriptor = new DatasetDescriptor.Builder().schema(classOf[ITPerson]).format(Formats.AVRO).build()

      val peopleDataset = Datasets.create[ITPerson, Dataset[ITPerson]](datasetURI, descriptor, classOf[ITPerson])

      val writer = peopleDataset.newWriter()

      val peopleList = (1 to 1000).map(i => ITPerson(s"person-$i", Random.nextInt(80)))
      peopleList.foreach(writer.write)
      writer.close()

      val data = sqlContext.kiteDatasetFile(peopleDataset)

      data.collect().sortBy(_.getAs[String](0).split("-")(1).toInt).map(row => ITPerson(row.getAs[String](0), row.getAs[Int](1))) must be(peopleList)

    }
  }

  override def afterAll() = {
    sparkContext.stop()
  }

}
