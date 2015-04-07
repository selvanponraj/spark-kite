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

import com.databricks.spark.avro.AvroSaver
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.kitesdk.data._
import org.kitesdk.data.mapreduce.DatasetKeyInputFormat
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpec}
import parquet.avro.AvroDataSupplier
import parquet.format.ColumnMetaData
import parquet.hadoop.ParquetInputFormat

import scala.util.Random

class SparkKiteIntegrationTestSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {
  var sparkContext: SparkContext = _

  def getJar(klass: Class[_]): String = {
    val codeSource = klass.getProtectionDomain.getCodeSource
    codeSource.getLocation.getPath
  }

  val listDependencies = List(
    getJar(AvroSaver.getClass),
    getJar(classOf[AvroInputFormat[GenericRecord]]),
    getJar(classOf[Dataset[_]]),
    getJar(classOf[DatasetKeyInputFormat[_]]),
    getJar(classOf[ColumnMetaData]),
    getJar(classOf[ParquetInputFormat[_]]),
    getJar(classOf[AvroDataSupplier])
  )

  println(listDependencies)

  override def beforeAll() = {
    val conf = new SparkConf().
      setAppName("spark-kite-integration-test").
      set("executor-memory", "256m").
      setJars(List(s"${System.getProperty("user.dir")}/target/scala-2.10/spark-cdh5-template-assembly-1.0.jar")).
      set("spark.yarn.jar", "hdfs:///user/spark/share/lib/spark-assembly.jar").
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
      ()

      val sqlContext = new SQLContext(sparkContext)

      val datasetURI = URIBuilder.build(s"repo:hdfs:///user/${System.getProperty("user.name")}/tmp", "test", "persons")

      val descriptor = new DatasetDescriptor.Builder().schema(classOf[Person]).format(Formats.AVRO).build()

      val peopleDataset = Datasets.create[Person, Dataset[Person]](datasetURI, descriptor, classOf[Person])

      val writer = peopleDataset.newWriter()

      val peopleList = (1 to 1000).map(i => Person(s"person-$i", Random.nextInt(80)))
      peopleList.foreach(writer.write)
      writer.close()

      val data = sqlContext.kiteDatasetFile(peopleDataset)

      data.collect().sortBy(_.getAs[String](0).split("-")(1).toInt).map(row => Person(row.getAs[String](0), row.getAs[Int](1))) must be(peopleList)

    }
  }

  override def afterAll() = {
    sparkContext.stop()
  }

}
