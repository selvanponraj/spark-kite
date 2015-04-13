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

import org.apache.spark.sql.SQLContext
import org.apache.spark.{ SparkConf, SparkContext }
import org.kitesdk.data._
import org.scalatest.{ BeforeAndAfterAll, MustMatchers, WordSpec }

import scala.util.Random

case class Person(name: String, age: Int) {
  def this() {
    this(null, 0)
  }
}

class SparkKiteSpec extends WordSpec with MustMatchers with BeforeAndAfterAll with TestSupport {

  var sparkContext: SparkContext = _

  override def beforeAll() = {
    val conf = new SparkConf().
      setAppName("spark-kite-spec-test").
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      setMaster("local[16]")
    sparkContext = new SparkContext(conf)
    ()
  }

  "Spark" must {
    "be able to create a SchemaRDD/Dataframe from a kite parquet dataset" in {

      cleanup()

      val products = generateDataset(Formats.PARQUET, CompressionType.Snappy)

      implicit val sqlContext = new SQLContext(sparkContext)

      val data = sqlContext.kiteDatasetFile(products)

      data.registerTempTable("product")

      val res = sqlContext.sql("select * from product where id < 10")

      res.map(row => (row.getAs[String](0), row.getAs[Long](1))).collect() must be(
        for {
          i <- 1 to 9
        } yield (s"product-$i", i.toLong)
      )

      cleanup()

    }
  }

  "Spark" must {
    "be able to create a SchemaRDD/Dataframe from a kite avro dataset" in {

      cleanup()

      val products = generateDataset(Formats.AVRO, CompressionType.Snappy)

      implicit val sqlContext = new SQLContext(sparkContext)

      val data = sqlContext.kiteDatasetFile(products)

      data.registerTempTable("product")

      val res = sqlContext.sql("select * from product where id < 10")

      res.map(row => (row.getAs[String](0), row.getAs[Long](1))).collect() must be(
        for {
          i <- 1 to 9
        } yield (s"product-$i", i.toLong)
      )

      cleanup()

    }
  }

  "Spark" must {
    "be able to create a kite parquet dataset from a SchemaRDD/Dataframe" in {

      cleanup()

      val sqlContext = new SQLContext(sparkContext)
      import sqlContext.implicits._

      val datasetURI = URIBuilder.build(s"repo:file:////${System.getProperty("user.dir")}/tmp", "test", "persons")

      val peopleList = List(Person("David", 50), Person("Ruben", 14), Person("Giuditta", 12), Person("Vita", 19))
      val people = sparkContext.parallelize[Person](peopleList).toDF
      people.registerTempTable("people")

      val teenagers = sqlContext.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

      val dataset = KiteDatasetSaver.saveAsKiteDataset(teenagers, datasetURI, Formats.PARQUET, CompressionType.Snappy)
      val reader = dataset.newReader()

      import collection.JavaConversions._
      reader.iterator().toList.sortBy(g => g.get("name").toString).mkString(",") must be("{\"name\": \"Ruben\", \"age\": 14},{\"name\": \"Vita\", \"age\": 19}")
      reader.close()

      cleanup()

    }
  }

  "Spark" must {
    "be able to create a kite avro dataset from a SchemaRDD/Dataframe" in {

      cleanup()

      val sqlContext = new SQLContext(sparkContext)
      import sqlContext.implicits._

      val datasetURI = URIBuilder.build(s"repo:file:////${System.getProperty("user.dir")}/tmp", "test", "persons")

      val peopleList = List(Person("David", 50), Person("Ruben", 14), Person("Giuditta", 12), Person("Vita", 19))
      val people = sparkContext.parallelize[Person](peopleList).toDF
      people.registerTempTable("people")

      val teenagers = sqlContext.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

      val dataset = KiteDatasetSaver.saveAsKiteDataset(teenagers, datasetURI, Formats.AVRO)
      val reader = dataset.newReader()

      import collection.JavaConversions._
      reader.iterator().toList.sortBy(g => g.get("name").toString).mkString(",") must be("{\"name\": \"Ruben\", \"age\": 14},{\"name\": \"Vita\", \"age\": 19}")
      reader.close()

      cleanup()

    }
  }

  "Spark" must {
    "be able to create a SchemaRDD/Dataframe from a kite avro dataset using a reflection based schema" in {

      cleanup()

      val sqlContext = new SQLContext(sparkContext)

      val datasetURI = URIBuilder.build(s"repo:file:////${System.getProperty("user.dir")}/tmp", "test", "persons")

      val descriptor = new DatasetDescriptor.Builder().schema(classOf[Person]).format(Formats.AVRO).build()

      val peopleDataset = Datasets.create[Person, Dataset[Person]](datasetURI, descriptor, classOf[Person])

      val writer = peopleDataset.newWriter()

      val peopleList = (1 to 1000).map(i => Person(s"person-$i", Random.nextInt(80)))
      peopleList.foreach(writer.write)
      writer.close()

      val data = sqlContext.kiteDatasetFile(peopleDataset)

      data.collect().sortBy(_.getAs[String](0).split("-")(1).toInt).map(row => Person(row.getAs[String](0), row.getAs[Int](1))) must be(peopleList)

      cleanup()

    }
  }

  override def afterAll() = {
    sparkContext.stop()
  }

}
