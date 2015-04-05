package com.cloudera.spark.kite

import org.apache.spark.sql.SQLContext
import org.apache.spark.{ SparkConf, SparkContext }
import org.kitesdk.data.{ Formats, URIBuilder }
import org.scalatest.{ BeforeAndAfterAll, MustMatchers, WordSpec }

case class Person(name: String, age: Int)

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

      val products = generateDataset(Formats.AVRO)

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
    "be able to create a kite dataset from a SchemaRDD/Dataframe" in {

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

  override def afterAll() = {
    sparkContext.stop()
  }

}
