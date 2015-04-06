package com.cloudera.spark

import com.databricks.spark.avro.SchemaSupport
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.{DataFrame, Row}
import org.kitesdk.data._
import org.kitesdk.data.mapreduce.DatasetKeyInputFormat

package object kite extends SchemaSupport {

  implicit class KiteDatasetContext(sqlContext: org.apache.spark.sql.SQLContext) {
    def kiteDatasetFile(dataSet: Dataset[_], minPartitions: Int = 0): DataFrame = {
      val job = Job.getInstance()
      DatasetKeyInputFormat.configure(job).readFrom(dataSet).withType(classOf[GenericRecord])
      val rdd = sqlContext.sparkContext.newAPIHadoopRDD(job.getConfiguration, classOf[DatasetKeyInputFormat[GenericRecord]], classOf[GenericRecord], classOf[Void])
      val converter = createConverter(dataSet.getDescriptor.getSchema)
      val rel = rdd.map(record => converter(record._1).asInstanceOf[Row])
      sqlContext.createDataFrame(rel, getSchemaType(dataSet.getDescriptor.getSchema))
    }
  }

}
