package com.cloudera.spark

import org.apache.spark.sql.DataFrame
import org.kitesdk.data._

package object kite {

  implicit class KiteDatasetContext(sqlContext: org.apache.spark.sql.SQLContext) {
    def kiteDatasetFile(dataSet: Dataset[_], minPartitions: Int = 0): DataFrame = {
      import com.databricks.spark.avro._
      dataSet.getDescriptor.getFormat match {
        case Formats.AVRO => sqlContext.avroFile(dataSet.getDescriptor.getLocation.getRawPath, minPartitions)
        case Formats.PARQUET => sqlContext.parquetFile(dataSet.getDescriptor.getLocation.getRawPath)
      }
    }
  }

}
