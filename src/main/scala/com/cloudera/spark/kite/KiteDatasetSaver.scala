package com.cloudera.spark.kite

import java.net.URI

import com.databricks.spark.avro.{ SchemaSupport, AvroSaver }
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData.Record
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types.StructType
import org.kitesdk.data._
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat

object KiteDatasetSaver extends SchemaSupport {
  private def rowsToAvro(rows: Iterator[Row], schema: StructType): Iterator[(Record, Null)] = {
    val converter = AvroSaver.createConverter(schema, "topLevelRecord")
    rows.map(x => (converter(x).asInstanceOf[Record], null))
  }

  def saveAsKiteDataset(dataFrame: DataFrame, uri: URI, format: Format = Formats.AVRO): Dataset[Record] = {
    assert(URIBuilder.DATASET_SCHEME == uri.getScheme, s"Not a dataset or view URI: $uri" + "")
    val job = Job.getInstance()
    val builder = SchemaBuilder.record("topLevelRecord")
    val schema = dataFrame.schema
    val avroSchema = getSchema(dataFrame)
    val descriptor = new DatasetDescriptor.Builder().schema(avroSchema).format(format).build()
    val dataset = Datasets.create[Record, Dataset[Record]](uri, descriptor, classOf[Record])
    DatasetKeyOutputFormat.configure(job).writeTo(dataset)
    dataFrame.mapPartitions(rowsToAvro(_, schema)).saveAsNewAPIHadoopDataset(job.getConfiguration)
    dataset
  }
}
