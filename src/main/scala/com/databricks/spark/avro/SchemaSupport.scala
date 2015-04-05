package com.databricks.spark.avro

import org.apache.avro.{ Schema, SchemaBuilder }
import org.apache.spark.sql.DataFrame

trait SchemaSupport {
  def getSchema(dataFrame: DataFrame): Schema = {
    val builder = SchemaBuilder.record("topLevelRecord")
    val schema: Schema = SchemaConverters.convertStructToAvro(dataFrame.schema, builder)
    schema
  }
}
