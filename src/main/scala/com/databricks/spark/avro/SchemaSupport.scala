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

package com.databricks.spark.avro

import org.apache.avro.{ Schema, SchemaBuilder }
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType

import scala.language.existentials

trait SchemaSupport {
  private def invokePrivate(x: AnyRef, methodName: String, _args: Any*): Any = {
    val args = _args.map(_.asInstanceOf[AnyRef])
    def _parents: Stream[Class[_]] = Stream(x.getClass) #::: _parents.map(_.getSuperclass)
    val parents = _parents.takeWhile(_ != null).toList
    val methods = parents.flatMap(_.getDeclaredMethods)
    val method = methods.find(_.getName == methodName).getOrElse(throw new IllegalArgumentException("Method " + methodName + " not found"))
    method.setAccessible(true)
    method.invoke(x, args: _*)
  }

  def createConverter(sqlContext: SQLContext, schema: Schema): Any => Any = invokePrivate(AvroRelation("", Some(getSchemaType(schema)), 0)(sqlContext), "com$databricks$spark$avro$AvroRelation$$createConverter", schema).asInstanceOf[(Any) => Any]

  def getSchema(structType: StructType): Schema = {
    val builder = SchemaBuilder.record("topLevelRecord")
    val schema: Schema = SchemaConverters.convertStructToAvro(structType, builder)
    schema
  }

  def getSchemaType(schema: Schema): StructType = SchemaConverters.toSqlType(schema).dataType.asInstanceOf[StructType]

}
