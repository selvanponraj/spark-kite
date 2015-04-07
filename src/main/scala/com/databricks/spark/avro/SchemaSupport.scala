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

import java.nio.ByteBuffer
import java.util

import org.apache.avro.Schema.Type._
import org.apache.avro.generic.GenericData.Fixed
import org.apache.avro.generic.{ GenericData, GenericRecord }
import org.apache.avro.{ Schema, SchemaBuilder }
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ DataFrame, Row }

import scala.collection.JavaConversions._

trait SchemaSupport {
  def getSchema(dataFrame: DataFrame): Schema = {
    val builder = SchemaBuilder.record("topLevelRecord")
    val schema: Schema = SchemaConverters.convertStructToAvro(dataFrame.schema, builder)
    schema
  }

  def getSchemaType(schema: Schema): StructType = SchemaConverters.toSqlType(schema).dataType.asInstanceOf[StructType]

  def createConverter(schema: Schema): Any => Any = {
    schema.getType match {
      case STRING | ENUM =>
        // Avro strings are in Utf8, so we have to call toString on them
        (item: Any) => if (item == null) null else item.toString
        case INT | BOOLEAN | DOUBLE | FLOAT | LONG =>
        (item: Any) => item

        case BYTES =>
        (item: Any) => {
          if (item == null) {
            null
          } else {
            val avroBytes = item.asInstanceOf[ByteBuffer]
            val javaBytes = new Array[Byte](avroBytes.remaining)
            avroBytes.get(javaBytes)
            javaBytes
          }
        }

        case FIXED =>
        // Byte arrays are reused by avro, so we have to make a copy of them.
        (item: Any) => if (item == null) null else item.asInstanceOf[Fixed].bytes.clone()

        case RECORD =>
        val fieldConverters = schema.getFields.map(f => createConverter(f.schema))

        (item: Any) => {
          if (item == null) {
            null
          } else {
            val record = item.asInstanceOf[GenericRecord]
            val converted = new Array[Any](fieldConverters.size)
            var idx = 0
            while (idx < fieldConverters.size) {
              converted(idx) = fieldConverters.apply(idx)(record.get(idx))
              idx += 1
            }

            Row.fromSeq(converted.toSeq)
          }
        }

        case ARRAY =>
        val elementConverter = createConverter(schema.getElementType)

        (item: Any) => {
          if (item == null) {
            null
          } else {
            val avroArray = item.asInstanceOf[GenericData.Array[Any]]
            val convertedArray = new Array[Any](avroArray.size)
            var idx = 0
            while (idx < avroArray.size) {
              convertedArray(idx) = elementConverter(avroArray(idx))
              idx += 1
            }
            convertedArray.toSeq
          }
        }

        case MAP =>
        val valueConverter = createConverter(schema.getValueType)

        (item: Any) => {
          if (item == null) {
            null
          } else {
            // Avro map keys are always strings, so it's enough to just call toString on them.
            item.asInstanceOf[util.Map[Any, Any]].map(x => (x._1.toString, valueConverter(x._2))).toMap
          }
        }

        case UNION =>
        if (schema.getTypes.exists(_.getType == NULL)) {
          val remainingUnionTypes = schema.getTypes.filterNot(_.getType == NULL)
          if (remainingUnionTypes.size == 1) {
            createConverter(remainingUnionTypes.get(0))
          } else {
            createConverter(Schema.createUnion(remainingUnionTypes))
          }
        } else schema.getTypes.map(_.getType) match {
          case Seq(t1, t2) if Set(t1, t2) == Set(INT, LONG) =>
            (item: Any) => {
              item match {
                case l: Long => l
                case i: Int => i.toLong
                case null => null
              }
            }
            case Seq(t1, t2) if Set(t1, t2) == Set(FLOAT, DOUBLE) =>
            (item: Any) => {
              item match {
                case d: Double => d
                case f: Float => f.toDouble
                case null => null
              }
            }
            case other =>
            sys.error(s"This mix of union types is not supported (see README): $other")
        }

      case other => sys.error(s"Unsupported type $other")
    }
  }

}
