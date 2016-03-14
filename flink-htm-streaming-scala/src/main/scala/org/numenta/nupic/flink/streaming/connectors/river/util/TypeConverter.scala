package org.numenta.nupic.flink.streaming.connectors.river.util

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.scala._
import org.apache.flink.streaming.util.FieldAccessor
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import spray.json._

import scala.reflect.ClassTag

/**
  * Converts JSON data as provided by River View to instances of a compatible user-defined type.
  */
class TypeConverter[T: TypeInformation : ClassTag]
    (streamId: String, jsFields: Seq[String], timezone: DateTimeZone)
    (implicit val executionConfig: ExecutionConfig) {

  import BasicTypeInfo._
  import TypeConverter._

  private val typeInfo = implicitly[TypeInformation[T]]match {
    case inputType:CompositeType[T] if inputType.hasField("datetime") => inputType
    case _ => throw new RuntimeException("unsupported type")
  }

  private val typeSerializer = typeInfo.createSerializer(executionConfig)

  private val DATETIME_FORMATTER = DateTimeFormat.forPattern("YYYY/MM/dd H:mm:ss").withZone(timezone)

  private type KeySetter = (T,String) => T

  private val keySetter: KeySetter = {
    val accessor = typeInfo match {
      case ti if ti.hasField("streamId") => Some(FieldAccessor.create[T,AnyRef]("streamId", typeInfo, executionConfig))
      case _ => None
    }
    accessor match {
      case Some(accessor) => (record: T, key: String) => accessor.set(record, key)
      case None => (record: T, key: String) => record
    }
  }

  private type ValueSetter = (T,JsValue) => T

  private val setters: Seq[ValueSetter] = jsFields.map { fieldName =>
    val accessor = FieldAccessor.create[T,AnyRef](fieldName, typeInfo, executionConfig)

    val converter = accessor.getFieldType match {
      case ti if ti == STRING_TYPE_INFO => (value: JsValue) => value.asInstanceOf[JsString].value
      case ti if ti == DOUBLE_TYPE_INFO => (value: JsValue) => Double.box(value.asInstanceOf[JsNumber].value.doubleValue())
      case ti if ti == DATETIME_TYPE_INFO => (value: JsValue) => DATETIME_FORMATTER.parseDateTime(value.asInstanceOf[JsString].value)
      case ti if ti == BOOLEAN_TYPE_INFO => (value: JsValue) => Boolean.box(value.asInstanceOf[JsBoolean].value.booleanValue())
      case ti if ti == INT_TYPE_INFO => (value: JsValue) => Int.box(value.asInstanceOf[JsNumber].value.intValue())
      case ti if ti == LONG_TYPE_INFO => (value: JsValue) => Long.box(value.asInstanceOf[JsNumber].value.longValue())
      case ti if ti == SHORT_TYPE_INFO => (value: JsValue) => Short.box(value.asInstanceOf[JsNumber].value.shortValue())
      case ti if ti == FLOAT_TYPE_INFO => (value: JsValue) => Float.box(value.asInstanceOf[JsNumber].value.floatValue())
      case _ => throw new IllegalArgumentException(s"unsupported field type [${accessor.getFieldType}]")
    }

    (record: T, value: JsValue) => {
      accessor.set(record, converter(value))
    }
  }

  def read(array: JsArray): T = {
    val record = keySetter(typeSerializer.createInstance(), streamId)
    array.elements.zip(setters).foldLeft(record) {
      case (record, (JsNull, converter)) => record
      case (record, (value, converter)) => converter(record, value) }
  }
}

object TypeConverter {
  private val DATETIME_TYPE_INFO = createTypeInformation[DateTime]
}
