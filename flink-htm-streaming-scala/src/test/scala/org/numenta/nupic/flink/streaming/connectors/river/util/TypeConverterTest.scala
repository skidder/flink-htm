package org.numenta.nupic.flink.streaming.connectors.river.util

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.scala._
import org.joda.time.{DateTimeZone, DateTime}
import org.scalatest.{Matchers, FlatSpec}
import spray.json.{JsNumber, JsString, JsArray}

/**
  * @author Eron Wright
  */
class TypeConverterTest extends FlatSpec with Matchers {

  implicit val executionConfig = new ExecutionConfig

  behavior of "TypeJsonConverter"

  it should "support known datatypes" in {

    var converter = new TypeConverter[TestClass](
      "testStream",
      Seq("datetime", "stringValue", "doubleValue"),
      DateTimeZone.UTC)

    val array = JsArray(JsString("2016/01/23 17:00:00"), JsString("a"), JsNumber(42.0))
    val record = converter.read(array)

    assert(record.streamId == "testStream")
    assert(record.datetime.getMillis == 1453568400000L)
    assert(record.stringValue == "a")
    assert(record.doubleValue == 42.0)
  }
}

case class TestClass(streamId: String, datetime: DateTime, stringValue: String, doubleValue: Double)
