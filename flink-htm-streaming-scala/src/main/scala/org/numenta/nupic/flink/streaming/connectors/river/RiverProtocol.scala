package org.numenta.nupic.flink.streaming.connectors.river

import org.joda.time.DateTimeZone
import spray.http.Uri
import spray.http.Uri.{Query, Path}
import spray.json._

/**
  * The River JSON message formats.
  */
object RiverProtocol extends DefaultJsonProtocol {

  implicit object DateTimeFormat extends RootJsonFormat[DateTimeZone] {
    def write(obj: DateTimeZone): JsValue = JsString(obj.getID)
    def read(json: JsValue): DateTimeZone = json match {
      case JsString(s) if DateTimeZone.getAvailableIDs.contains(s) => DateTimeZone.forID(s)
      case _ => throw new DeserializationException("valid timezone ID expected")
    }
  }

  implicit val streamUrls = jsonFormat2(StreamUrls.apply)

  implicit val streamMetaTime = jsonFormat2(StreamMetaTime.apply)

  implicit val streamMeta = jsonFormat4(StreamMeta.apply)

  implicit val streamData = jsonFormat8(StreamWindow.apply)

  implicit val riverKey = jsonFormat2(RiverKey.apply)

  implicit val riverKeys = jsonFormat2(RiverKeys.apply)

  implicit val riverUrls = jsonFormat2(RiverUrls.apply)

  implicit val riverMeta = jsonFormat13(RiverMeta.apply)

}

case class RiverMeta(
    urls: RiverUrls, `type`: String, description: String,
    poweredBy: String, author: String, email: String, timezone: DateTimeZone, sources: Seq[String],
    interval: String, expires: String, fields: Seq[String], metadata: Seq[String], name: String)

case class RiverUrls(keys: String, meta: String)

case class RiverKeys(name: String, keys: Vector[RiverKey])

case class RiverKey(name: String, urlEncodedName: String)
