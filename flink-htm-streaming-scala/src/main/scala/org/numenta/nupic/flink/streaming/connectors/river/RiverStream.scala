package org.numenta.nupic.flink.streaming.connectors.river

import org.joda.time.DateTimeZone
import spray.json.JsArray

/**
  * A window of stream data.
  */
case class StreamWindow(
    name: String,
    `type`: String,
    timezone: DateTimeZone,
    id: String,
    headers: Seq[String],
    data: Seq[JsArray],
    meta: StreamMeta,
    urls: Option[StreamUrls])

/**
  * The metadata associated with a stream.
  */
case class StreamMeta(
    count: Long,
    since: Option[StreamMetaTime],
    until: Option[StreamMetaTime],
    duration: Option[String])

/**
  * A time value associated with a stream window.
  */
case class StreamMetaTime(timestamp: Long, timestring: String)

/**
  * Paging information associated with a stream window.
  */
case class StreamUrls(next: String, prev: String)

/**
  * An enumeration of stream types.
  */
object StreamTypes {
  sealed trait StreamType
  object Scalar extends StreamType
}


