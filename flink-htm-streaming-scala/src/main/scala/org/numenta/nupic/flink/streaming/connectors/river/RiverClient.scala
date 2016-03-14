package org.numenta.nupic.flink.streaming.connectors.river

import akka.actor.{ActorRefFactory, ActorSystem}
import spray.http.Uri.{Query, Path}
import spray.httpx.SprayJsonSupport

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import akka.pattern.ask
import spray.http._
import spray.client.pipelining._
import RiverProtocol._
import SprayJsonSupport._

/**
  * River View client.
  *
  * @author Eron Wright
  */
class RiverClient()(implicit actorRefFactory: ActorRefFactory, executionContext: ExecutionContext) {
  import RiverClient._

  /**
    * The pipeline for river metadata requests.
    */
  val meta: HttpRequest => Future[RiverMeta] = sendReceive ~> unmarshal[RiverMeta]

  /**
    * The URI for the metadata for a river.
    * @param river the river to refer to
    * @return a URI
    */
  def metaUri(river: String): Uri =
    Uri("http://data.numenta.org").withPath(Path /river/"meta.json")

  /**
    * The pipeline for river key requests.
    */
  val keys: HttpRequest => Future[RiverKeys] = sendReceive ~> unmarshal[RiverKeys]

  /**
    * The URI for the keys for a river.
    * @param river the river to refer to
    * @return a URI
    */
  def keysUri(river: String): Uri =
    Uri("http://data.numenta.org").withPath(Path /river/"keys.json")

  /**
    * The pipeline for stream data requests.
    */
  val data: HttpRequest => Future[StreamWindow] = sendReceive ~> unmarshal[StreamWindow]

  /**
    * The URI for a window of stream data.
    * @param river the river to refer to
    * @param streamId the stream to refer to
    * @param since a timestamp to request events after
    * @return a URI
    */
  def dataUri(river: String, streamId: String, since: Long = 0, limit: Int = windowLimit): Uri =
    Uri("http://data.numenta.org")
      .withPath(Path /river/streamId/"data.json")
      .withQuery(Query(("limit", limit.toString), ("since", since.toString)))
}

object RiverClient {
  val windowLimit: Int = 5000
}