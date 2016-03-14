package org.numenta.nupic.flink.streaming.connectors.river

import akka.actor.ActorSystem
import org.scalatest.{Matchers, FlatSpec}
import org.scalatest.concurrent.ScalaFutures

import spray.client.pipelining._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}
import org.scalatest.time.{Millis, Seconds, Span}


class RiverClientSuite extends FlatSpec with Matchers with ScalaFutures {

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(500, Millis))

  implicit val actorSystem = ActorSystem("RiverClientSuite")
  import actorSystem.dispatcher

  val riverClient = new RiverClient()
  import riverClient._

  behavior of "RiverClient"

  it should "download keys" in {
    val request = riverClient.keys(Get(keysUri("nyc-traffic")))
    whenReady(request) {  result =>
      println(s"result: ${result.keys.length} keys")
      result.keys.length should equal(153)
    }
  }

  it should "download windows of data" in {
    val request = riverClient.data(Get(dataUri("nyc-traffic", "1", since = 0)))
    whenReady(request) {  result =>
      println(s"result: ${result.meta.count} items")
      result.meta.count should equal(1559)
    }
  }
}
