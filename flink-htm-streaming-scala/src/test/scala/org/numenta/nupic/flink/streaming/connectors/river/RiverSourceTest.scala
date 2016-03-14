package org.numenta.nupic.flink.streaming.connectors.river

import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.joda.time.DateTime
import org.junit.Test

class RiverSourceTest extends StreamingMultipleProgramsTestBase {

  @Test
  def testConstructor(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.addDefaultKryoSerializer(classOf[DateTime], classOf[JodaDateTimeSerializer])

    val source = new RiverSource[TrafficReport]("nyc-traffic")
    assert(source.partitions.size == 153)
  }

  @Test
  def testOrdering(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.addDefaultKryoSerializer(classOf[DateTime], classOf[JodaDateTimeSerializer])
    env.setParallelism(1)

    val source = new RiverSource[TrafficReport]("nyc-traffic", streamIds = Some(Seq("1", "119")))

    val stream = env.addSource(source)

    // verify that the elements are totally ordered
    stream.keyBy { _ => None }
      .mapWithState {
        (element, lastTimestamp: Option[DateTime]) =>
          lastTimestamp match {
            case Some(t) => assert(t.compareTo(element.datetime) <= 0)
            case None =>
          }
          (element, Some(element.datetime))
      }

    env.execute
  }
}

case class TrafficReport(streamId: String, datetime: DateTime, Speed: Double, TravelTime: Double)
