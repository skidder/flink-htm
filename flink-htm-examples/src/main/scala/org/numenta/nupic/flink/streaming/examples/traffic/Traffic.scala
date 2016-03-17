package org.numenta.nupic.flink.streaming.examples.traffic

import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, Interval}
import org.numenta.nupic.Parameters.KEY
import org.numenta.nupic.algorithms.{Anomaly, SpatialPooler, TemporalMemory}
import org.numenta.nupic.encoders.scala._
import org.numenta.nupic.flink.streaming.connectors.river._
import org.numenta.nupic.flink.streaming.examples.common.NetworkDemoParameters
import org.numenta.nupic.network.Network
import org.numenta.nupic.flink.streaming.api.scala._

trait TrafficModel {

  case class TrafficReport(streamId: String, datetime: DateTime, Speed: Double, TravelTime: Double)

  val network = (key: AnyRef) => {
    val encoder = MultiEncoder(
      DateEncoder().name("datetime").timeOfDay(21, 9.5),
      ScalarEncoder().name("Speed").n(50).w(21).maxVal(80.0).resolution(1).clipInput(true)
    )

    val params = NetworkDemoParameters.params

    val network = Network.create(s"TrafficModel-$key", params)
      .add(Network.createRegion("Region 1")
        .add(Network.createLayer("Layer 2/3", params)
          .alterParameter(KEY.AUTO_CLASSIFY, true)
          .add(encoder)
          .add(Anomaly.create())
          .add(new TemporalMemory())
          .add(new SpatialPooler())))
    network.setEncoder(encoder)
    network
  }
}

/**
  * Traffic Anomaly Tutorial
  */
object Demo extends TrafficModel {
  def main(args: Array[String]) {

    lazy val appArgs = ParameterTool.fromArgs(args)

    lazy val env = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
      env.addDefaultKryoSerializer(classOf[DateTime], classOf[JodaDateTimeSerializer])
      env
    }

    val nycTraffic = new RiverSource[TrafficReport]("nyc-traffic", streamIds = Some(Seq("1", "204", "446")))

    val investigationInterval = {
      val formatter = ISODateTimeFormat.basicDate().withZone(nycTraffic.meta.timezone)
      new Interval(DateTime.parse("20150803", formatter), DateTime.parse("20150804", formatter))
    }

    val anomalyScores = env
      .addSource(nycTraffic)
      .filter { report => report.datetime.isBefore(investigationInterval.getEnd) }
      .keyBy("streamId")
      .learn(network)
      .select(inference => (inference.input, inference.anomalyScore))

    val anomalousRoutes = anomalyScores
      .filter { anomaly => investigationInterval.contains(anomaly._1.datetime) }
      .filter { anomaly => anomaly._2 >= 0.9 }
      .map { anomaly => (anomaly._1.datetime, anomaly._1.streamId, anomaly._2) }

    if (appArgs.has("output")) {
      anomalousRoutes.writeAsCsv(appArgs.getRequired("output"), writeMode = WriteMode.OVERWRITE).setParallelism(1)
    }
    else {
      anomalousRoutes.print()
    }

    env.execute("nyc-traffic")
  }
}



