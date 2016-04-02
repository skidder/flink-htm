package org.numenta.nupic.flink.streaming.examples.hotgym

import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala._
import org.joda.time.DateTime
import org.numenta.nupic.Parameters.KEY
import org.numenta.nupic.algorithms.{Anomaly, SpatialPooler, TemporalMemory}
import org.numenta.nupic.encoders.DateEncoder._
import org.numenta.nupic.encoders.scala._
import org.numenta.nupic.flink.streaming.examples.common.NetworkDemoParameters
import org.numenta.nupic.network.Network
import org.numenta.nupic.flink.streaming.api.scala._

trait HotGymModel {
  case class Consumption(timestamp: DateTime, consumption: Double)

  case class Prediction(timestamp: String, actual: Double, predicted: Double, anomalyScore: Double)

  val network = (key: AnyRef) => {
    val encoder = MultiEncoder(
      DateEncoder().name("timestamp").timeOfDay(21, 9.5),
      ScalarEncoder().name("consumption").n(50).w(21).maxVal(100.0).resolution(0.1).clipInput(true)
    )

    val params = NetworkDemoParameters.params

    val network = Network.create("HotGym Network", params)
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
  * Demonstrate the hotgym "basic network" as a Flink job.
  */
object Demo extends HotGymModel {
  def main(args: Array[String]) {

    val appArgs = ParameterTool.fromArgs(args)

    val env = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.getConfig.setGlobalJobParameters(appArgs)
      env.setParallelism(1)
      env.addDefaultKryoSerializer(classOf[DateTime], classOf[JodaDateTimeSerializer])
      env
    }

    /**
      * Parse the hotgym csv file as a datastream of consumption records.
      */
    def readCsvFile(path: String): DataStream[Consumption] = {
      env.readTextFile(appArgs.getRequired("input"))
        .map {
          _.split(",") match {
            case Array(timestamp, consumption) => Consumption(LOOSE_DATE_TIME.parseDateTime(timestamp), consumption.toDouble)
          }
        }
    }

    val hotGymConsumption: DataStream[Consumption] = readCsvFile(appArgs.getRequired("input"))

    val inferences: DataStream[Prediction] = hotGymConsumption
      .learn(network)
      .select { inference => inference }
      .keyBy { _ => None }
      .mapWithState { (inference, state: Option[Double]) =>

        val prediction = Prediction(
          inference.getInput.timestamp.toString(LOOSE_DATE_TIME),
          inference.getInput.consumption,
          state match {
            case Some(prediction) => prediction
            case None => 0.0
          },
          inference.getAnomalyScore)

        // store the prediction about the next value as state for the next iteration,
        // so that actual vs predicted is a meaningful comparison
        val predictedConsumption = inference.getClassification("consumption").getMostProbableValue(1).asInstanceOf[Any] match {
          case value: Double if value != 0.0 => value
          case _ => state.getOrElse(0.0)
        }

        (prediction, Some(predictedConsumption))
      }

    if (appArgs.has("output")) {
      inferences.writeAsCsv(appArgs.getRequired("output"), writeMode = WriteMode.OVERWRITE)
    }
    else {
      inferences.print()
    }

    env.execute("hotgym")
  }
}

