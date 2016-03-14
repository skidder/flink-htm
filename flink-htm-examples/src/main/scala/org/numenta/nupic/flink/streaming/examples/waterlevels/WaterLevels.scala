package org.numenta.nupic.flink.streaming.examples.waterlevels

import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.numenta.nupic.flink.streaming.connectors.river._

/**
  *
  * Basic demonstration of processing the mo-water-levels data set.
  */
object Demo extends App {

  /**
    * The command-line arguments as parsed by {@Link ParameterTool}
    */
  lazy val appArgs = ParameterTool.fromArgs(this.args)

  def prettyPrint(w: WaterLevel) = (w.streamId, DATETIME_FORMATTER.print(w.datetime), w.Stage, w.Flow)

  private val DATETIME_FORMATTER = DateTimeFormat.forPattern("YYYY/MM/dd H:mm:ss")

  /**
    * The configured Flink execution environment.
    */
  lazy val env = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(appArgs)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1) // TODO: don't assume local mode
    env.addDefaultKryoSerializer(classOf[DateTime], classOf[JodaDateTimeSerializer])
    env
  }

  val source = new RiverSource[WaterLevel]("mo-water-levels") //("nyc-traffic")
  val stream = env.addSource(source)
  stream.map(prettyPrint _).writeAsCsv("build/output/mowaterlevels-raw.csv", writeMode = WriteMode.OVERWRITE)

  stream.keyBy("streamId").timeWindow(Time.days(1)).max("Flow")
    .map(prettyPrint _).writeAsCsv("build/output/mowaterlevels-max-per-day.csv", writeMode = WriteMode.OVERWRITE)

  val maxFlowPerMonth = stream.keyBy("streamId").timeWindow(Time.days(1)).maxBy("Flow")
  maxFlowPerMonth.map(prettyPrint _).writeAsCsv("build/output/mowaterlevels-monthly.csv", writeMode = WriteMode.OVERWRITE)

  env.execute("mo-water-levels")
}

case class WaterLevel(streamId: String, datetime: DateTime, Stage: Double, Flow: Double)
