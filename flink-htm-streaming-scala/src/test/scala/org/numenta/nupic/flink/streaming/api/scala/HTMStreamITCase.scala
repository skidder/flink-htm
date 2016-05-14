package org.numenta.nupic.flink.streaming.api.scala

import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.junit.Test
import org.apache.flink.streaming.api.scala._

import org.numenta.nupic.flink.streaming.api.TestHarness.{DayDemoNetworkFactory, DayDemoRecord, DayDemoRecordSourceFunction}
import org.numenta.nupic.flink.streaming.api.scala._

class HTMStreamITCase extends StreamingMultipleProgramsTestBase {

  @Test
  def testResetLambda(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.enableCheckpointing(1000)

    env
      .addSource(new DayDemoRecordSourceFunction(1, false))
      .learn(new DayDemoNetworkFactory)
      .resetOn(input => input.dayOfWeek == 0)
      .select(inference => (inference._1.dayOfWeek, inference._2.getAnomalyScore))
      .print()

    env.execute()
  }
}
