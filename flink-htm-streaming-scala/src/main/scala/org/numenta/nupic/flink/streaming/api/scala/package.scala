package org.numenta.nupic.flink.streaming.api

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._
import org.numenta.nupic.network.Network
import reflect.ClassTag

/**
  * Provides implicit conversions for HTM.
  */
package object scala {

  /**
    * Enrich a [[DataStream]] to directly support HTM learning.
    * @param dataStream
    * @tparam T
    */
  implicit class RichDataStream[T: TypeInformation : ClassTag](dataStream: DataStream[T]) {

    def learn(network: AnyRef => Network): scala.HTMStream[T] = {
      HTM.network(dataStream, network)
    }
  }
}
