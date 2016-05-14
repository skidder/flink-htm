package org.numenta.nupic.flink.streaming.api

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._
import org.numenta.nupic.network.Network
import reflect.ClassTag
import org.numenta.nupic.flink.streaming.{api => jnupic}

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

    /**
      * Create an HTM stream based on the current [[DataStream]].
      * @param network a function producing the network to use.
      * @return an HTM stream.
      */
    def learn(network: AnyRef => Network): scala.HTMStream[T] = {
      HTM.learn(dataStream, network)
    }

    /**
      * Create an HTM stream based on the current [[DataStream]].
      * @param factory a factory producing the network to use.
      * @return an HTM stream.
      */
    def learn(factory: jnupic.NetworkFactory[T]): scala.HTMStream[T] = {
      HTM.learn(dataStream, factory)
    }
  }
}
