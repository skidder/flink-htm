package org.numenta.nupic.flink.streaming.api.scala

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.scala.ClosureCleaner
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.function.StatefulFunction
import org.numenta.nupic.flink.streaming.{api => jnupic}
import org.numenta.nupic.network.Network

import scala.reflect.ClassTag

/**
  * Convenience methods for HTM streams.
  */
object HTM {

  private[nupic] def clean[F <: AnyRef](f: F, checkSerializable: Boolean = true)(implicit config: ExecutionConfig): F = {
    if (config.isClosureCleanerEnabled) {
      ClosureCleaner.clean(f, checkSerializable)
    }
    ClosureCleaner.ensureSerializable(f)
    f
  }

  private def wrapStream[T: TypeInformation : ClassTag](stream: jnupic.HTMStream[T]): HTMStream[T] = {
    new HTMStream(stream)
  }

  /**
    * Create an HTM stream based on the current [[DataStream]].
    *
    * @param input the input data stream to model.
    * @param factory the factory to create the HTM network.
    * @tparam T the type of the input elements.
    * @return an HTM stream to select results.
    */
  def learn[T: TypeInformation : ClassTag](
      input: DataStream[T],
      factory: jnupic.NetworkFactory[T]): HTMStream[T] = {

    wrapStream(jnupic.HTM.learn(input.javaStream, factory))
  }

  /**
    * Create an HTM stream based on the current [[DataStream]].
    *
    * @param input the input data stream to model.
    * @param fun the factory to create the HTM network.
    * @tparam T the type of the input elements.
    * @return an HTM stream to select results.
    */
  def learn[T: TypeInformation : ClassTag](
       input: DataStream[T],
       fun: AnyRef => Network): HTMStream[T] = {
    implicit val config = input.executionConfig
    val factory: jnupic.NetworkFactory[T] = new jnupic.NetworkFactory[T] {
      val cleanFun = clean(fun)
      def createNetwork(key: AnyRef): Network = cleanFun(key)
    }
    wrapStream(jnupic.HTM.learn(input.javaStream, factory))
  }
}

/**
  * Represents an HTM stream of inferences.
  *
  * @tparam T the input type of the input data about which inferences are made
  */
final class HTMStream[T: TypeInformation : ClassTag](jstream: jnupic.HTMStream[T]) {
  import HTM._

  implicit val config = jstream.getExecutionEnvironment.getConfig

  /**
    * Select output elements from the HTM stream.
    *
    * @param selector the select function.
    * @tparam R the type of the output elements.
    * @return a new data stream.
    */
  def select[R: TypeInformation](selector: MapFunction[jnupic.Inference2[T],R]): DataStream[R] = {
    val outType : TypeInformation[R] = implicitly[TypeInformation[R]]
    new DataStream[R](jstream.select(selector, outType))
  }

  /**
    * Select output elements from the HTM stream with the given function.
    *
    * @param fun the select function.
    * @tparam R the type of the output elements.
    * @return a new data stream.
    */
  def select[R: TypeInformation](fun: jnupic.Inference2[T] => R): DataStream[R] = {
    val outType : TypeInformation[R] = implicitly[TypeInformation[R]]
    val selector: jnupic.InferenceSelectFunction[T,R] = new jnupic.InferenceSelectFunction[T,R] {
      val cleanFun = clean(fun)
      def map(in: jnupic.Inference2[T]): R = cleanFun(in)
    }
    new DataStream[R](jstream.select(selector, outType))
  }

  /**
    * Select output elements from the HTM stream with the given stateful function.
    *
    * @param fun the stateful select function.
    * @tparam R the type of the output elements.
    * @tparam S the type of the state.
    * @return a new data stream.
    */
  def selectWithState[R: TypeInformation, S: TypeInformation](
      fun: (jnupic.Inference2[T], Option[S]) => (R, Option[S])): DataStream[R] = {
    if (fun == null) {
      throw new NullPointerException("Select function must not be null.")
    }

    val cleanFun = clean(fun)
    val stateTypeInfo: TypeInformation[S] = implicitly[TypeInformation[S]]
    val serializer: TypeSerializer[S] = stateTypeInfo.createSerializer(config)

    val mapper = new RichMapFunction[jnupic.Inference2[T], R] with StatefulFunction[jnupic.Inference2[T], R, S] {

      override val stateSerializer: TypeSerializer[S] = serializer

      override def map(in: jnupic.Inference2[T]): R = {
        applyWithState(in, cleanFun)
      }
    }

    select(mapper)
  }
}
