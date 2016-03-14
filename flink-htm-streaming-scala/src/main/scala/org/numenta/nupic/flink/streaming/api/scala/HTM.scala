package org.numenta.nupic.flink.streaming.api.scala

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.ClosureCleaner
import org.apache.flink.streaming.api.scala.DataStream
import org.numenta.nupic.flink.streaming.{api => jnupic}
import org.numenta.nupic.network.Network

import scala.reflect.ClassTag

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

  def network[T: TypeInformation : ClassTag](
      input: DataStream[T],
      factory: jnupic.NetworkFactory[T]): HTMStream[T] = {

    wrapStream(jnupic.HTM.network(input.javaStream, factory))
  }

  def network[T: TypeInformation : ClassTag](
       input: DataStream[T],
       fun: AnyRef => Network): HTMStream[T] = {
    implicit val config = input.executionConfig
    val factory: jnupic.NetworkFactory[T] = new jnupic.NetworkFactory[T] {
      val cleanFun = clean(fun)
      def createNetwork(key: AnyRef): Network = cleanFun(key)
    }
    wrapStream(jnupic.HTM.network(input.javaStream, factory))
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

  def select[R: TypeInformation : ClassTag](selector: jnupic.InferenceSelectFunction[T,R]): DataStream[R] = {
    val outType : TypeInformation[R] = implicitly[TypeInformation[R]]
    new DataStream[R](jstream.select(selector, outType))
  }

  def select[R: TypeInformation : ClassTag](fun: jnupic.Inference2[T] => R): DataStream[R] = {
    val outType : TypeInformation[R] = implicitly[TypeInformation[R]]
    val selector: jnupic.InferenceSelectFunction[T,R] = new jnupic.InferenceSelectFunction[T,R] {
      val cleanFun = clean(fun)
      def select(in: jnupic.Inference2[T]): R = cleanFun(in)
    }
    new DataStream[R](jstream.select(selector, outType))
  }
}
