package org.numenta.nupic.flink.streaming.connectors.river

import java.util.concurrent.Executors

import akka.actor.ActorSystem
import com.typesafe.config.{ConfigFactory, Config}
import RiverSource._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.operators.sort.MergeIterator
import org.apache.flink.streaming.api.checkpoint.Checkpointed
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.util.FieldAccessor
import org.apache.flink.util.MutableObjectIterator
import org.joda.time.{DateTime, DateTimeZone}
import org.numenta.nupic.flink.streaming.connectors.river.util.TypeConverter
import org.slf4j.{Logger, LoggerFactory}
import spray.client.pipelining._

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.concurrent.{Future, Await, ExecutionContext}
import scala.reflect.ClassTag
import scala.util.{Failure, Success}

/**
  * A Flink connector for Numenta River View.
  *
  * To use the connector, use `StreamExecutionEnvironment.addSource(sourceFunction)`.
  *
  * All river streams are read in parallel to provide a totally-ordered event stream.
  * Increase the parallelism of the source to improve performance.  This source supports
  * event-time semantics.
  *
  * @param river the river to read.
  * @param since controls the start time of the stream (in event time).
  * @tparam T the type of element to produce.
  */
class RiverSource[T >: Null <: AnyRef: TypeInformation : ClassTag](
    river: String,
    since: Option[DateTime] = None,
    streamIds: Option[Seq[String]] = None)
  extends RichSourceFunction[T] with Checkpointed[RiverSourceCheckpointState] { source =>

  private val LOG: Logger = LoggerFactory.getLogger(classOf[RiverSource[_]])

  val typeInfo = implicitly[TypeInformation[T]].asInstanceOf[CompositeType[T]]

  /**
    * The river metadata and streams to be handled by this source.
    */
  val (meta: RiverMeta, partitions: Seq[String]) = {
    implicit val actorSystem = createActorSystem()
    try {
      import actorSystem.dispatcher

      val riverClient = new RiverClient()
      import riverClient._

      val metaRequest = riverClient.meta(Get(metaUri(river)))
      val keysRequest: Future[Seq[String]] = streamIds match {
        case None => riverClient.keys(Get(keysUri(river))).map { _.keys.map { _.name } }
        case Some(keys) => Future { keys }
      }

      val metaResult = Await.result(metaRequest, 30 seconds)
      val keysResult = Await.result(keysRequest, 30 seconds)

      (metaResult, keysResult)
    }
    finally {
      actorSystem.shutdown()
    }
  }

  // ------------------------------------------------------------------------
  // task lifecyle
  //
  // the source is serialized then copied to each parallel subtask
  // ------------------------------------------------------------------------

  /**
    * Open the source in a task context.
    * @param parameters
    */
  override def open(parameters: Configuration): Unit = {
    actorSystem = Some(createActorSystem())
  }

  override def close(): Unit = {
    actorSystem.foreach { _.shutdown() }
  }

  private var actorSystem: Option[ActorSystem] = None

  /**
    * The thread which runs the data stream's pump, downloading and emitting ordered elements.
    */
  @transient
  private var pump: Option[Thread] = None

  /**
    * Run the pump, blocking until the data stream is complete.
    */
  override def run(sourceContext: SourceContext[T]): Unit = {

    // divvy up the river streams among the parallel subtasks
    val keysForThisSubtask = partitions.filter(key =>
      Math.abs(key.hashCode % getRuntimeContext.getNumberOfParallelSubtasks) == getRuntimeContext.getIndexOfThisSubtask)

    // start the pump
    pump = Some(new Thread(new Pump(keysForThisSubtask, sourceContext)(actorSystem.get)))
    pump.foreach {pump =>
      pump.start()
      pump.join()
    }
  }

  /**
    * Cancel execution of the source.
    */
  override def cancel(): Unit = {
    pump.foreach { _.interrupt() }
  }

  /**
    * The event time of the most recently collected element
    */
  private var lastEventTimestamp: Long = since.map(_.getMillis).getOrElse(0)

  /**
    * A pump that downloads the configured streams, applies a total ordering, and emits elements.
    *
    * @param keys the set of river streams to handle.
    * @param sourceContext the source context
    */
  class Pump(keys: Seq[String], sourceContext: SourceContext[T])(implicit actorSystem: ActorSystem) extends Runnable {

    import actorSystem.dispatcher

    val riverClient = new RiverClient() // fixme: extract the heavyweight actorsystem object, and call shutdown
    import riverClient._

    implicit val executionConfig = getRuntimeContext.getExecutionConfig

    private val datetimeAccessor = FieldAccessor.create[T,DateTime]("datetime", typeInfo, executionConfig)

    /**
      * The internal iterator over the totally-ordered elements across all streams.
      */
    private val iterator: MutableObjectIterator[T] = {
      keys.length match {
          // return an empty iterator
        case 0 => new MutableObjectIterator[T] {
          override def next(reuse: T): T = null
          override def next(): T = null
        }
        case 1 =>
          // use the single river stream directly
          makeIterator(keys.head)
        case _ =>
          // use a MergeIterator to merge the various river streams
          val comparator = typeInfo.createComparator(Array(typeInfo.getFieldIndex("datetime")), Array(true), 0, executionConfig)
          new MergeIterator(keys.map(makeIterator).toList, comparator)
      }
    }

    /**
      * Run the pump.
      */
    def run(): Unit = {
      var element: T = null
      do {
        element = iterator.next(element)
        if(element != null) {
          sourceContext.getCheckpointLock.synchronized {
            val timestamp = datetimeAccessor.get(element)
            assert(lastEventTimestamp <= timestamp.getMillis)

            sourceContext.collectWithTimestamp(element, lastEventTimestamp)
            sourceContext.emitWatermark(new Watermark(lastEventTimestamp))

            lastEventTimestamp = timestamp.getMillis
          }
        }
      } while (element != null && !Thread.currentThread().isInterrupted)
    }

    /**
      * Make an iterator for a given river stream.
      *
      * The iterator handles paging over the stream, blocking as necessary.   Note that the first page
      * must be downloaded for +all+ streams before the first element may be emitted (to achieve a total ordering).
      * Therefore, requests are issued eagerly (and in parallel) for the first page, then lazily for subsequent pages.
      * We rely on Spray's internal queueing to limit the # of simultaneous connections to River View.
      *
      * @param streamId the stream to download.
      * @return an iterator over the river stream's elements
      */
    private def makeIterator(streamId: String): MutableObjectIterator[T] = {
      new MutableObjectIterator[T] {

        /**
          * Converts JSON records to instances of the user-supplied type
          */
        private val typeConverter = new TypeConverter[T](streamId, Seq("datetime") ++ source.meta.fields, source.meta.timezone)

        /**
          * An iterator over the windows of a particular stream.
          */
        private def paged(): Iterator[StreamWindow] = {

          LOG.debug(s"requesting the initial window (${dataUri(river, streamId, since = lastEventTimestamp)})")
          val first = riverClient.data(Get(dataUri(river, streamId, since = lastEventTimestamp)))

          new Iterator[StreamWindow] {
            var current = first

            override def hasNext: Boolean = {
              Await.ready(current, Duration.Inf)
              current.value match {
                case None => throw new RuntimeException
                case Some(Success(window)) => window.urls.isDefined
                case Some(Failure(e)) => throw e
              }
            }

            override def next(): StreamWindow = {
              assert(current.isCompleted)
              val Some(Success(window)) = current.value
              LOG.debug(s"requesting a window (${window.urls.get.next})")
              current = riverClient.data(Get(window.urls.get.next))
              window
            }
          }
        }

        /**
          * A lazy iterator over the paged elements.
          */
        private val iterator: Iterator[T] = paged().flatMap {
          window => window.data.map(typeConverter.read)
        }

        /**
          * Obtain the next stream element from the queue.
          */
        override def next(): T = {
          if (!iterator.hasNext) null
          else iterator.next()
        }

        override def next(reuse: T): T = next()
      }
    }
  }

  // ------------------------------------------------------------------------
  //  Checkpoint and restore
  // ------------------------------------------------------------------------

  /**
    * Checkpoint the state of the source.
    *
    * This source's state consists of the event time of the last collected element.
    *
    * @param checkpointId a monotonically increasing checkpoint id
    * @param checkpointTimestamp the processing time at which the checkpoint was initiated.
    * @return the checkpointed state
    */
  override def snapshotState(checkpointId: Long, checkpointTimestamp: Long): RiverSourceCheckpointState = {
    LOG.debug(s"snapshotState called (checkpointId=$checkpointId, checkpointTimestamp=$checkpointTimestamp, lastEventTimestamp=$lastEventTimestamp)")
    RiverSourceCheckpointState(lastEventTimestamp)
  }

  /**
    * Restore to a previous checkpointed state.
    *
    * This method is called before the open method.  On open, the source seeks to the restored event timestamp.
    *
    * @param state the state to restore.
    */
  override def restoreState(state: RiverSourceCheckpointState): Unit = {
    LOG.debug("restoreState called (state={})", state)

    lastEventTimestamp = state.lastEventTimestamp
  }
}


object RiverSource {
  case class RiverSourceCheckpointState(lastEventTimestamp: Long)

  private[nupic] def createActorSystem(): ActorSystem = {
    val config =
      s"""
         |akka {
         | daemonic = on
         |
         | jvm-exit-on-fatal-error = off
         |
         | loglevel = WARNING
         | stdout-loglevel = OFF
         |
         | log-dead-letters = off
         |
         |}
      """.stripMargin

    val actorSystem = ActorSystem("RiverSource", ConfigFactory.parseString(config))
    actorSystem
  }
}

