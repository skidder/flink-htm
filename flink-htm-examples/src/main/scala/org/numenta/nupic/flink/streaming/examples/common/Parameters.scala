package org.numenta.nupic.flink.streaming.examples.common

import org.numenta.nupic.Parameters.KEY
import org.numenta.nupic.Parameters.KEY._
import org.numenta.nupic.scala.Parameters

/**
  * Network parameters drawn from the htm.java-examples (hotgym).
  */
object NetworkDemoParameters {

  import KEY._

  /**
    * The HTM network parameters.
    */
  val params =
    Parameters.spatialDefaultParameters
      .union(Parameters.temporalDefaultParameters)
      .union(Parameters(
        INPUT_DIMENSIONS -> Array(8),
        COLUMN_DIMENSIONS -> Array(20),
        CELLS_PER_COLUMN -> 6,

        //SpatialPooler specific
        POTENTIAL_RADIUS -> 12, //3
        POTENTIAL_PCT -> 0.5, //0.5
        GLOBAL_INHIBITION -> false,
        LOCAL_AREA_DENSITY -> -1.0,
        NUM_ACTIVE_COLUMNS_PER_INH_AREA -> 5.0,
        STIMULUS_THRESHOLD -> 1.0,
        SYN_PERM_INACTIVE_DEC -> 0.01,
        SYN_PERM_ACTIVE_INC -> 0.1,
        SYN_PERM_TRIM_THRESHOLD -> 0.05,
        SYN_PERM_CONNECTED -> 0.1,
        MIN_PCT_OVERLAP_DUTY_CYCLE -> 0.1,
        MIN_PCT_ACTIVE_DUTY_CYCLE -> 0.1,
        DUTY_CYCLE_PERIOD -> 10,
        MAX_BOOST -> 10.0,
        SEED -> 42,
        SP_VERBOSITY -> 0,

        //Temporal Memory specific
        INITIAL_PERMANENCE -> 0.2,
        CONNECTED_PERMANENCE -> 0.8,
        MIN_THRESHOLD -> 5,
        MAX_NEW_SYNAPSE_COUNT -> 6,
        PERMANENCE_INCREMENT -> 0.05,
        PERMANENCE_DECREMENT -> 0.05,
        ACTIVATION_THRESHOLD -> 4))
      .union(Parameters(
        GLOBAL_INHIBITION -> true,
        COLUMN_DIMENSIONS -> Array(2048),
        CELLS_PER_COLUMN -> 32,
        NUM_ACTIVE_COLUMNS_PER_INH_AREA -> 40.0,
        POTENTIAL_PCT -> 0.8,
        SYN_PERM_CONNECTED -> 0.1,
        SYN_PERM_ACTIVE_INC -> 0.0001,
        SYN_PERM_INACTIVE_DEC -> 0.0005,
        MAX_BOOST -> 1.0,
        MAX_NEW_SYNAPSE_COUNT -> 20,
        INITIAL_PERMANENCE -> 0.21,
        PERMANENCE_INCREMENT -> 0.1,
        PERMANENCE_DECREMENT -> 0.1,
        MIN_THRESHOLD -> 9,
        ACTIVATION_THRESHOLD -> 12,
        CLIP_INPUT -> true
      ))
}

/**
  * Network parameters drawn from the nupic workshop.
  * https://github.com/rhyolight/nupic.workshop/blob/master/part-1-scalar-input/model_params/model_params.json
  */
trait WorkshopAnomalyParameters {

  val params = Parameters.spatialDefaultParameters
      .union(Parameters.temporalDefaultParameters)
    .union(Parameters(
      // spParams
      POTENTIAL_PCT -> 0.8,
      COLUMN_DIMENSIONS -> Array(2048),
      GLOBAL_INHIBITION -> true,
      /* inputWidth */
      MAX_BOOST -> 1.0,
      NUM_ACTIVE_COLUMNS_PER_INH_AREA -> 40,
      SEED -> 1956,
      SP_VERBOSITY -> 0,
      SYN_PERM_ACTIVE_INC -> 0.003,
      SYN_PERM_CONNECTED -> 0.2,
      SYN_PERM_INACTIVE_DEC -> 0.0005,

      // tpParams
      ACTIVATION_THRESHOLD -> 13,
      CELLS_PER_COLUMN -> 32,
      /* globalDecay */
      INITIAL_PERMANENCE -> 0.21,
      /* maxAge */
      /* maxSegmentsPerCell */
      /* maxSynapsesPerSegment */
      MIN_THRESHOLD -> 10,
      MAX_NEW_SYNAPSE_COUNT -> 20,
      /* outputType */
      /* pamLength */
      PERMANENCE_INCREMENT -> 0.1,
      PERMANENCE_DECREMENT -> 0.1
    ))

}

