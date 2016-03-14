package org.numenta.nupic.scala

import org.numenta.{nupic => jnupic}

/**
  * Convenience object for building a parameters instance.
  */
object Parameters {

  def spatialDefaultParameters = jnupic.Parameters.getSpatialDefaultParameters
  def temporalDefaultParameters = jnupic.Parameters.getTemporalDefaultParameters
  def encoderDefaultParameters = jnupic.Parameters.getEncoderDefaultParameters

  def apply(parameters: (jnupic.Parameters.KEY, Any)*): jnupic.Parameters = {
    val p = jnupic.Parameters.empty()
    parameters.foreach { case (key, value) => p.setParameterByKey(key, value) }
    p
  }
}
