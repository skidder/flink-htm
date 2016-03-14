package org.numenta.nupic.flink.streaming.api;

import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;

/**
 * Base interface for an inference select function.  Such a function is called with
 * the raw inference emitted by an HTM network.  The select method returns exactly one result.
 *
 * @author Eron Wright
 */
public interface InferenceSelectFunction<IN, OUT> extends Function, Serializable {

    /**
     * Generates a result from the given raw inference.
     *
     * @param inference Inference emitted by the network
     * @return resulting element
     * @throws Exception This method may throw exceptions.   Throwing an exception
     * will cause the operation to fail and may trigger recovery.
     */
    OUT select(Inference2<IN> inference) throws Exception;
}
