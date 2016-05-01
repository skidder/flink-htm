package org.numenta.nupic.flink.streaming.api;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.tuple.Tuple2;
import org.numenta.nupic.network.Inference;

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
     * @param inference A tuple combining the input and associated inference emitted by the network
     * @return resulting element
     * @throws Exception This method may throw exceptions.   Throwing an exception
     * will cause the operation to fail and may trigger recovery.
     */
    OUT select(Tuple2<IN, NetworkInference> inference) throws Exception;
}
