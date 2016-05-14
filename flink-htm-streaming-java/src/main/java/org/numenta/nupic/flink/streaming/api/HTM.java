package org.numenta.nupic.flink.streaming.api;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.numenta.nupic.flink.serialization.KryoSerializer;

/**
 * Utility class for stream processing with hierarchical temporal memory (HTM).
 *
 * @author Eron Wright
 */
public class HTM {
    /**
     * Transforms a {@Link DataStream<T>} into an {@Link HTMStream}.   The HTMStream makes
     * inferences about the incoming stream data and emits an inference element for each input element.
     *
     * @param input          DataStream containing the input records
     * @param networkFactory Factory for HTM network
     * @param <T>            Type of the input records
     * @param <K>            Type of the key in case of a KeyedStream
     * @return Resulting HTM stream
     */
    public static <T, K> HTMStream<T> learn(DataStream<T> input, NetworkFactory<T> networkFactory) {

        KryoSerializer.registerTypes(input.getExecutionConfig());

        return new HTMStream<T>(input, networkFactory);
    }
}
