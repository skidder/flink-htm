package org.numenta.nupic.flink.streaming.api;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.numenta.nupic.flink.streaming.api.operator.GlobalHTMInferenceOperator;
import org.numenta.nupic.flink.streaming.api.operator.KeyedHTMInferenceOperator;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;

/**
 * Utility class for stream processing with hierarchical temporal memory (HTM).
 *
 * @author Eron Wright
 */
public class HTM {
    private static final String INFERENCE_OPERATOR_NAME = "AbstractHTMInferenceOperator";

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
    public static <T, K> HTMStream<T> network(DataStream<T> input, NetworkFactory<T> networkFactory) {
        final boolean isProcessingTime = input.getExecutionEnvironment().getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime;

        final DataStream<Inference2<T>> inferenceStream;

        if (input instanceof KeyedStream) {
            // each key will be processed by a dedicated Network instance.
            KeyedStream<T, K> keyedStream = (KeyedStream<T, K>) input;

            KeySelector<T, K> keySelector = keyedStream.getKeySelector();
            TypeSerializer<K> keySerializer = keyedStream.getKeyType().createSerializer(keyedStream.getExecutionConfig());

            inferenceStream = input.transform(
                    INFERENCE_OPERATOR_NAME,
                    (TypeInformation<Inference2<T>>) (TypeInformation<?>) TypeExtractor.getForClass(Inference2.class),
                    new KeyedHTMInferenceOperator<>(input.getExecutionConfig(), input.getType(), isProcessingTime, keySelector, keySerializer, networkFactory)
            ).name("network");

        } else {
            // all stream elements will be processed by a single Network instance, hence parallelism -> 1.
            inferenceStream = input.transform(
                    INFERENCE_OPERATOR_NAME,
                    (TypeInformation<Inference2<T>>) (TypeInformation<?>) TypeExtractor.getForClass(Inference2.class),
                    new GlobalHTMInferenceOperator<T>(input.getExecutionConfig(), input.getType(), isProcessingTime, networkFactory)
            ).name("network").setParallelism(1);
        }

        return new HTMStream<T>(inferenceStream, input.getType());
    }
}
