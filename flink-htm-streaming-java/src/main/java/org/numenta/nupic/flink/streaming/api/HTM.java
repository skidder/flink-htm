package org.numenta.nupic.flink.streaming.api;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.numenta.nupic.flink.serialization.KryoSerializer;
import org.numenta.nupic.flink.streaming.api.operator.GlobalHTMInferenceOperator;
import org.numenta.nupic.flink.streaming.api.operator.KeyedHTMInferenceOperator;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.numenta.nupic.network.Inference;

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
    public static <T, K> HTMStream<T> learn(DataStream<T> input, NetworkFactory<T> networkFactory) {

        KryoSerializer.registerTypes(input.getExecutionConfig());

        final boolean isProcessingTime = input.getExecutionEnvironment().getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime;

        final TypeInformation<Inference> inferenceTypeInfo = TypeExtractor.getForClass(Inference.class);
        final TypeInformation<Tuple2<T,Inference>> inferenceStreamTypeInfo = new TupleTypeInfo<>(input.getType(), inferenceTypeInfo);

        final DataStream<Tuple2<T, Inference>> inferenceStream;

        if (input instanceof KeyedStream) {
            // each key will be processed by a dedicated Network instance.
            KeyedStream<T, K> keyedStream = (KeyedStream<T, K>) input;

            KeySelector<T, K> keySelector = keyedStream.getKeySelector();
            TypeSerializer<K> keySerializer = keyedStream.getKeyType().createSerializer(keyedStream.getExecutionConfig());

            inferenceStream = input.transform(
                    INFERENCE_OPERATOR_NAME,
                    inferenceStreamTypeInfo,
                    new KeyedHTMInferenceOperator<>(input.getExecutionConfig(), input.getType(), isProcessingTime, keySelector, keySerializer, networkFactory)
            ).name("Learn");

        } else {
            // all stream elements will be processed by a single Network instance, hence parallelism -> 1.
            inferenceStream = input.transform(
                    INFERENCE_OPERATOR_NAME,
                    inferenceStreamTypeInfo,
                    new GlobalHTMInferenceOperator<>(input.getExecutionConfig(), input.getType(), isProcessingTime, networkFactory)
            ).name("Learn").setParallelism(1);
        }

        return new HTMStream<T>(inferenceStream, input.getType());
    }
}
