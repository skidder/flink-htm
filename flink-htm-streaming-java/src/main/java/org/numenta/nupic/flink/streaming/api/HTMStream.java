package org.numenta.nupic.flink.streaming.api;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.numenta.nupic.flink.streaming.api.operator.GlobalHTMInferenceOperator;
import org.numenta.nupic.flink.streaming.api.operator.KeyedHTMInferenceOperator;

/**
 * Stream abstraction for HTM inference.  An HTM stream is a stream which emits
 * inferences based on incoming elements, using logic provided by an HTM network.
 *
 * @param <T> Type of the input elements.
 * @author Eron Wright
 */
public class HTMStream<T> {

    private static final String INFERENCE_OPERATOR_NAME = "AbstractHTMInferenceOperator";

    // underlying data stream
    private final InferenceStreamBuilder<T> inferenceStreamBuilder;

    //type information of input type T
    private final TypeInformation<T> inputType;

    HTMStream(final DataStream<T> input, NetworkFactory<T> networkFactory) {
        this.inferenceStreamBuilder = new InferenceStreamBuilder(input, networkFactory);
        this.inputType = input.getType();
    }

    /**
     * Returns the {@link StreamExecutionEnvironment} that was used to create this
     * {@link HTMStream}.
     *
     * @return The Execution Environment
     */
    public StreamExecutionEnvironment getExecutionEnvironment() {
        return inferenceStreamBuilder.input.getExecutionEnvironment();
    }

    /**
     * Invokes the {@link org.apache.flink.api.java.ClosureCleaner}
     * on the given function if closure cleaning is enabled in the {@link ExecutionConfig}.
     *
     * @return The cleaned Function
     */
    private <F> F clean(F f) {
        return getExecutionEnvironment().clean(f);
    }

    /**
     * Applies a reset function to input elements to determine when to reset a temporal sequence.
     *
     * The reset logic is applied <i>before</i> the input element is processed by the network.
     *
     * @param resetFunction the function to apply.
     * @return the HTM stream.
     */
    public HTMStream<T> resetOn(ResetFunction<T> resetFunction) {
        inferenceStreamBuilder.resetOn(clean(resetFunction));
        return this;
    }

    /**
     * Applies a select function to the inference emitted by the HTM network.  For each
     * inference the provided {@Link InferenceSelectFunction} is called.
     *
     * @param inferenceSelectFunction The inference select function which is called for each inference
     * @param <R> The type of teh resulting elements
     * @return {@Link DataStream} which contains the resulting elements from the inference select function.
     */
    public <R> DataStream<R> select(final InferenceSelectFunction<T, R> inferenceSelectFunction) {
        TypeInformation<R> returnType = TypeExtractor.getUnaryOperatorReturnType(
                inferenceSelectFunction,
                InferenceSelectFunction.class,
                false,
                false,
                inputType,
                null,
                false);
        return select(inferenceSelectFunction, returnType);
    }

    public <R> DataStream<R> select(final InferenceSelectFunction<T, R> inferenceSelectFunction, TypeInformation<R> returnType) {

        final DataStream<Tuple2<T, NetworkInference>> inferenceStream = inferenceStreamBuilder.build();

        return inferenceStream
                .map(new InferenceSelectMapper<T, R>(clean(inferenceSelectFunction)))
                .returns(returnType);
    }

    private static class InferenceSelectMapper<T, R> implements MapFunction<Tuple2<T, NetworkInference>, R> {

        private final InferenceSelectFunction<T, R> inferenceSelectFunction;

        public InferenceSelectMapper(InferenceSelectFunction<T, R> inferenceSelectFunction) {
            this.inferenceSelectFunction = inferenceSelectFunction;
        }

        @Override
        public R map(Tuple2<T, NetworkInference> value) throws Exception {
            return inferenceSelectFunction.select(value);
        }
    }

    /**
     * Supports the HTM stream DSL.
     */
    private static class InferenceStreamBuilder<T> {

        private final DataStream<T> input;
        private final NetworkFactory<T> networkFactory;
        private ResetFunction<T> resetFunction = null;

        public InferenceStreamBuilder(DataStream<T> input, NetworkFactory<T> networkFactory) {
            this.input = input;
            this.networkFactory = networkFactory;
        }

        public InferenceStreamBuilder<T> resetOn(ResetFunction<T> resetFunction) {
            this.resetFunction = resetFunction;
            return this;
        }

        public <K> DataStream<Tuple2<T, NetworkInference>> build() {

            final boolean isProcessingTime = input.getExecutionEnvironment().getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime;

            final TypeInformation<NetworkInference> inferenceTypeInfo = TypeExtractor.getForClass(NetworkInference.class);
            final TypeInformation<Tuple2<T,NetworkInference>> inferenceStreamTypeInfo = new TupleTypeInfo<>(input.getType(), inferenceTypeInfo);

            final DataStream<Tuple2<T, NetworkInference>> inferenceStream;

            if (input instanceof KeyedStream) {
                // each key will be processed by a dedicated Network instance.
                KeyedStream<T, K> keyedStream = (KeyedStream<T, K>) input;

                KeySelector<T, K> keySelector = keyedStream.getKeySelector();
                TypeSerializer<K> keySerializer = keyedStream.getKeyType().createSerializer(keyedStream.getExecutionConfig());

                KeyedHTMInferenceOperator<T, K> operator = new KeyedHTMInferenceOperator<>(
                        input.getExecutionConfig(), input.getType(), isProcessingTime, keySelector, keySerializer, networkFactory, resetFunction);
                inferenceStream = input.transform(
                        INFERENCE_OPERATOR_NAME,
                        inferenceStreamTypeInfo,
                        operator
                ).name("Learn");

            } else {
                // all stream elements will be processed by a single Network instance, hence parallelism -> 1.
                GlobalHTMInferenceOperator<T> operator = new GlobalHTMInferenceOperator<>(
                        input.getExecutionConfig(), input.getType(), isProcessingTime, networkFactory, resetFunction);
                inferenceStream = input.transform(
                        INFERENCE_OPERATOR_NAME,
                        inferenceStreamTypeInfo,
                        operator
                ).name("Learn").setParallelism(1);
            }

            return inferenceStream;
        }
    }
}
