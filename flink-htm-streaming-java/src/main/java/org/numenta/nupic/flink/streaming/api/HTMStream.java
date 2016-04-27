package org.numenta.nupic.flink.streaming.api;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.numenta.nupic.network.Inference;

/**
 * Stream abstraction for HTM inference.  An HTM stream is a stream which emits
 * inferences based on incoming elements, using logic provided by an HTM network.
 *
 * @param <T> Type of the input elements.
 * @author Eron Wright
 */
public class HTMStream<T> {

    // underlying data stream
    private final DataStream<Tuple2<T, Inference>> inferenceStream;
    //type information of input type T
    private final TypeInformation<T> inputType;

    HTMStream(final DataStream<Tuple2<T, Inference>> inferenceStream, final TypeInformation<T> inputType) {
        this.inferenceStream = inferenceStream;
        this.inputType = inputType;
    }

    /**
     * Returns the {@link StreamExecutionEnvironment} that was used to create this
     * {@link DataStream}
     *
     * @return The Execution Environment
     */
    public StreamExecutionEnvironment getExecutionEnvironment() {
        return inferenceStream.getExecutionEnvironment();
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
        return inferenceStream.map(
                new InferenceSelectMapper<T, R>(
                        inferenceStream.getExecutionEnvironment().clean(inferenceSelectFunction))
        ).returns(returnType);
    }

    private static class InferenceSelectMapper<T, R> implements MapFunction<Tuple2<T, Inference>, R> {

        private final InferenceSelectFunction<T, R> inferenceSelectFunction;

        public InferenceSelectMapper(InferenceSelectFunction<T, R> inferenceSelectFunction) {
            this.inferenceSelectFunction = inferenceSelectFunction;
        }

        @Override
        public R map(Tuple2<T, Inference> value) throws Exception {
            return inferenceSelectFunction.select(value);
        }
    }
}
