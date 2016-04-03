package org.numenta.nupic.flink.streaming.api;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Stream abstraction for HTM inference.  An HTM stream is a stream which emits
 * inferences based on incoming elements, using logic provided by an HTM network.
 *
 * @param <T> Type of the input elements.
 * @author Eron Wright
 */
public class HTMStream<T> {

    // underlying data stream
    private final DataStream<Inference2<T>> inferenceStream;
    //type information of input type T
    private final TypeInformation<T> inputType;

    HTMStream(final DataStream<Inference2<T>> inferenceStream, final TypeInformation<T> inputType) {
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
    public <R> DataStream<R> select(final MapFunction<Inference2<T>, R> inferenceSelectFunction) {
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

    public <R> DataStream<R> select(final MapFunction<Inference2<T>, R> inferenceSelectFunction, TypeInformation<R> returnType) {
        return inferenceStream.map(inferenceSelectFunction).returns(returnType);
    }
}
