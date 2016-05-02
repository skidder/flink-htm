package org.numenta.nupic.flink.streaming.api.operator;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.AverageAccumulator;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.numenta.nupic.flink.streaming.api.NetworkFactory;
import org.numenta.nupic.flink.streaming.api.NetworkInference;
import org.numenta.nupic.flink.streaming.api.codegen.GenerateEncoderInputFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.numenta.nupic.Parameters;
import org.numenta.nupic.encoders.MultiEncoder;
import org.numenta.nupic.network.Inference;
import org.numenta.nupic.network.Network;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Base class for HTM inference operator.  The operator uses a {@Link Network} to
 * make inferences about input stream elements.
 *
 * @param <IN> Type of the input elements
 *
 * @author Eron Wright
 */
public abstract class AbstractHTMInferenceOperator<IN>
        extends AbstractStreamOperator<Tuple2<IN, NetworkInference>>
        implements OneInputStreamOperator<IN, Tuple2<IN, NetworkInference>> {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractHTMInferenceOperator.class);

    protected static final int INITIAL_PRIORITY_QUEUE_CAPACITY = 11;

    protected IntCounter networkCounter;
    private AverageAccumulator avgProcessingTime;

    private final ExecutionConfig executionConfig;
    private final TypeInformation<IN> inputType;
    private final TypeSerializer<IN> inputSerializer;
    private final boolean isProcessingTime;
    private final NetworkFactory<IN> networkFactory;

    private transient EncoderInputFunction<IN> inputFunction;

    public AbstractHTMInferenceOperator(
            final ExecutionConfig executionConfig,
            final TypeInformation<IN> inputType,
            final boolean isProcessingTime,
            final NetworkFactory<IN> networkFactory
    ) {
        this.executionConfig = executionConfig;
        this.inputType = inputType;
        this.isProcessingTime = isProcessingTime;
        this.networkFactory = networkFactory;

        this.inputSerializer = inputType.createSerializer(executionConfig);
    }

    public TypeSerializer<IN> getInputSerializer() {
        return inputSerializer;
    }

    @Override
    public void open() throws Exception {
        super.open();

        networkCounter = getRuntimeContext().getIntCounter("networks");
        avgProcessingTime = new AverageAccumulator();
        getRuntimeContext().addAccumulator("processing time (ms)", avgProcessingTime);
    }

    protected abstract Network getInputNetwork() throws Exception;

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        long startTime = System.currentTimeMillis();

        if (isProcessingTime) {
            // there can be no out of order elements in processing time
            Network network = getInputNetwork();
            processInput(network, element.getValue(), element.getTimestamp());
        } else {
            // TODO order input elements by timestamp (HTM causality)
            // see Flink CEP code for an example
            Network network = getInputNetwork();
            processInput(network, element.getValue(), element.getTimestamp());
        }

        long duration = System.currentTimeMillis() - startTime;
        avgProcessingTime.add(duration);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        output.emitWatermark(mark);
    }

    protected void processInput(Network network, IN record, long timestamp) {
        Object input = convertToInput(record, timestamp);
        Inference inference = network.computeImmediate(input);

        if(inference != null) {
            NetworkInference outputInference = NetworkInference.fromInference(inference);
            StreamRecord<Tuple2<IN,NetworkInference>> streamRecord = new StreamRecord<>(
                    new Tuple2(record, outputInference),
                    timestamp);
            output.collect(streamRecord);
        }
    }

    /**
     * Initialize the input function to map input elements to HTM encoder input.
     * @throws Exception
     */
    protected void initInputFunction() throws Exception {

        // it is premature to call getInputNetwork, because no 'key' is available
        // when the operator is first opened.
        Network network = networkFactory.createNetwork(null);
        MultiEncoder encoder = network.getEncoder();

        if(encoder == null)
            throw new IllegalArgumentException("a network encoder must be provided");

        // handle the situation where an encoder parameter map was supplied rather than a fully-baked encoder.
        if(encoder.getEncoders(encoder) == null || encoder.getEncoders(encoder).size() < 1) {
            Map<String, Map<String, Object>> encoderParams =
                    (Map<String, Map<String, Object>>) network.getParameters().getParameterByKey(Parameters.KEY.FIELD_ENCODING_MAP);
            if(encoderParams == null || encoderParams.size() < 1) {
                throw new IllegalStateException("No field encoding map found for MultiEncoder");
            }
            encoder.addMultipleEncoders(encoderParams);
        }

        // generate the encoder input function
        final GenerateEncoderInputFunction<IN> generator = new GenerateEncoderInputFunction<>((CompositeType<IN>) inputType, encoder, executionConfig);
        inputFunction = generator.generate();
    }

    private Object convertToInput(IN record, long timestamp) {
        if(inputFunction == null) throw new IllegalStateException("inputFunction is null");

        Map<String, Object> inputMap = inputFunction.map(record);
        return inputMap;
    }
}
