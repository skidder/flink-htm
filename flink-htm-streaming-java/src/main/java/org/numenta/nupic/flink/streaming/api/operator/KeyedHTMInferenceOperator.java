package org.numenta.nupic.flink.streaming.api.operator;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.numenta.nupic.flink.streaming.api.NetworkFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.numenta.nupic.network.Network;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Eron Wright
 */
public class KeyedHTMInferenceOperator<IN, KEY> extends AbstractHTMInferenceOperator<IN> {

    private static final String HTM_INFERENCE_OPERATOR_STATE_NAME = "htmInferenceOperatorState";

    // necessary to extract the key from the input elements
    private final KeySelector<IN, KEY> keySelector;

    // necessary to serialize the set of seen keys
    private final TypeSerializer<KEY> keySerializer;

    // necessary to create an HTM network per key.
    private final NetworkFactory<IN> networkFactory;

    // stores the keys we've already seen to trigger execution upon receiving a watermark
    // this can be problematic, since it is never cleared
    // TODO: fix once the state refactoring is completed
    private transient Set<KEY> keys;

    // stores the per-key network
    private transient ValueState<Network> networkState;

    public KeyedHTMInferenceOperator(
            final ExecutionConfig executionConfig,
            final TypeInformation<IN> inputType,
            boolean isProcessingTime,
            KeySelector<IN, KEY> keySelector,
            TypeSerializer<KEY> keySerializer,
            NetworkFactory<IN> networkFactory) {
        super(executionConfig, inputType, isProcessingTime, networkFactory);

        this.keySelector = keySelector;
        this.keySerializer = keySerializer;
        this.networkFactory = networkFactory;
    }

    @Override
    public void open() throws Exception {
        if (keys == null) {
            keys = new HashSet<KEY>();
        }

        if (networkState == null) {
            networkState = getPartitionedState(new ValueStateDescriptor<Network>(
                    HTM_INFERENCE_OPERATOR_STATE_NAME,
                    new KryoSerializer<Network>((Class<Network>) (Class<?>) Network.class, getExecutionConfig()),
                    null
            ));
        }

        initInputFunction();
    }

    @Override
    protected Network getInputNetwork() throws Exception {
        Network network = networkState.value();
        if(network == null) {
            Object currentKey = getStateBackend().getCurrentKey();
            network = networkFactory.createNetwork(currentKey);
            networkState.update(network);

            LOG.info("Created HTM network {}", network.getName());
        }
        return network;
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        keys.add(keySelector.getKey(element.getValue()));

        super.processElement(element);
    }
}
