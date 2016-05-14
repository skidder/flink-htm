package org.numenta.nupic.flink.streaming.api.operator;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;
import org.numenta.nupic.flink.streaming.api.NetworkFactory;
import org.numenta.nupic.flink.streaming.api.ResetFunction;
import org.numenta.nupic.network.Network;

/**
 * HTM inference operator implementation which is used for non-keyed streams.
 *
 * @param <IN> Type of the input elements.
 *
 * @author Eron Wright
 */
public class GlobalHTMInferenceOperator<IN> extends AbstractHTMInferenceOperator<IN> {

    final private NetworkFactory<IN> networkFactory;

    // global network for all elements
    transient private Network network;

    // state serializer
    private KryoSerializer<Network> stateSerializer;

    public GlobalHTMInferenceOperator(
            final ExecutionConfig executionConfig,
            final TypeInformation<IN> inputType,
            boolean isProcessingTime,
            NetworkFactory<IN> networkFactory,
            ResetFunction<IN> resetFunction) {
        super(executionConfig, inputType, isProcessingTime, networkFactory, resetFunction);

        this.networkFactory = networkFactory;

        stateSerializer = new KryoSerializer<Network>((Class<Network>) (Class<?>) Network.class, executionConfig);
    }

    @Override
    public void open() throws Exception {
        super.open();

        network = networkFactory.createNetwork(null);
        networkCounter.add(1);
        LOG.info("Created HTM network {}", network.getName());
        initInputFunction();
    }

    @Override
    protected Network getInputNetwork() throws Exception {
        return network;
    }

    @Override
    public StreamTaskState snapshotOperatorState(long checkpointId, long timestamp) throws Exception {
        StreamTaskState taskState = super.snapshotOperatorState(checkpointId, timestamp);

        final AbstractStateBackend.CheckpointStateOutputView ov =
                this.getStateBackend().createCheckpointStateOutputView(checkpointId, timestamp);
        stateSerializer.serialize(network, ov);
        taskState.setOperatorState(ov.closeAndGetHandle());

        return taskState;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void restoreState(StreamTaskState state, long recoveryTimestamp) throws Exception {
        super.restoreState(state, recoveryTimestamp);

        final StateHandle<DataInputView> handle = (StateHandle<DataInputView>) state.getOperatorState();
        final DataInputView iv = handle.getState(getUserCodeClassloader());
        network = stateSerializer.deserialize(iv);
    }
}
