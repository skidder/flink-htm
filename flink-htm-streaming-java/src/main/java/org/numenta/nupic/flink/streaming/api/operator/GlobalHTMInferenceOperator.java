package org.numenta.nupic.flink.streaming.api.operator;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.numenta.nupic.flink.streaming.api.NetworkFactory;
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

    public GlobalHTMInferenceOperator(
            final ExecutionConfig executionConfig,
            final TypeInformation<IN> inputType,
            boolean isProcessingTime,
            NetworkFactory<IN> networkFactory) {
        super(executionConfig, inputType, isProcessingTime, networkFactory);

        this.networkFactory = networkFactory;
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
}
