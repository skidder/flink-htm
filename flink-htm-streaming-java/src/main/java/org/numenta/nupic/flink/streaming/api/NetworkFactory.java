package org.numenta.nupic.flink.streaming.api;

import org.numenta.nupic.network.Network;

import java.io.Serializable;

/**
 * Factory interface for an HTM {@Link Network}.
 *
 * @param <T> Type of the input records which are processed by the network.
 *
 * @author Eron Wright
 */
public interface NetworkFactory<T> extends Serializable {

    /**
     * Create a network for the given key.
     * @param key the key, or null.
     * @return a network instance.
     */
    Network createNetwork(Object key);
}
