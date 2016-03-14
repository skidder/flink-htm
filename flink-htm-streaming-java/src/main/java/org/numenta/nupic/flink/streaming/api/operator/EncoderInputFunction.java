package org.numenta.nupic.flink.streaming.api.operator;

import java.util.Map;

/**
 * Mapping function to convert input data to an HTM-friendly format.
 *
 * @param <T> the input type
 */
public interface EncoderInputFunction<T> {
    public Map<String,Object> map(T input);
}
