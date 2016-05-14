package org.numenta.nupic.flink.streaming.api;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;

@Public
public interface ResetFunction<T> extends Function, Serializable {

    /**
     * The reset function that evaluates the predicate to determine whether to reset the HTM sequence memory.
     * <p>
     * <strong>IMPORTANT:</strong> The system assumes that the function does not
     * modify the elements on which the predicate is applied. Violating this assumption
     * can lead to incorrect results.
     *
     * @param value The value to be evaluated.
     * @return True for values that should cause a sequence memory reset, false otherwise.
     *
     * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
     *                   to fail and may trigger recovery.
     */
    boolean reset(T value) throws Exception;
}