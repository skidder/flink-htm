package org.numenta.nupic.flink.streaming.api;

import org.numenta.nupic.algorithms.ClassifierResult;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Container for the results of a classification computation.
 */
public class ClassifierResult2<T> implements Serializable {
    /** Array of actual values */
    private final T[] actualValues;

    /** Map of step count -to- probabilities */
    private final Map<Integer, double[]> probabilities;

    private ClassifierResult2(T[] actualValues, Map<Integer, double[]> probabilities) {
        this.actualValues = actualValues;
        this.probabilities = probabilities;
    }

    /**
     * Returns the actual value for the specified bucket index
     *
     * @param bucketIndex
     * @return
     */
    public T getActualValue(int bucketIndex) {
        if(actualValues == null || actualValues.length < bucketIndex + 1) {
            return null;
        }
        return (T)actualValues[bucketIndex];
    }

    /**
     * Returns all actual values entered
     *
     * @return  array of type &lt;T&gt;
     */
    public T[] getActualValues() {
        return actualValues;
    }

    /**
     * Returns a count of actual values entered
     * @return
     */
    public int getActualValueCount() {
        return actualValues.length;
    }

    /**
     * Returns the probability at the specified index for the given step
     * @param step
     * @param bucketIndex
     * @return
     */
    public double getStat(int step, int bucketIndex) {
        return probabilities.get(step)[bucketIndex];
    }

    /**
     * Returns the probabilities for the specified step
     * @param step
     * @return
     */
    public double[] getStats(int step) {
        return probabilities.get(step);
    }

    /**
     * Returns the input value corresponding with the highest probability
     * for the specified step.
     *
     * @param step		the step key under which the most probable value will be returned.
     * @return
     */
    public T getMostProbableValue(int step) {
        int idx = -1;
        if(probabilities.get(step) == null || (idx = getMostProbableBucketIndex(step)) == -1) {
            return null;
        }
        return getActualValue(idx);
    }

    /**
     * Returns the bucket index corresponding with the highest probability
     * for the specified step.
     *
     * @param step		the step key under which the most probable index will be returned.
     * @return			-1 if there is no such entry
     */
    public int getMostProbableBucketIndex(int step) {
        if(probabilities.get(step) == null) return -1;

        double max = 0;
        int bucketIdx = -1;
        int i = 0;
        for(double d : probabilities.get(step)) {
            if(d > max) {
                max = d;
                bucketIdx = i;
            }
            ++i;
        }
        return bucketIdx;
    }

    /**
     * Returns the count of steps
     * @return
     */
    public int getStepCount() {
        return probabilities.size();
    }

    /**
     * Returns the count of probabilities for the specified step
     * @param step the step indexing the probability values
     * @return
     */
    public int getStatCount(int step) {
        return probabilities.get(step).length;
    }

    /**
     * Returns a set of steps being recorded.
     * @return
     */
    public int[] stepSet() {
        int[] set = new int[probabilities.size()];
        int i = 0;
        for(Integer key : probabilities.keySet()) set[i++] = key;
        return set;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(actualValues);
        result = prime * result + ((probabilities == null) ? 0 : probabilities.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        if(obj == null)
            return false;
        if(getClass() != obj.getClass())
            return false;
        @SuppressWarnings("rawtypes")
        ClassifierResult2 other = (ClassifierResult2)obj;
        if(!Arrays.equals(actualValues, other.actualValues))
            return false;
        if(probabilities == null) {
            if(other.probabilities != null)
                return false;
        } else if(!probabilities.equals(other.probabilities))
            return false;
        return true;
    }

    public static <T> ClassifierResult2 fromClassifierResult(ClassifierResult<T> result) {
        Map<Integer, double[]> probabilities = new HashMap<>();

        for(int i = 0; i <= result.getStepCount(); i++) {
            probabilities.put(i, result.getStats(i));
        }

        return new ClassifierResult2(result.getActualValues(), probabilities);
    }
}
