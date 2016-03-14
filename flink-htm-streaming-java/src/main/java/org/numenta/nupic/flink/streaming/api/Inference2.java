package org.numenta.nupic.flink.streaming.api;

import org.numenta.nupic.algorithms.ClassifierResult;
import org.numenta.nupic.network.Inference;

import java.io.Serializable;
import java.util.Map;

/**
 * @author Eron Wright
 */
public class Inference2<IN> implements Serializable {
    public double anomalyScore;

    public IN input;

    public Map<String,ClassifierResult<Object>> classifications;

    /**
     * Returns the most recent {@link ClassifierResult}
     *
     * @param fieldName
     * @return
     */
    public ClassifierResult<Object> getClassification(String fieldName) {
        if(classifications == null) throw new IllegalStateException("no classification results are available");
        return classifications.get(fieldName);
    }
}
