package org.numenta.nupic.flink.streaming.api;

import org.numenta.nupic.algorithms.Classification;
import org.numenta.nupic.network.Inference;
import org.numenta.nupic.network.Layer;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Container for output from a given {@link Layer}. Represents the
 * result accumulated by the computation of a sequence of algorithms
 * contained in a given Layer and contains information needed at
 * various stages in the sequence of calculations a Layer may contain.
 */
public class Inference2<IN> implements Serializable {

    private final double anomalyScore;

    private final IN input;

    private final Map<String,Classification<Object>> classifications;

    public Inference2(IN input, double anomalyScore, Map<String,Classification<Object>> classifications) {
        this.input = input;
        this.anomalyScore = anomalyScore;
        this.classifications = classifications;
    }

    public double getAnomalyScore() { return this.anomalyScore; }

    public IN getInput() { return this.input; }

    /**
     * Returns the most recent {@link Classification}
     *
     * @param fieldName the field for which to get the classification.
     * @return the classification result.
     */
    public Classification<Object> getClassification(String fieldName) {
        if(classifications == null) throw new IllegalStateException("no classification results are available");
        return classifications.get(fieldName);
    }

    public static <IN> Inference2<IN> fromInference(IN input, Inference i) {
        Map<String, Classification<Object>> classifications = new HashMap<>();
        for(String field : i.getClassifiers().keys()) {
            classifications.put(field, i.getClassification(field));
        }
        return new Inference2<IN>(input, i.getAnomalyScore(), classifications);
    }
}
