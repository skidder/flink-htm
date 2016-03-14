package org.numenta.nupic.flink.streaming.api;

import org.numenta.nupic.Parameters;
import org.numenta.nupic.algorithms.Anomaly;
import org.numenta.nupic.algorithms.SpatialPooler;
import org.numenta.nupic.algorithms.TemporalMemory;
import org.numenta.nupic.encoders.Encoder;
import org.numenta.nupic.encoders.MultiEncoder;
import org.numenta.nupic.network.Network;
import org.numenta.nupic.util.MersenneTwister;

import java.util.HashMap;
import java.util.Map;

import static org.numenta.nupic.algorithms.Anomaly.KEY_MODE;

/**
 * @author Eron Wright
 */
public class TestHarness {

    public static class DayDemoRecord {
        public int dayOfWeek;

        public DayDemoRecord() {}
        public DayDemoRecord(int dayOfWeek) { this.dayOfWeek = dayOfWeek; }
    }

    public static class DayDemoNetworkFactory implements NetworkFactory<TestHarness.DayDemoRecord> {
        @Override
        public Network createNetwork(Object key) {
            Parameters p = getParameters();
            p = p.union(getEncoderParams());
            p.setParameterByKey(Parameters.KEY.COLUMN_DIMENSIONS, new int[] { 30 });
            p.setParameterByKey(Parameters.KEY.SYN_PERM_INACTIVE_DEC, 0.1);
            p.setParameterByKey(Parameters.KEY.SYN_PERM_ACTIVE_INC, 0.1);
            p.setParameterByKey(Parameters.KEY.SYN_PERM_TRIM_THRESHOLD, 0.05);
            p.setParameterByKey(Parameters.KEY.SYN_PERM_CONNECTED, 0.4);
            p.setParameterByKey(Parameters.KEY.MAX_BOOST, 10.0);
            p.setParameterByKey(Parameters.KEY.DUTY_CYCLE_PERIOD, 7);
            p.setParameterByKey(Parameters.KEY.RANDOM, new MersenneTwister(42));

            Map<String, Object> params = new HashMap<>();
            params.put(KEY_MODE, Anomaly.Mode.PURE);

            Network n = Network.create("DayDemoNetwork", p)
                    .add(Network.createRegion("r1")
                            .add(Network.createLayer("1", p)
                                    .alterParameter(Parameters.KEY.AUTO_CLASSIFY, Boolean.TRUE)
                                    .add(new TemporalMemory())
                                    .add(new SpatialPooler())
                                    .add(MultiEncoder.builder().name("").build())));
            return n;
        }

        /**
         * Parameters and meta information for the "dayOfWeek" encoder
         * @return
         */
        public static Map<String, Map<String, Object>> getFieldEncodingMap() {
            Map<String, Map<String, Object>> fieldEncodings = setupMap(
                    null,
                    8, // n
                    3, // w
                    0.0, 8.0, 0, 1, Boolean.TRUE, null, Boolean.TRUE,
                    "dayOfWeek", "number", "ScalarEncoder");
            return fieldEncodings;
        }

        /**
         * Returns Encoder parameters for the "dayOfWeek" test encoder.
         * @return
         */
        public static Parameters getEncoderParams() {
            Map<String, Map<String, Object>> fieldEncodings = getFieldEncodingMap();

            Parameters p = Parameters.getEncoderDefaultParameters();
            p.setParameterByKey(Parameters.KEY.FIELD_ENCODING_MAP, fieldEncodings);

            return p;
        }

        /**
         * Returns the default parameters used for the "dayOfWeek" encoder and algorithms.
         * @return
         */
        public static Parameters getParameters() {
            Parameters parameters = Parameters.getAllDefaultParameters();
            parameters.setParameterByKey(Parameters.KEY.INPUT_DIMENSIONS, new int[] { 8 });
            parameters.setParameterByKey(Parameters.KEY.COLUMN_DIMENSIONS, new int[] { 20 });
            parameters.setParameterByKey(Parameters.KEY.CELLS_PER_COLUMN, 6);

            //SpatialPooler specific
            parameters.setParameterByKey(Parameters.KEY.POTENTIAL_RADIUS, 12);//3
            parameters.setParameterByKey(Parameters.KEY.POTENTIAL_PCT, 0.5);//0.5
            parameters.setParameterByKey(Parameters.KEY.GLOBAL_INHIBITIONS, false);
            parameters.setParameterByKey(Parameters.KEY.LOCAL_AREA_DENSITY, -1.0);
            parameters.setParameterByKey(Parameters.KEY.NUM_ACTIVE_COLUMNS_PER_INH_AREA, 5.0);
            parameters.setParameterByKey(Parameters.KEY.STIMULUS_THRESHOLD, 1.0);
            parameters.setParameterByKey(Parameters.KEY.SYN_PERM_INACTIVE_DEC, 0.01);
            parameters.setParameterByKey(Parameters.KEY.SYN_PERM_ACTIVE_INC, 0.1);
            parameters.setParameterByKey(Parameters.KEY.SYN_PERM_TRIM_THRESHOLD, 0.05);
            parameters.setParameterByKey(Parameters.KEY.SYN_PERM_CONNECTED, 0.1);
            parameters.setParameterByKey(Parameters.KEY.MIN_PCT_OVERLAP_DUTY_CYCLE, 0.1);
            parameters.setParameterByKey(Parameters.KEY.MIN_PCT_ACTIVE_DUTY_CYCLE, 0.1);
            parameters.setParameterByKey(Parameters.KEY.DUTY_CYCLE_PERIOD, 10);
            parameters.setParameterByKey(Parameters.KEY.MAX_BOOST, 10.0);
            parameters.setParameterByKey(Parameters.KEY.SEED, 42);
            parameters.setParameterByKey(Parameters.KEY.SP_VERBOSITY, 0);

            //Temporal Memory specific
            parameters.setParameterByKey(Parameters.KEY.INITIAL_PERMANENCE, 0.2);
            parameters.setParameterByKey(Parameters.KEY.CONNECTED_PERMANENCE, 0.8);
            parameters.setParameterByKey(Parameters.KEY.MIN_THRESHOLD, 5);
            parameters.setParameterByKey(Parameters.KEY.MAX_NEW_SYNAPSE_COUNT, 6);
            parameters.setParameterByKey(Parameters.KEY.PERMANENCE_INCREMENT, 0.05);
            parameters.setParameterByKey(Parameters.KEY.PERMANENCE_DECREMENT, 0.05);
            parameters.setParameterByKey(Parameters.KEY.ACTIVATION_THRESHOLD, 4);

            return parameters;
        }
    }

    /**
     * Sets up an Encoder Mapping of configurable values.
     *
     * @param map               if called more than once to set up encoders for more
     *                          than one field, this should be the map itself returned
     *                          from the first call to {@code #setupMap(Map, int, int, double,
     *                          double, double, double, Boolean, Boolean, Boolean, String, String, String)}
     * @param n                 the total number of bits in the output encoding
     * @param w                 the number of bits to use in the representation
     * @param min               the minimum value (if known i.e. for the ScalarEncoder)
     * @param max               the maximum value (if known i.e. for the ScalarEncoder)
     * @param radius            see {@link Encoder}
     * @param resolution        see {@link Encoder}
     * @param periodic          such as hours of the day or days of the week, which repeat in cycles
     * @param clip              whether the outliers should be clipped to the min and max values
     * @param forced            use the implied or explicitly stated ratio of w to n bits rather than the "suggested" number
     * @param fieldName         the name of the encoded field
     * @param fieldType         the data type of the field
     * @param encoderType       the Camel case class name minus the .class suffix
     * @return
     */
    public static Map<String, Map<String, Object>> setupMap(
            Map<String, Map<String, Object>> map,
            int n, int w, double min, double max, double radius, double resolution, Boolean periodic,
            Boolean clip, Boolean forced, String fieldName, String fieldType, String encoderType) {

        if(map == null) {
            map = new HashMap<String, Map<String, Object>>();
        }
        Map<String, Object> inner = null;
        if((inner = map.get(fieldName)) == null) {
            map.put(fieldName, inner = new HashMap<String, Object>());
        }

        inner.put("n", n);
        inner.put("w", w);
        inner.put("minVal", min);
        inner.put("maxVal", max);
        inner.put("radius", radius);
        inner.put("resolution", resolution);

        if(periodic != null) inner.put("periodic", periodic);
        if(clip != null) inner.put("clipInput", clip);
        if(forced != null) inner.put("forced", forced);
        if(fieldName != null) inner.put("fieldName", fieldName);
        if(fieldType != null) inner.put("fieldType", fieldType);
        if(encoderType != null) inner.put("encoderType", encoderType);

        return map;
    }
}
