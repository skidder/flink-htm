package org.numenta.nupic.flink.streaming.api;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


/**
 * @author Eron Wright
 */
public class HTMIntegrationTest extends StreamingMultipleProgramsTestBase {
    private String resultPath;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void before() throws Exception {
        resultPath = tempFolder.newFile().toURI().toString();
    }

    @After
    public void after() throws Exception {
    }

    @Test
    public void testSimple() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final int NUM_CYCLES = 400;
        final int INPUT_GROUP_COUNT = 7; // Days of Week
        List<TestHarness.DayDemoRecord> records = IntStream.range(0, NUM_CYCLES)
                .flatMap(c -> IntStream.range(0, INPUT_GROUP_COUNT))
                .mapToObj(day -> new TestHarness.DayDemoRecord(day))
                .collect(Collectors.toList());

        DataStream<TestHarness.DayDemoRecord> input = env.fromCollection(records);

        DataStream<Tuple3<Integer,Double,Double>> result = HTM
                .learn(input, new TestHarness.DayDemoNetworkFactory())
                .select(new InferenceSelectFunction<TestHarness.DayDemoRecord, Tuple3<Integer,Double,Double>>() {
                    @Override
                    public Tuple3<Integer,Double,Double> select(Tuple2<TestHarness.DayDemoRecord,NetworkInference> inference) throws Exception {
                        return new Tuple3(
                                inference.f0.dayOfWeek,
                                (Double) inference.f1.getClassification("dayOfWeek").getMostProbableValue(1),
                                inference.f1.getAnomalyScore());
                    }
                });

        result.writeAsCsv(resultPath, FileSystem.WriteMode.OVERWRITE);

        result.print();

        env.execute();
    }


    /**
     * Test the checkpoint behavior of the HTM operator.
     * @throws Exception
     */
    @Test
    public void testCheckpointing1() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

        DataStream<TestHarness.DayDemoRecord> source = env
                .addSource(new DayDemoRecordSourceFunction(2, true))
                .broadcast();

        DataStream<Tuple3<Integer,Double,Double>> result =
            HTM.learn(source, new TestHarness.DayDemoNetworkFactory())
            .select(new InferenceSelectFunction<TestHarness.DayDemoRecord, Tuple3<Integer,Double,Double>>() {
                @Override
                public Tuple3<Integer,Double,Double> select(Tuple2<TestHarness.DayDemoRecord,NetworkInference> inference) throws Exception {
                    return new Tuple3(
                            inference.f0.dayOfWeek,
                            (Double) inference.f1.getClassification("dayOfWeek").getMostProbableValue(1),
                            inference.f1.getAnomalyScore());
                }
            });

        result.print();

        env.execute();
    }

    /**
     * Test the checkpoint behavior of the HTM operator.
     * @throws Exception
     */
    @Test
    public void testCheckpointWithKeyedStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

        DataStream<TestHarness.DayDemoRecord> source = env
                .addSource(new DayDemoRecordSourceFunction(2, true))
                .keyBy("dayOfWeek");

        DataStream<Tuple3<Integer,Double,Double>> result =
                HTM.learn(source, new TestHarness.DayDemoNetworkFactory())
                        .select(new InferenceSelectFunction<TestHarness.DayDemoRecord, Tuple3<Integer,Double,Double>>() {
                            @Override
                            public Tuple3<Integer,Double,Double> select(Tuple2<TestHarness.DayDemoRecord,NetworkInference> inference) throws Exception {
                                return new Tuple3(
                                        inference.f0.dayOfWeek,
                                        (Double) inference.f1.getClassification("dayOfWeek").getMostProbableValue(1),
                                        inference.f1.getAnomalyScore());
                            }
                        });

        result.print();

        env.execute();
    }

    private static class DayDemoRecordSourceFunction extends TestSourceFunction<TestHarness.DayDemoRecord> {

        private volatile int dayOfWeek = 0;

        public DayDemoRecordSourceFunction(int numCheckpoints, boolean failAfterCheckpoint) {
            super(numCheckpoints, failAfterCheckpoint);
        }

        @Override
        protected Supplier<TestHarness.DayDemoRecord> generate() {
            return new Supplier<TestHarness.DayDemoRecord>() {
                @Override
                public TestHarness.DayDemoRecord get() {
                    return new TestHarness.DayDemoRecord(dayOfWeek++ % 7);
                }
            };
        }

        @Override
        public Long snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
            super.snapshotState(checkpointId, checkpointTimestamp);
            return Long.valueOf(dayOfWeek);
        }

        @Override
        public void restoreState(Long state) throws Exception {
            super.restoreState(state);
            dayOfWeek = state.intValue();
        }
    }
}
