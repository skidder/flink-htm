package org.numenta.nupic.flink.streaming.api;

import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Iterator;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Emit records until exactly numCheckpoints occur.
 *
 * Checkpoints are based on stream barriers that flow with the records;
 * by ceasing to emit records after checkpoint X, the sink is guaranteed to
 * not receive any records after checkpoint X.  This arrangement stabilizes the test.
 */
public abstract class TestSourceFunction<T> implements SourceFunction<T>, Checkpointed<Long>, CheckpointListener {
    private static final long serialVersionUID = 1L;

    private volatile boolean running = true;

    private int numCheckpoints;
    private boolean failAfterCheckpoint;

    private volatile boolean failOnNext = false;

    /**
     * Create a test source function that runs until a given number of checkpoints.
     * @param numCheckpoints the number of checkpoints to run for.
     * @param failAfterCheckpoint indicates whether to simulate a failure after the first checkpoint.
     */
    public TestSourceFunction(int numCheckpoints, boolean failAfterCheckpoint) {
        this.numCheckpoints = numCheckpoints;
        this.failAfterCheckpoint = failAfterCheckpoint;
    }

    /**
     * Generate an unbounded sequence of elements.
     */
    protected abstract Supplier<T> generate();

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        Iterator<T> inputs = Stream.generate(generate()).iterator();
        while(running) {
            synchronized (ctx.getCheckpointLock()) {
                if(running) {
                    if(failOnNext) {
                        failOnNext = false;
                        throw new Exception("Artificial Failure");
                    }
                    assert(inputs.hasNext());
                    ctx.collect(inputs.next());
                    Thread.sleep(10);
                }
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public Long snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
        if(--numCheckpoints == 0) {
            running = false;
        }
        return 0L;
    }

    @Override
    public void restoreState(Long state) throws Exception {
        // do not cause repeated failures
        failAfterCheckpoint = false;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if(running && failAfterCheckpoint) {
            // any simulated failure should come after the entire checkpoint has been taken.
            failOnNext = true;
        }
    }
}
