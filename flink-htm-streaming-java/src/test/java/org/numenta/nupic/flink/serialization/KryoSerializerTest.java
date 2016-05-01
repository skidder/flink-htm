package org.numenta.nupic.flink.serialization;

import static org.junit.Assert.*;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.Before;
import org.junit.Test;
import org.numenta.nupic.util.Tuple;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.junit.Assert.*;

/**
 * Test the KryoSerializer.
 */
public class KryoSerializerTest {

    public Kryo createKryo() {
        Kryo.DefaultInstantiatorStrategy initStrategy = new Kryo.DefaultInstantiatorStrategy();

        // use Objenesis to create classes without calling the constructor (Flink's technique)
        //initStrategy.setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());

        Kryo kryo = new Kryo();
        kryo.setInstantiatorStrategy(initStrategy);
        return kryo;
    }

    private Kryo kryo;

    @Before
    public void before() {
        kryo = createKryo();
        kryo.register(Tuple.class, new KryoSerializer<>());
    }

    @Test
    public void testReadWrite() throws Exception {

        // write numerous objects to the stream, to verify that the read buffers
        // aren't too greedy

        Tuple expected1 = new Tuple(42);
        Tuple expected2 = new Tuple(101);

        ByteArrayOutputStream baout = new ByteArrayOutputStream();
        Output output = new Output(baout);
        kryo.writeObject(output, expected1);
        kryo.writeObject(output, expected2);
        output.close();

        ByteArrayInputStream bain = new ByteArrayInputStream(baout.toByteArray());
        Input input = new Input(bain);

        Tuple actual1 = kryo.readObject(input, Tuple.class);
        assertNotSame(expected1, actual1);
        assertEquals(expected1, actual1);

        Tuple actual2 = kryo.readObject(input, Tuple.class);
        assertNotSame(expected2, actual2);
        assertEquals(expected2, actual2);
    }

    @Test
    public void testCopy() throws Exception {
        Tuple expected = new Tuple(42);

        Tuple actual = kryo.copy(expected);
        assertNotSame(expected, actual);
        assertEquals(expected, actual);
    }
}