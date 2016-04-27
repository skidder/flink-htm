package org.numenta.nupic.flink.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.flink.api.common.ExecutionConfig;
import org.numenta.nupic.Persistable;
import org.numenta.nupic.network.Network;
import org.numenta.nupic.serialize.HTMObjectInput;
import org.numenta.nupic.serialize.HTMObjectOutput;
import org.numenta.nupic.serialize.SerialConfig;
import org.numenta.nupic.serialize.SerializerCore;

import java.io.IOException;
import java.io.Serializable;

/**
 *  Kryo serializer for HTM network and related objects.
 *
 */
public class KryoSerializer<T> extends Serializer<T> implements Serializable {

    private final SerializerCore serializer = new SerializerCore(SerialConfig.DEFAULT_REGISTERED_TYPES);

    /**
     *
     * @param kryo      instance of {@link Kryo} object
     * @param output    a Kryo {@link Output} object
     * @param t         instance to serialize
     */
    @Override
    public void write(Kryo kryo, Output output, T t) {
        try {
            HTMObjectOutput writer = serializer.getObjectOutput(output);
            writer.writeObject(t, t.getClass());
            writer.flush();
        }
        catch(IOException e) {
            throw new KryoException(e);
        }
    }

    /**
     *
     * @param kryo      instance of {@link Kryo} object
     * @param input     a Kryo {@link Input}
     * @param aClass    The class of the object to be read in.
     * @return  an instance of type &lt;T&gt;
     */
    @Override
    public T read(Kryo kryo, Input input, Class<T> aClass) {
        try {
            HTMObjectInput reader = serializer.getObjectInput(input);
            T t = (T) reader.readObject(aClass);
            return t;
        }
        catch(Exception e) {
            throw new KryoException(e);
        }
    }

    /**
     * Register the HTM types with the Kryo serializer.
     * @param config
     */
    public static void registerTypes(ExecutionConfig config) {
        for(Class<?> c : SerialConfig.DEFAULT_REGISTERED_TYPES) {
            config.registerTypeWithKryoSerializer(c, (Class<? extends Serializer<?>>) (Class<?>) KryoSerializer.class);
        }
        for(Class<?> c : KryoSerializer.ADDITIONAL_REGISTERED_TYPES) {
            config.registerTypeWithKryoSerializer(c, (Class<? extends Serializer<?>>) (Class<?>) KryoSerializer.class);
        }
    }

    static final Class<?>[] ADDITIONAL_REGISTERED_TYPES = { Network.class };
}