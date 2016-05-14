package org.numenta.nupic.flink.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.numenta.nupic.Persistable;
import org.numenta.nupic.network.Network;
import org.numenta.nupic.serialize.HTMObjectInput;
import org.numenta.nupic.serialize.HTMObjectOutput;
import org.numenta.nupic.serialize.SerialConfig;
import org.numenta.nupic.serialize.SerializerCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *  Kryo serializer for HTM network and related objects.
 *
 */
public class KryoSerializer<T extends Persistable> extends Serializer<T> implements Serializable {

    protected static final Logger LOGGER = LoggerFactory.getLogger(KryoSerializer.class);

    private final SerializerCore serializer = new SerializerCore(SerialConfig.DEFAULT_REGISTERED_TYPES);

    /**
     * Write the given instance to the given output.
     *
     * @param kryo      instance of {@link Kryo} object
     * @param output    a Kryo {@link Output} object
     * @param t         instance to serialize
     */
    @Override
    public void write(Kryo kryo, Output output, T t) {
        try {
            preSerialize(t);

            try(ByteArrayOutputStream stream = new ByteArrayOutputStream(4096)) {

                // write the object using the HTM serializer
                HTMObjectOutput writer = serializer.getObjectOutput(stream);
                writer.writeObject(t, t.getClass());
                writer.close();

                // write the serialized data
                output.writeInt(stream.size());
                stream.writeTo(output);

                LOGGER.debug("wrote {} bytes", stream.size());
            }
        }
        catch(IOException e) {
            throw new KryoException(e);
        }
    }

    /**
     * Read an instance of the given class from the given input.
     *
     * @param kryo      instance of {@link Kryo} object
     * @param input     a Kryo {@link Input}
     * @param aClass    The class of the object to be read in.
     * @return  an instance of type &lt;T&gt;
     */
    @Override
    public T read(Kryo kryo, Input input, Class<T> aClass) {

        // read the serialized data
        byte[] data = new byte[input.readInt()];
        input.readBytes(data);

        try {
            try(ByteArrayInputStream stream = new ByteArrayInputStream(data)) {
                HTMObjectInput reader = serializer.getObjectInput(stream);
                T t = (T) reader.readObject(aClass);

                postDeSerialize(t);

                return t;
            }
        }
        catch(Exception e) {
            throw new KryoException(e);
        }
    }

    /**
     * Copy the given instance.
     * @param kryo instance of {@link Kryo} object
     * @param original an object to copy.
     * @return
     */
    @Override
    public T copy(Kryo kryo, T original) {
        try {
            preSerialize(original);

            try(CopyStream output = new CopyStream(4096)) {
                HTMObjectOutput writer = serializer.getObjectOutput(output);
                writer.writeObject(original, original.getClass());
                writer.close();

                try(InputStream input = output.toInputStream()) {
                    HTMObjectInput reader = serializer.getObjectInput(input);
                    T t = (T) reader.readObject(original.getClass());

                    postDeSerialize(t);

                    return t;
                }
            }
        }
        catch(Exception e) {
            throw new KryoException(e);
        }
    }

    static class CopyStream extends ByteArrayOutputStream {
        public CopyStream(int size) { super(size); }

        /**
         * Get an input stream based on the contents of this output stream.
         * Do not use the output stream after calling this method.
         * @return an {@link InputStream}
         */
        public InputStream toInputStream() {
            return new ByteArrayInputStream(this.buf, 0, this.count);
        }
    }

    /**
     * The HTM serializer handles the Persistable callbacks automatically, but
     * this method is for any additional actions to be taken.
     * @param t the instance to be serialized.
     */
    protected void preSerialize(T t) {
    }

    /**
     * The HTM serializer handles the Persistable callbacks automatically, but
     * this method is for any additional actions to be taken.
     * @param t the instance newly deserialized.
     */
    protected void postDeSerialize(T t) {
    }

    /**
     * Register the HTM types with the Kryo serializer.
     * @param config
     */
    public static void registerTypes(ExecutionConfig config) {
        for(Class<?> c : SerialConfig.DEFAULT_REGISTERED_TYPES) {
            Class<?> serializerClass = DEFAULT_SERIALIZERS.getOrDefault(c, (Class<?>) KryoSerializer.class);
            config.registerTypeWithKryoSerializer(c, (Class<? extends Serializer<?>>) serializerClass);
        }
        for(Class<?> c : KryoSerializer.ADDITIONAL_REGISTERED_TYPES) {
            Class<?> serializerClass = DEFAULT_SERIALIZERS.getOrDefault(c, (Class<?>) KryoSerializer.class);
            config.registerTypeWithKryoSerializer(c, (Class<? extends Serializer<?>>) serializerClass);
        }
    }

    static final Class<?>[] ADDITIONAL_REGISTERED_TYPES = { Network.class };

    /**
     * A map of serializers for various classes.
     */
    static final Map<Class<?>,Class<?>> DEFAULT_SERIALIZERS = Stream.<Tuple2<Class,Class>>of(
            // new Tuple2<>(Network.class, NetworkSerializer.class)
    ).collect(Collectors.toMap(kv -> kv.f0, kv -> kv.f1));
}