package org.numenta.nupic.flink.streaming.api.codegen;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.numenta.nupic.flink.streaming.api.operator.EncoderInputFunction;
import org.apache.flink.streaming.util.FieldAccessor;
import org.numenta.nupic.encoders.Encoder;
import org.numenta.nupic.encoders.EncoderTuple;
import org.numenta.nupic.encoders.MultiEncoder;
import org.numenta.nupic.encoders.ScalarEncoder;

import java.util.*;
import java.util.stream.Collectors;
import java.util.function.Function;


/**
 * Generates code to transform input elements to a value map as understood by {@Link MultiEncoder}.
 *
 * @param <T> the input type to transform.
 */
public class GenerateEncoderInputFunction<T> {

    private CompositeType<T> inputTypeInfo;
    private MultiEncoder inputTypeEncoder;
    private ExecutionConfig config;

    public GenerateEncoderInputFunction(CompositeType<T> inputTypeInfo, MultiEncoder inputTypeEncoder, ExecutionConfig config) {

        if(inputTypeEncoder == null)
            throw new IllegalArgumentException("a network encoder must be provided");

        this.inputTypeInfo = inputTypeInfo;
        this.inputTypeEncoder = inputTypeEncoder;
        this.config = config;
    }

    /**
     * Generate an input function for a given input type.
     * The generated function transforms input elements to an compatible format for the encoder.
     *
     * @return a function to be applied to the input elements.
     */
    public EncoderInputFunction<T> generate() {

        final List<EncoderTuple> encoders = this.inputTypeEncoder.getEncoders(this.inputTypeEncoder);

        // verify that all encoder fields have corresponding input fields
        final List<EncoderTuple> nonExistentFields = encoders.stream()
                .filter(encoder -> !this.inputTypeInfo.hasField(encoder.getName()))
                .collect(Collectors.toList());
        if(nonExistentFields.size() > 0) {
            throw new IllegalArgumentException(String.format("encoder refers to non-existent field(s) [%s]",
                    nonExistentFields.stream().map(f -> f.getName()).collect(Collectors.joining(","))));
        }

        // lookup an accessor function for each field
        final Map<String, Function<T,Object>> accessors = encoders.stream()
                .collect(Collectors.toMap(EncoderTuple::getName, encoder -> fieldAccessorFor(encoder)));

        return new EncoderInputFunction<T>() {
            @Override
            public Map<String, Object> map(T input) {
                // create a map of field values for use by the encoder
                final Map<String, Object> inputMap = new LinkedHashMap<>(accessors.size());
                for(Map.Entry<String, Function<T,Object>> entry : accessors.entrySet()) {
                    inputMap.put(entry.getKey(), entry.getValue().apply(input));
                }
                return inputMap;
            }
        };
    }

    /**
     * Create an accessor function for the given field.
     *
     * The accessor must convert the value of the given field into a
     * value that the respective encoder understands.
     *
     * @param encoderTuple the field and associated encoder.
     * @return a function to produce the input value for the encoder.
     */
    private Function<T,Object> fieldAccessorFor(EncoderTuple encoderTuple) {
        FieldAccessor fieldAccessor = FieldAccessor.create(encoderTuple.getName(), inputTypeInfo, config);
        Encoder encoder = encoderTuple.getEncoder();

        if(encoder instanceof ScalarEncoder) {
            if(!Number.class.isAssignableFrom(fieldAccessor.getFieldType().getTypeClass()))
                throw new IllegalArgumentException(String.format("the type of field [%s] is incompatible with the configured scalar encoder", encoderTuple.getName()));
            if(Double.class == fieldAccessor.getFieldType().getTypeClass())
                return (T input) -> fieldAccessor.get(input);
            else
                return (T input) -> new Double(((Number) fieldAccessor.get(input)).doubleValue());
        } else {
            return (T input) -> fieldAccessor.get(input);
        }
    }
}
