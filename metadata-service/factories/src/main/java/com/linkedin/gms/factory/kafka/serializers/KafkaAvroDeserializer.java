package com.linkedin.gms.factory.kafka.serializers;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

@Slf4j
public class KafkaAvroDeserializer<T extends GenericRecord> implements Deserializer<T> {

    protected final Class<T> targetType;

    public KafkaAvroDeserializer(Class<T> targetType) {
        this.targetType = targetType;
    }

    @Override
    public void configure(Map configs, boolean isKey) {
        // do nothing
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        T returnObject = null;

        try {

            if (bytes != null) {
                DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(targetType.newInstance().getSchema());
                Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
                returnObject = (T) datumReader.read(null, decoder);
                log.info("deserialized data='{}'", returnObject.toString());
            }
        } catch (Exception e) {
            log.error("Unable to Deserialize bytes[] ", e);
        }

        return returnObject;
    }

    @Override
    public void close() {
        // do nothing
    }
}