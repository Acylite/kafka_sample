package sample.data.customKafkaDeserializer;

import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class GenericRecordWithSchemaAvroDeserializer extends
        AbstractKafkaAvroDeserializer implements Deserializer<GenericRecordWithSchemaId> {

    @Override
    public GenericRecordWithSchemaId deserialize(String s, byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        ByteBuffer buffer = getByteBuffer(bytes);

        if (buffer == null) {
            return null;
        }

        // Read the next 4 bytes as the schema ID
        int schemaId = buffer.getInt();

        GenericRecord record = null;
        try {
            record = (GenericRecord) this.deserialize(bytes);
        } catch (Exception e) {
            // Handle deserialization exception
            e.printStackTrace();
            return null;
        }

        if (record == null) {
            return null;
        }

        GenericRecordWithSchemaId recordWithSchemaId = new GenericRecordWithSchemaId(record, schemaId);

        return recordWithSchemaId;
    }

    public void configure(Map<String, ?> configs, boolean isKey) {
        this.configure(new KafkaAvroDeserializerConfig(configs));
    }

    @Override
    public void close() {
    }

    private ByteBuffer getByteBuffer(byte[] payload) {
        if (payload == null) {
            return null;
        }

        ByteBuffer buffer = ByteBuffer.wrap(payload);
        if (buffer.get() != 0) {
            throw new SerializationException("Unknown magic byte!");
        } else {
            return buffer;
        }
    }
}
