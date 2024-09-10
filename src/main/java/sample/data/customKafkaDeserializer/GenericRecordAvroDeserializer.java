package sample.data.customKafkaDeserializer;

import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import java.util.Map;

public class GenericRecordAvroDeserializer extends
        AbstractKafkaAvroDeserializer implements Deserializer<GenericRecord> {

    @Override
    public GenericRecord deserialize(String s, byte[] bytes) {
        return (GenericRecord) this.deserialize(bytes);
    }

    public void configure(Map<String, ?> configs, boolean isKey) {
        this.configure(new KafkaAvroDeserializerConfig(configs));
    }

    @Override
    public void close() {}

}
