package sample.data.transformations;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.values.PCollection;

import sample.data.customKafkaDeserializer.GenericRecordWithSchemaId;
import sample.data.models.MessageBase;
import sample.data.util.KafkaStreamingOptions;

public interface TransformationStrategy {
    // Interface defining a transformation strategy for Kafka messages
    interface TransformationStrategyGeneric<T extends MessageBase> {
        PCollection<T> transform(PCollection<KafkaRecord<String, GenericRecordWithSchemaId>> kafkaMessages, KafkaStreamingOptions options);
    }

    // Interface defining another transformation strategy for Kafka messages
    interface TransformationStrategyString<T extends MessageBase> {
        PCollection<T> transform(PCollection<KafkaRecord<String, String>> kafkaMessages, KafkaStreamingOptions options);
    }
}