package sample.data.transformations;

// import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.values.PCollection;

import sample.data.customKafkaDeserializer.GenericRecordWithSchemaId;
import sample.data.models.MessageBase;
import sample.data.transformations.TransformationStrategy.TransformationStrategyGeneric;
import sample.data.transformations.TransformationStrategy.TransformationStrategyString;
import sample.data.util.KafkaStreamingOptions;


public class TransformationContext {
    public static class TransformationContextGeneric<T extends MessageBase> {
        private TransformationStrategyGeneric<T> strategy;

        public TransformationContextGeneric(TransformationStrategyGeneric<T> strategy) {
            this.strategy = strategy;
        }

        public void setStrategy(TransformationStrategyGeneric<T> strategy) {
            this.strategy = strategy;
        }

        public PCollection<T> executeTransformation(PCollection<KafkaRecord<String, GenericRecordWithSchemaId>> kafkaMessages, KafkaStreamingOptions options) {
            return strategy.transform(kafkaMessages, options);
        }
        
    }

    public static class TransformationContextString<T extends MessageBase> {
        private TransformationStrategyString<T> strategy;

        public TransformationContextString(TransformationStrategyString<T> strategy) {
            this.strategy = strategy;
        }

        public void setStrategy(TransformationStrategyString<T> strategy) {
            this.strategy = strategy;
        }

        public PCollection<T> executeTransformation(PCollection<KafkaRecord<String, String>> kafkaMessages, KafkaStreamingOptions options) {
            return strategy.transform(kafkaMessages, options);
        }
    }
}