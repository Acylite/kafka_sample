package sample.data.transformations;

// import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import sample.data.models.surveyvote;
import sample.data.transformations.TransformationStrategy.TransformationStrategyString;
import sample.data.util.KafkaStreamingOptions;

public class OneQuestionSurveyVotingHistoryStrategy implements TransformationStrategyString<surveyvote> {

    public PCollection<surveyvote> transform(PCollection<KafkaRecord<String, String>> kafkaMessages, KafkaStreamingOptions options) {
        String env = options.getEnv();
        return kafkaMessages
                .apply("Map to java class using class",
                        MapElements.into(TypeDescriptor.of(surveyvote.class))
                                .via(element -> element != null ? new surveyvote(element, env) : null));
    }
}
