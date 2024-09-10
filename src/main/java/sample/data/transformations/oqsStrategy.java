package sample.data.transformations;

// import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import sample.data.models.surveyquestion;
import sample.data.transformations.TransformationStrategy.TransformationStrategyString;
import sample.data.util.KafkaStreamingOptions;

public class OneQuestionSurveyQuestionStrategy implements TransformationStrategyString<surveyquestion> {

    public PCollection<surveyquestion> transform(PCollection<KafkaRecord<String, String>> kafkaMessages, KafkaStreamingOptions options) {
        String env = options.getEnv();
        return kafkaMessages
                .apply("Map to java class using class",
                        MapElements.into(TypeDescriptor.of(surveyquestion.class))
                                .via(element -> element != null ? new surveyquestion(element, env) : null));
    }
}
