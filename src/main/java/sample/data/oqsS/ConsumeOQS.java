package sample.data.oqsS;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Field;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import sample.data.customKafkaDeserializer.CustomNullableAvroCoder;
import sample.data.customKafkaDeserializer.GenericRecordWithSchemaId;
import sample.data.models.MessageBase;
import sample.data.transformations.TransformationContext.TransformationContextString;
import sample.data.transformations.TransformationStrategy.TransformationStrategyString;
import sample.data.util.KafkaConsumerFactoryFn;
import sample.data.util.KafkaStreamingOptions;
import sample.data.util.PropertiesLoader;
import sample.data.util.SinkFactory;

//import org.apache.kafka.common.serialization.IntegerDeserializer;
// import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.google.api.services.bigquery.model.TableSchema;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConsumeOneQuestionSurvey {
  private static final Logger LOG = LoggerFactory.getLogger(ConsumeOneQuestionSurvey.class);

  public static void main(String[] args) throws IOException, RestClientException {
    System.out.println("array of args....");
    System.out.println(Arrays.toString(args));
    KafkaStreamingOptions options = PipelineOptionsFactory
            .fromArgs(args)
            .withValidation()
            .as(KafkaStreamingOptions.class);

    LOG.info("Starting Pipeline");

    // issue with wrong job name used. temp fix. check single quotes
    //options.setJobName("amtest2");

    System.out.println(options.getBootstrapServers());
    System.out.println(options.getSchemaRegistry());
    System.out.println(options.getJobName());
    run(options);
  }


  private static PCollection<KafkaRecord<String, String>> readFromKafka(Pipeline p, KafkaStreamingOptions options) {

    String env = options.getEnv();
    LOG.info("Running on " + env);

    HashMap<String, Object> props = PropertiesLoader.loadProperties(env, options);
    List<String> topics = Collections.singletonList(options.getTopic());

    LOG.info("topics to consume: ".concat(String.join(", ", topics)));
    LOG.info("Consumer properties: {}", props);

    CoderRegistry coderRegistry = p.getCoderRegistry();
    coderRegistry.registerCoderForClass(GenericRecordWithSchemaId.class, new CustomNullableAvroCoder());

    return p.apply("ReadFromKafka",
            KafkaIO.<String, String>read()
                    .withReadCommitted()        // restrict reader to committed messages on Kafka
                    .withBootstrapServers(options.getBootstrapServers())
                    .withTopics(topics)
                    .withKeyDeserializer(StringDeserializer.class)
                    .withValueDeserializer(StringDeserializer.class)
                    .withConsumerConfigUpdates(props)
                    .withConsumerFactoryFn(new KafkaConsumerFactoryFn())
    );
  }

  private static <T extends MessageBase> void writeToConsole(PCollection<T> processedData) {
    processedData.apply("Write to Console", SinkFactory.createConsoleSink());
  }

  private static void writeToBigQueryRaw(PCollection<KafkaRecord<String, String>> kafkaMessages, KafkaStreamingOptions options) {
    kafkaMessages.apply("Write to BigQuery Raw", SinkFactory.createRawBigQuerySinkString(options));
  }

  private static <T extends MessageBase> void writeToBigQuery(PCollection<T> processedData, KafkaStreamingOptions options) {
    String modelClassName = options.getModelClassName();
    try {
      // Load the model class dynamically
      Class<?> modelClass = Class.forName(modelClassName);

      // Use reflection to access the static BqTableSchema field
      Field schemaField = modelClass.getField("BqTableSchema");
      TableSchema schema = (TableSchema) schemaField.get(null); // null for static fields

      // Apply the BigQuery sink with the dynamically obtained schema
      processedData.apply("Write to BigQuery", SinkFactory.createBigQuerySink(options, schema));
    } catch (ClassNotFoundException | NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException("Failed to load BqTableSchema from model class: " + modelClassName, e);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T loadStrategy(String className) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
    Class<?> clazz = Class.forName(className);
    return (T) clazz.getDeclaredConstructor().newInstance();
  }
  
  private static <T extends MessageBase> PCollection<T> transform(
          PCollection<KafkaRecord<String, String>> kafkaMessages,
          KafkaStreamingOptions options) {

    // Dynamically load the transformation strategy based on the class name provided in options
    String transformationClassName = options.getTransformationStrategy();
    TransformationStrategyString<T> transformationStrategy;

    try {
      transformationStrategy = loadStrategy(transformationClassName);
    } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException |
             InstantiationException | InvocationTargetException e) {
      throw new RuntimeException("Failed to load transformation strategy: " + transformationClassName, e);
    }

    // Use the loaded strategy with the transformation context
    TransformationContextString<T> context = new TransformationContextString<>(transformationStrategy);
    return context.executeTransformation(kafkaMessages, options);
  }

  // Run the pipeline
  public static PipelineResult run(KafkaStreamingOptions options)
          throws IOException, RestClientException {

    Pipeline p = Pipeline.create(options);
    PCollection<KafkaRecord<String, String>> kafkaMessages = readFromKafka(p, options);

    // transform messages for BQ
    PCollection<MessageBase> oqsTransformed = transform(kafkaMessages, options);

    // write to output sinks
    writeToBigQueryRaw(kafkaMessages, options);
    writeToBigQuery(oqsTransformed, options);
    writeToConsole(oqsTransformed);

    return p.run();
  }
}
