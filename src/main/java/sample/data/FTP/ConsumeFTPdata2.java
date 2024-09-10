package sample.data.FTP;

import static sample.data.models.BU.UP.SchemaFields;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import sample.data.customKafkaDeserializer.CustomNullableAvroCoder;
import sample.data.customKafkaDeserializer.GenericRecordWithSchemaId;
import sample.data.models.BU.UP;
import sample.data.util.KafkaConsumerFactoryFn;
import sample.data.util.KafkaStreamingOptions;
import sample.data.util.PropertiesLoader;
import sample.data.util.SinkFactory;

import org.apache.kafka.common.serialization.StringDeserializer;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableSchema;


public class ConsumeFTPdata2 {
  private static final Logger LOG = LoggerFactory.getLogger(ConsumeFTPdata2.class);

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

  private static void writeToBigQueryRaw(PCollection<KafkaRecord<String, String>> kafkaMessages, KafkaStreamingOptions options) {
    kafkaMessages.apply("Write to BigQuery Raw", SinkFactory.createRawBigQuerySinkString(options));
  }

  

  // Run the pipeline
  public static PipelineResult run(KafkaStreamingOptions options)
          throws IOException, RestClientException {
    String env = options.getEnv();
    Pipeline p = Pipeline.create(options);
    PCollection<KafkaRecord<String, String>> kafkaMessages = readFromKafka(p, options);
    // transform messages for BQ
    PCollection<UP> predsTransformed = kafkaMessages
                .apply("Map to java class using class",
                        MapElements.into(TypeDescriptor.of(UP.class))
                                .via(element -> {
                                    try {
                                        return element != null ? new UP(element, env) : null;
                                    } catch (ParseException e) {
                                        throw new RuntimeException(e);
                                    }
                                }));

    // write to output sinks
    writeToBigQueryRaw(kafkaMessages, options); 
    TableSchema schema = SchemaFields; // null for static fields
    predsTransformed
                .apply("Write to BigQuery", SinkFactory.createBigQuerySink(options, schema));
   
    return p.run();
  }
}
