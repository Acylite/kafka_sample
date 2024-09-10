package sample.data.util;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.PDone;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import sample.data.customKafkaDeserializer.GenericRecordWithSchemaId;
import sample.data.models.MessageBase;
import sample.data.util.ExtractBasicBigQueryOutputs.ExtractBasicBigQueryOutputsGeneric;
import sample.data.util.ExtractBasicBigQueryOutputs.ExtractBasicBigQueryOutputsString;

import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.WriteSuccessSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkFactory {

    private static final Logger LOG = LoggerFactory.getLogger(SinkFactory.class);

    public static  <T extends MessageBase> PTransform<PCollection<T>, WriteResult> createBigQuerySink(
            KafkaStreamingOptions options, TableSchema schema) {

        String parsedTable = options.getParsedTableName();
        String bqDataset = options.getDatasetName();
        String env = options.getEnv();

        // check if schema is null and if so, throw an error
        if (schema == null) {
            throw new NullPointerException("Schema cannot be null");
        }

        return new PTransform<>() {
            @Override
            public WriteResult expand(PCollection<T> input) {
                // Convert BetTransactionDetail to TableRow
                PCollection<TableRow> formattedRows = input
                        .apply("Convert Element to TableRow",
                                MapElements.into(TypeDescriptor.of(TableRow.class))
                                        .via((SerializableFunction<T, TableRow>)
                                                message -> message != null
                                                        ? (message).toTableRow()
                                                        : null));


                // Use the WriteToBigQuery class to write the TableRows to BigQuery
                return formattedRows.apply("Write to BigQuery",
                        new WriteToBigQuery("data-" + env, bqDataset, parsedTable,
                                schema));
            }
        };
    }

    public static <I extends IndexedRecord> PTransform<PCollection<KafkaRecord<String, GenericRecord>>, WriteResult> createRawBigQuerySinkGeneric(
            KafkaStreamingOptions options) {

        String rawTable = options.getRawTableName();
        String bqDataset = options.getDatasetName();
        String env = options.getEnv();

        // String env = options.getEnv();

        return new PTransform<PCollection<KafkaRecord<String, GenericRecord>>, WriteResult>() {
            @Override
            public WriteResult expand(PCollection<KafkaRecord<String, GenericRecord>> input) {
                // Convert BetTransactionDetail to TableRow
                PCollection<TableRow> partitionedRawRows = input
                        .apply("Format raw BQ row", ParDo.of(new ExtractBasicBigQueryOutputsGeneric(LOG)));

                // Use the WriteToBigQuery class to write the TableRows to BigQuery
                return partitionedRawRows.apply("Write raw BQ row",
                        //new util.WriteToBigQuery("data-" + env, bqDataset, rawTable,
                        new WriteToBigQuery("data-" + env, bqDataset, rawTable,
                                ExtractBasicBigQueryOutputsGeneric.tableSchema));
            }
        };
    }

    public static <I extends IndexedRecord> PTransform<PCollection<KafkaRecord<String, GenericRecordWithSchemaId>>, WriteResult> createRawBigQuerySinkGenericSchema(
            KafkaStreamingOptions options) {

        String rawTable = options.getRawTableName();
        String bqDataset = options.getDatasetName();
        String env = options.getEnv();

        // String env = options.getEnv();

        return new PTransform<PCollection<KafkaRecord<String, GenericRecordWithSchemaId>>, WriteResult>() {
            @Override
            public WriteResult expand(PCollection<KafkaRecord<String, GenericRecordWithSchemaId>> input) {
                // Convert BetTransactionDetail to TableRow
                PCollection<TableRow> partitionedRawRows = input
                        .apply("Format raw BQ row", ParDo.of(new ExtractBasicBigQueryOutputsSchema(LOG)));

                // Use the WriteToBigQuery class to write the TableRows to BigQuery
                return partitionedRawRows.apply("Write raw BQ row",
                        //new util.WriteToBigQuery("data-" + env, bqDataset, rawTable,
                        new WriteToBigQuery("data-" + env, bqDataset, rawTable,
                                ExtractBasicBigQueryOutputsGeneric.tableSchema));
            }
        };
    }

    public static PTransform<PCollection<KafkaRecord<String, String>>, WriteResult> createRawBigQuerySinkString(
            KafkaStreamingOptions options) {

        String rawTable = options.getRawTableName();
        String bqDataset = options.getDatasetName();
        String env = options.getEnv();

        return new PTransform<PCollection<KafkaRecord<String, String>>, WriteResult>() {
            @Override
            public WriteResult expand(PCollection<KafkaRecord<String, String>> input) {
                // Convert BetTransactionDetail to TableRow
                PCollection<TableRow> partitionedRawRows = input
                        .apply("Format raw BQ row", ParDo.of(new ExtractBasicBigQueryOutputsString(LOG)));

                // Use the WriteToBigQuery class to write the TableRows to BigQuery
                return partitionedRawRows.apply("Write raw BQ row",
                        //new util.WriteToBigQuery("data-" + env, bqDataset, rawTable,
                        new WriteToBigQuery("data-" + env, bqDataset, rawTable,
                                ExtractBasicBigQueryOutputsString.tableSchema));
            }
        };
    }

    public static <T> PTransform<PCollection<T>, PDone> createConsoleSink() {
        return new PTransform<PCollection<T>, PDone>() {
            @Override
            public PDone expand(PCollection<T> input) {
                input.apply("Print to Console", ParDo.of(new DoFn<T, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        System.out.println("Printing message....");
                        System.out.println(c.element().toString());
                    }
                }));

                return PDone.in(input.getPipeline());
            }
        };
    }

    public static <T extends MessageBase> PTransform<PCollection<T>, PDone> createPubSubSink(String projectId, String topic) {
        return new PTransform<>() {
            @Override
            public PDone expand(PCollection<T> input) {
                PCollection<PubsubMessage> pubsubOutput = input
                        .apply("Convert BetTransactionDetails to PubSub format",
                                MapElements.into(TypeDescriptor.of(PubsubMessage.class))
                                        .via((SerializableFunction<T, PubsubMessage>)
                                                message -> message != null
                                                        ? PubSubMessageBuilder.build(message)
                                                        : null));


                return pubsubOutput.apply("Write to PubSub",
                        PubsubIO.writeMessages()
                                .to(String.format("projects/%s/topics/%s", projectId, topic)));
            }
        };
    }

}
