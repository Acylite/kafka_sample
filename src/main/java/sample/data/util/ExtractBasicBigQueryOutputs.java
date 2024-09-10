package sample.data.util;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;

import java.util.Date;
public class ExtractBasicBigQueryOutputs {

  public static TableSchema tableSchema = new TableSchema().setFields(
      ImmutableList.of(
          new TableFieldSchema()
              .setName("messageId").setMode("NULLABLE").setType("STRING"),
          new TableFieldSchema()
              .setName("kafkaTimestamp").setMode("NULLABLE").setType("TIMESTAMP"),
          new TableFieldSchema()
              .setName("processingTimestamp").setMode("NULLABLE").setType("TIMESTAMP"),
          new TableFieldSchema()
              .setName("topicName").setMode("NULLABLE").setType("STRING"),
          new TableFieldSchema()
              .setName("kafkaPartition").setMode("NULLABLE").setType("INTEGER"),
          new TableFieldSchema()
              .setName("offset").setMode("NULLABLE").setType("INTEGER"),
          new TableFieldSchema()
              .setName("messageContent").setMode("NULLABLE").setType("STRING")
      )
  );
  
  public static class ExtractBasicBigQueryOutputsGeneric extends DoFn<KafkaRecord<String, GenericRecord>, TableRow> {

    private final Logger LOG;

    public ExtractBasicBigQueryOutputsGeneric(Logger loggerInput) {
      LOG = loggerInput;
    }

    public static TableSchema tableSchema = ExtractBasicBigQueryOutputs.tableSchema;

    @ProcessElement
    public void processElement(@Element KafkaRecord<String, GenericRecord> element,
      OutputReceiver<TableRow> receiver,
      ProcessContext context) {
      
      try {
        long currentTimestamp = System.currentTimeMillis();
        Date processingTimestamp = new Date(currentTimestamp);
        Date kafkaTimestamp = new Date(element.getTimestamp());
        String messageId = element.getKV().getKey();
        String topicName = element.getTopic();
        int kafkaPartition = element.getPartition();
        long offset = element.getOffset();
        GenericRecord record = element.getKV().getValue();

        String messageContent = (record != null) ? record.toString() : null;

        TableRow row = new TableRow();
        row.set("messageId", messageId);
        row.set("kafkaTimestamp", kafkaTimestamp);
        row.set("processingTimestamp", processingTimestamp);
        row.set("topicName", topicName);
        row.set("kafkaPartition", kafkaPartition);
        row.set("offset", offset);
        row.set("messageContent", messageContent);
        receiver.output(row);
      } catch (Exception e) {
        LOG.error("Could not convert kafka element to BigQuery Record:\n".concat(e.toString()));
      }
    }
  }

  public static class ExtractBasicBigQueryOutputsString extends DoFn<KafkaRecord<String, String>, TableRow> {

    private final Logger LOG;

    public ExtractBasicBigQueryOutputsString(Logger loggerInput) {
      LOG = loggerInput;
    }

    public static TableSchema tableSchema = ExtractBasicBigQueryOutputs.tableSchema;

    @ProcessElement
    public void processElement(@Element KafkaRecord<String, String> element,
      OutputReceiver<TableRow> receiver,
      ProcessContext context) {
        
      try {
        long currentTimestamp = System.currentTimeMillis();
        Date processingTimestamp = new Date(currentTimestamp);
        Date kafkaTimestamp = new Date(element.getTimestamp());
        String messageId = element.getKV().getKey();
        String topicName = element.getTopic();
        int kafkaPartition = element.getPartition();
        long offset = element.getOffset();
        
        String record = element.getKV().getValue();

        String messageContent = (record != null) ? record.toString() : null;

        TableRow row = new TableRow();
        row.set("messageId", messageId);
        row.set("kafkaTimestamp", kafkaTimestamp);
        row.set("processingTimestamp", processingTimestamp);
        row.set("topicName", topicName);
        row.set("kafkaPartition", kafkaPartition);
        row.set("offset", offset);
        row.set("messageContent", messageContent);
        receiver.output(row);
      } catch (Exception e) {
        LOG.error("Could not convert kafka element to BigQuery Record:\n".concat(e.toString()));
      }
    }
  }
}