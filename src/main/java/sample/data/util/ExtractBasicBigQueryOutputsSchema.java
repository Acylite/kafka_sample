package sample.data.util;

import java.io.Serializable;
import java.util.Date;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;

import sample.data.customKafkaDeserializer.GenericRecordWithSchemaId;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;

public class ExtractBasicBigQueryOutputsSchema extends
    DoFn<KafkaRecord<String, GenericRecordWithSchemaId>, TableRow>
    implements Serializable {

  private final Logger LOG;

  public ExtractBasicBigQueryOutputsSchema(Logger loggerInput) {
    LOG = loggerInput;
  }

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

  @ProcessElement
  public void processElement(@Element KafkaRecord<String, GenericRecordWithSchemaId> element,
      OutputReceiver<TableRow> receiver,
      ProcessContext context) {

    KafkaRecordData recordData = KafkaRecordUtil.extractKafkaRecordData(element);

    try {
      Date processingTimestamp =  recordData.processingTime;;
      Date kafkaTimestamp = recordData.creationTimestamp;
      String messageId = recordData.messageId;
      String topicName = element.getTopic();
      int kafkaPartition = recordData.partition;
      long offset = recordData.offset;
      GenericRecord record = recordData.record;

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
