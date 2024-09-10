package sample.data.util;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecord;

import sample.data.customKafkaDeserializer.GenericRecordWithSchemaId;

import java.util.Date;

public class KafkaRecordUtil {
    public static KafkaRecordData extractKafkaRecordData(KafkaRecord<String, GenericRecordWithSchemaId> element) {
        long currentTimestamp = System.currentTimeMillis();
        Date processingTime = new Date(currentTimestamp);
        Date creationTimestamp = new Date(element.getTimestamp());
        String messageId = element.getKV().getKey();
        int partition = element.getPartition();
        long offset = element.getOffset();

        GenericRecordWithSchemaId recordWithId = element.getKV().getValue();
        GenericRecord record = null;
        int schemaId = 0;

        if (recordWithId != null) {
            record = (GenericRecord) recordWithId.getRecord();
            schemaId = recordWithId.getSchemaId();
        } else {
            System.out.println("Warning: recordWithId is null.");
        }


        return new KafkaRecordData(processingTime, creationTimestamp, messageId, partition, offset, schemaId, record);
    }
}