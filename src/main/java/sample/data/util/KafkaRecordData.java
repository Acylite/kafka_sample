package sample.data.util;

import org.apache.avro.generic.GenericRecord;

import java.util.Date;

public class KafkaRecordData {
    public Date processingTime;
    public Date creationTimestamp;
    public String messageId;
    public int partition;
    public long offset;
    public int schemaId;
    //public String domain;
    public GenericRecord record;

    public KafkaRecordData(Date processingTime, Date creationTimestamp, String messageId, int partition, long offset,
                           int schemaId, GenericRecord record) {
        this.processingTime = processingTime;
        this.creationTimestamp = creationTimestamp;
        this.messageId = messageId;
        this.partition = partition;
        this.offset = offset;
        this.schemaId = schemaId;
        this.record = record;
    }
}