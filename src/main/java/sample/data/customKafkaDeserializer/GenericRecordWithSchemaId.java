package sample.data.customKafkaDeserializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public class GenericRecordWithSchemaId implements GenericRecord {
    private final GenericRecord record;
    private final int schemaId;

    public GenericRecordWithSchemaId(GenericRecord record, int schemaId) {
        this.record = record;
        this.schemaId = schemaId;
    }

    public GenericRecord getRecord() {
        return record;
    }

    public int getSchemaId() {
        return schemaId;
    }

    @Override
    public void put(String key, Object v) {

    }

    @Override
    public Object get(String key) {
        return null;
    }

    @Override
    public void put(int i, Object v) {

    }

    @Override
    public Object get(int i) {
        return null;
    }

    @Override
    public Schema getSchema() {
        return null;
    }

    // Additional methods as needed
}
