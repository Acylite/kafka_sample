package sample.data.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;

import sample.data.util.ExtractProperty;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import java.util.List;
import java.util.Date;
import java.lang.Boolean;


public class surveyvote extends MessageBase {
     
    public static TableSchema BqTableSchema = new TableSchema().setFields(
        ImmutableList.of(
            new TableFieldSchema().setName("location").setType("STRING").setMode("NULLABLE"),
            new TableFieldSchema().setName("platform").setType("STRING").setMode("NULLABLE"),
            new TableFieldSchema().setName("language").setType("STRING").setMode("NULLABLE"),
            new TableFieldSchema().setName("offset").setType("INTEGER").setMode("NULLABLE"),
            new TableFieldSchema().setName("anonymous_id").setType("STRING").setMode("NULLABLE"),
            new TableFieldSchema().setName("permutive_id").setType("STRING").setMode("NULLABLE"),
            new TableFieldSchema().setName("lithium_id").setType("STRING").setMode("NULLABLE"),
            new TableFieldSchema().setName("device_id").setType("STRING").setMode("NULLABLE"),
            new TableFieldSchema().setName("survey_id").setType("STRING").setMode("NULLABLE"),
            new TableFieldSchema().setName("option_id").setType("INTEGER").setMode("REPEATED"),
            new TableFieldSchema().setName("api_version").setType("STRING").setMode("NULLABLE"),
            new TableFieldSchema().setName("logged_in").setType("BOOLEAN").setMode("NULLABLE"),
            new TableFieldSchema().setName("is_new_user").setType("BOOLEAN").setMode("NULLABLE"),
            new TableFieldSchema().setName("creationTimestamp").setType("TIMESTAMP").setMode("NULLABLE"),
            new TableFieldSchema().setName("processingTime").setType("TIMESTAMP").setMode("NULLABLE"),
            new TableFieldSchema().setName("creationDateTime").setType("DATETIME").setMode("NULLABLE"),
            new TableFieldSchema().setName("messageId").setType("STRING").setMode("NULLABLE")
        )
    );

    // @JsonIgnore for some of these?
    public String location;
    public String messageId;
    public String platform;
    public String language;
    public String anonymous_id;
    public String permutive_id;
    public String lithium_id;
    public String device_id;
    public String survey_id;
    public List<Integer> option_id;
    public String api_version;
    public Boolean logged_in;
    public Boolean is_new_user;
    public Date creationTimestamp;
    public Date processingTime;
    public String creationDateTime;
    public String collectionId;
    public int schemaId;
    public long offset;


    // Constructor of OneQuestionSurvey class
    @JsonIgnore
    public surveyvote(KafkaRecord<String, String> element, String env) {

        // GenericRecord record = element.getKV().getValue();

        String record = element.getKV().getValue();
    
        // schemaId = recordWithId.getSchemaId();
        creationTimestamp = new Date(element.getTimestamp());
        messageId = element.getKV().getKey();
        long currentTimestamp = System.currentTimeMillis();
        processingTime = new Date(currentTimestamp);
        location = ExtractProperty.safeCastUtf8String(record, "location");
        platform = ExtractProperty.safeCastUtf8String(record, "platform");
        language = ExtractProperty.safeCastUtf8String(record, "language");
        anonymous_id = ExtractProperty.safeCastUtf8String(record, "anonymous_id");
        permutive_id = ExtractProperty.safeCastUtf8String(record, "permutive_id"); 
        lithium_id = ExtractProperty.safeCastUtf8String(record, "lithium_id");
        device_id = ExtractProperty.safeCastUtf8String(record, "device_id");
        survey_id = ExtractProperty.safeCastUtf8String(record, "survey_id");
        option_id = ExtractProperty.safeCastUtf8IntList(record, "option_id");
        creationDateTime = ExtractProperty.safeCastUtf8String(record, "created_at");
        api_version = ExtractProperty.safeCastUtf8String(record, "api_version");
        logged_in = ExtractProperty.safeCast(record, "logged_in", Boolean.class);
        is_new_user = ExtractProperty.safeCast(record, "is_new_user", Boolean.class);
        offset = element.getOffset();
    }

    @Override
    @JsonIgnore
    public TableRow toTableRow() {
        TableRow row = new TableRow();
        row.set("location", location);
        row.set("platform", platform);
        row.set("language", language);
        row.set("anonymous_id", anonymous_id);
        row.set("permutive_id", permutive_id);
        row.set("lithium_id", lithium_id);
        row.set("device_id", device_id);
        row.set("survey_id", survey_id);
        row.set("option_id", option_id);
        row.set("api_version", api_version);
        row.set("logged_in", logged_in);
        row.set("is_new_user", is_new_user);
        row.set("creationTimestamp", creationTimestamp);
        row.set("processingTime", processingTime);
        row.set("creationDateTime", creationDateTime);
        row.set("offset", offset);
        row.set("messageId", messageId);

        return row;
    }

    @JsonIgnore
    @Override
    public Date getProcessingTime() {
        return creationTimestamp;
    }

    @JsonIgnore
    @Override
    public String getMessageId() {
        return messageId;
    }

    @JsonIgnore
    @Override
    public String getDomain() {
        return domain;
    }
}

