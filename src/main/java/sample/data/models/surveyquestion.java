package sample.data.models;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;

import sample.data.util.ExtractProperty;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import java.util.List;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.Date;

public class surveyquestion extends MessageBase {
    
    public String title;
    public List<OptionsDTO> options;
    public String creationDateTime;
    public Date processingTime;
    public Date creationTimestamp;
    public String messageId;
    public long offset;

    public static TableSchema BqTableSchema = new TableSchema().setFields(
        ImmutableList.of(
            new TableFieldSchema().setName("messageId").setType("STRING").setMode("NULLABLE"),
            new TableFieldSchema().setName("creationTimestamp").setType("TIMESTAMP").setMode("NULLABLE"),
            new TableFieldSchema().setName("processingTime").setType("TIMESTAMP").setMode("NULLABLE"),
            new TableFieldSchema().setName("offset").setType("INTEGER").setMode("NULLABLE"),
            new TableFieldSchema().setName("title").setType("STRING").setMode("NULLABLE"),
            new TableFieldSchema().setName("options").setType("RECORD").setMode("REPEATED"),
            new TableFieldSchema().setName("creationDateTime").setType("DATETIME").setMode("NULLABLE")
        )
    );

    public class OptionTuple<A, B> {
        private final A first;
        private final B second;
    
        public OptionTuple(A first, B second) {
            this.first = first;
            this.second = second;
        }
    
        public A getOptionId() {
            return first;
        }
    
        public B getOption() {
            return second;
        }
    }
    
    // Constructor of OneQuestionSurveyQuestion class
    @JsonIgnore
    public surveyquestion(KafkaRecord<String, String> element, String env) {
        String record = element.getKV().getValue();
        long currentTimestamp = System.currentTimeMillis();
        processingTime = new Date(currentTimestamp);
        creationTimestamp = new Date(element.getTimestamp());
        messageId = element.getKV().getKey();
        creationDateTime = ExtractProperty.safeCastUtf8String(record, "created_at");
        title = ExtractProperty.safeCastUtf8String(record, "title");
        List<String> option_extract = ExtractProperty.safeCastUtf8StringList(record, "options");
        ArrayList<OptionTuple<Integer,String>> option_list = new ArrayList<>();
        for(int i = 0; i < option_extract.size(); i++){
            Integer option_id = i+1;
            String option = option_extract.get(i);
            OptionTuple<Integer, String> option_tuple = new OptionTuple<>(option_id, option);
            option_list.add(option_tuple);
        }
        options = option_list != null ? new ArrayList<OptionsDTO>(option_list.stream().map(OptionsDTO::new).collect(Collectors.toCollection(ArrayList::new))) : null;

        offset = element.getOffset();
    }

    @Override
    @JsonIgnore
    public TableRow toTableRow() {
        TableRow row = new TableRow();
        row.set("messageId", messageId);
        row.set("creationTimestamp", creationTimestamp);
        row.set("processingTime", processingTime);
        row.set("offset", offset);
        row.set("title", title);
        row.set("options", options != null ? options.stream().map(OptionsDTO::toTableRow).collect(Collectors.toList()) : null);
        row.set("creationDateTime", creationDateTime);

        return row;
    }

    
    @JsonIgnore
    @Override
    public Date getProcessingTime() {
        return processingTime;
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

