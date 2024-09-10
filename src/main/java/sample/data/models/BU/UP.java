package sample.data.models.BU;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;

import sample.data.models.MessageBase;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.ArrayList;

public class UP extends MessageBase {
    public static TableSchema SchemaFields = new TableSchema().setFields(
            ImmutableList.of(
                    new TableFieldSchema().setName("messageId").setType("STRING"),
                    new TableFieldSchema().setName("kafkaTimestamp").setType("TIMESTAMP"),
                    new TableFieldSchema().setName("processingTimestamp").setType("TIMESTAMP"),
                    new TableFieldSchema().setName("offset").setType("INTEGER"),
                    new TableFieldSchema().setName("ID").setType("STRING"),
                    new TableFieldSchema().setName("userID").setType("INTEGER"),
                    new TableFieldSchema().setName("eventType").setType("STRING"),
                    new TableFieldSchema().setName("firstAnsweredTime").setType("DATETIME"),
                    new TableFieldSchema().setName("answeredDateTime").setType("DATETIME"),
                    new TableFieldSchema().setName("settledDateTime").setType("DATETIME"),
                    new TableFieldSchema().setName("totalPoints").setType("STRING"),
                    new TableFieldSchema().setName("leaderboardPosition").setType("INTEGER"),
                    new TableFieldSchema().setName("roundId").setType("STRING"),
                    new TableFieldSchema().setName("predictions").setType("RECORD").setMode("REPEATED")
        )
    );

    // @JsonIgnore for some of these?
    public String creationDateTime;
    public Date processingTime;
    public Date creationTimestamp;
    public String messageId;
    public long offset;
    public Integer schemaId;
    public String ID;
    public Long userID;
    public String eventType;
    public String roundID;
    public String firstAnsweredTime;
    public String answeredDateTime;
    public String settledDateTime;
    public String totalPoints;
    public Long leaderboardPosition;
    public String roundId;
    public List<UPDTO.PredsDTO> predictions;

    public UP(KafkaRecord<String, String> element, String env) throws ParseException { 
    
        long currentTimestamp = System.currentTimeMillis();
        
        // kafka message data
        messageId = element.getKV().getKey();
        processingTime = new Date(currentTimestamp);
        creationTimestamp = new Date(element.getTimestamp());
        offset = element.getOffset();

        // create record
        String record = element.getKV().getValue();
        UPDTO predsObject;
      
        predsObject = new UPDTO(record);
      
        ID = predsObject.id;
        userID = predsObject.userId == null ? null : Long.parseLong(predsObject.userId);
        eventType = predsObject.eventType;
        roundID = predsObject.roundId;
        Long firstAnsweredEpoch = predsObject.firstAnsweredTime;
        Long answeredDateEpoch = predsObject.answeredTime;
        SimpleDateFormat isoformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        if(firstAnsweredEpoch != null){
            Date firstAnsweredTimeDate = new Date(firstAnsweredEpoch);
            firstAnsweredTime = isoformat.format(firstAnsweredTimeDate);
        } else {
            firstAnsweredTime = null;
        }

        if(answeredDateEpoch != null){
            Date answeredDateTimeDate = new Date(answeredDateEpoch);
            answeredDateTime = isoformat.format(answeredDateTimeDate);
        } else {
            answeredDateTime = null;
        }
        Long settledEpoch = predsObject.settledTime;
        if(settledEpoch != null){
            Date settledDate = new Date(settledEpoch);
            settledDateTime = isoformat.format(settledDate);
        } else {
            settledDateTime = null;
        }
       
        totalPoints = predsObject.totalPoints;
        leaderboardPosition = predsObject.leaderboardPosition;
       
        List<UPDTO.PredsDTO> predsList = predsObject.predictionResults;
     
        predictions = predsList;
    }

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

    @JsonIgnore
    public TableRow toTableRow() {
        TableRow row = new TableRow();
        row.set("messageId", messageId);
        row.set("kafkaTimestamp", creationTimestamp);
        row.set("processingTimestamp", processingTime);
        row.set("offset", offset);
        row.set("ID", ID);
        row.set("userID", userID);
        row.set("eventType", eventType);
        row.set("firstAnsweredTime", firstAnsweredTime);
        row.set("answeredDateTime", answeredDateTime);
        row.set("settledDateTime", settledDateTime);
        row.set("totalPoints", totalPoints);
        row.set("leaderboardPosition", leaderboardPosition);
        row.set("roundId", roundID);
        row.set("predictions", predictions != null ? predictions.stream().map(UPDTO.PredsDTO::toTableRow).collect(Collectors.toList()) : new ArrayList<>());

        return row;
    }
}
