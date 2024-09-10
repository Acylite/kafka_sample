package sample.data.models.BU;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;

import sample.data.models.MessageBase;
import sample.data.models.BU.RDTO.QuestionsDTO;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class R extends MessageBase {
    public static TableSchema SchemaFields = new TableSchema().setFields(
            ImmutableList.of(
                    new TableFieldSchema().setName("messageId").setType("STRING"),
                    new TableFieldSchema().setName("kafkaTimestamp").setType("TIMESTAMP"),
                    new TableFieldSchema().setName("processingTimestamp").setType("TIMESTAMP"),
                    new TableFieldSchema().setName("offset").setType("INTEGER"),
                    new TableFieldSchema().setName("ID").setType("STRING"),
                    new TableFieldSchema().setName("name").setType("STRING"),
                    new TableFieldSchema().setName("openDateTime").setType("DATETIME"),
                    new TableFieldSchema().setName("closeDateTime").setType("DATETIME"),
                    new TableFieldSchema().setName("archiveDateTime").setType("DATETIME"),
                    new TableFieldSchema().setName("prize").setType("RECORD").setMode("NULLABLE"),
                    new TableFieldSchema().setName("questions").setType("RECORD").setMode("REPEATED"),
                    new TableFieldSchema().setName("event").setType("RECORD").setMode("NULLABLE"),
                    new TableFieldSchema().setName("game").setType("RECORD").setMode("NULLABLE")
        )
    );

  
    public String messageId;
    public Date kafkaTimestamp;
    public Date processingTimestamp;
    public long offset;
    public String ID;
    public String name;
    public String openDateTime;
    public String closeDateTime;
    public String archiveDateTime;
    public RDTO.PrizeDTO prize;
    public List<RDTO.QuestionsDTO> questions;
    public RDTO.EventDTO event;
    public RDTO.GameDTO game;

    public R(KafkaRecord<String, String> element, String env) throws ParseException { 
    
        long currentTimestamp = System.currentTimeMillis();
        
        // kafka message data
        messageId = element.getKV().getKey();
        processingTime = new Date(currentTimestamp);
        creationTimestamp = new Date(element.getTimestamp());
        offset = element.getOffset();

        // create record
        String record = element.getKV().getValue();
        
        RDTO roundsObject = new RDTO(record);
       
        
        ID = roundsObject.id;
        name = roundsObject.name;
        Long openEpoch = roundsObject.openTime;
        Long closeEpoch = roundsObject.closeTime;
        Long archiveEpoch = roundsObject.archiveTime;
        SimpleDateFormat isoformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        if(openEpoch != null){
            Date openDateTimeDate = new Date(openEpoch);
            openDateTime = isoformat.format(openDateTimeDate);
        } else {
            openDateTime = null;
        }
        if(closeEpoch != null){
            Date closeDateTimeDate = new Date(closeEpoch);
            closeDateTime = isoformat.format(closeDateTimeDate);
        } else {
            closeDateTime = null;
        }
        if(archiveEpoch != null){
            Date archiveDateTimeDate = new Date(archiveEpoch);
            archiveDateTime = isoformat.format(archiveDateTimeDate);
        } else {
            archiveDateTime = null;
        }
        prize = roundsObject.prize;
            
        questions = roundsObject.questions;
        
        event = roundsObject.event;

        game = roundsObject.game;
        
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
        row.set("name", name);
        row.set("openDateTime", openDateTime);
        row.set("closeDateTime", closeDateTime);
        row.set("archiveDateTime", archiveDateTime);
        row.set("prize", prize != null ? prize.toTableRow() : null);
        row.set("questions", questions != null ? questions.stream().map(QuestionsDTO::toTableRow).collect(Collectors.toList()) : new ArrayList<>());
        row.set("event", event != null ? event.toTableRow() : null);
        row.set("game", game != null ? game.toTableRow() : null);

        return row;
    }
}
