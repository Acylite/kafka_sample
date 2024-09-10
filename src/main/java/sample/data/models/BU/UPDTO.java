package sample.data.models.BU;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.api.services.bigquery.model.TableRow;

import sample.data.util.ExtractProperty;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class UPDTO implements Serializable {
    public String id;
    public String userId;
    public String eventType;
    public String roundId;
    public Long firstAnsweredTime;
    public Long answeredTime;
    public Long settledTime;
    public Long points;
    public String totalPoints;
    public Long leaderboardPosition;
    public List<PredsDTO> predictionResults;

    public UPDTO(String record){
        this.id = ExtractProperty.safeCastUtf8String(record, "id");
        this.userId = ExtractProperty.safeCastUtf8String(record, "userId");
        this.eventType = ExtractProperty.safeCastUtf8String(record, "eventType");
        this.roundId = ExtractProperty.safeCastUtf8String(record, "roundId");
        this.firstAnsweredTime = ExtractProperty.safeCast(record, "firstAnsweredTime", Long.class);
        this.answeredTime = ExtractProperty.safeCast(record, "answeredTime", Long.class);
        this.settledTime = ExtractProperty.safeCast(record, "settledTime", Long.class);
        this.points = ExtractProperty.safeCast(record, "points", Long.class);
        this.totalPoints = ExtractProperty.safeCastUtf8String(record, "totalPoints");
        this.leaderboardPosition = ExtractProperty.safeCast(record, "leaderboardPosition", Long.class);
        List<String> predictionRecord = ExtractProperty.safeCastDTOArray(record, "predictionResults");
        this.predictionResults = predictionRecord.stream().map(PredsDTO::new).collect(Collectors.toList());
        

    }
        
    public static class PredsDTO implements Serializable{
        public String questionId;
        public Long totalPoints;
        public String status;
        public String optionId;


        public PredsDTO(String record){
            this.questionId = ExtractProperty.safeCastUtf8String(record, "id");
            this.totalPoints = ExtractProperty.safeCast(record, "totalPoints", Long.class);
            this.status = ExtractProperty.safeCastUtf8String(record, "status");
            this.optionId = ExtractProperty.safeCastUtf8String(record, "optionId");
        }

        @JsonIgnore
        public TableRow toTableRow() {
            TableRow row = new TableRow();
            row.set("questionId", questionId);
            row.set("totalPoints", totalPoints);
            row.set("status", status);
            row.set("optionId", optionId);

            return row;
        }
    }
    
}