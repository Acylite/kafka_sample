package sample.data.models.BU;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.api.services.bigquery.model.TableRow;

import sample.data.util.ExtractProperty;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class RDTO implements Serializable {

    public String id;
    public String name;
    public Long openTime;
    public Long closeTime;
    public Long archiveTime;
    public PrizeDTO prize;
    public List<QuestionsDTO> questions;
    public EventDTO event;
    public GameDTO game;

    public RDTO(String record) {
        
        this.id = ExtractProperty.safeCastUtf8String(record, "id");
        this.name = ExtractProperty.safeCastUtf8String(record, "name");
        this.openTime = ExtractProperty.safeCast(record, "openTime", Long.class);
        this.closeTime = ExtractProperty.safeCast(record, "closeTime", Long.class);
        this.archiveTime = ExtractProperty.safeCast(record, "archiveTime", Long.class);

        String prizeRecord = ExtractProperty.safeCastJSONString(record, "prize");
        this.prize = (prizeRecord != null) ? new PrizeDTO(prizeRecord) : null;
        
        
        List<String> questionsRecords = ExtractProperty.safeCastDTOArray(record, "questions");
        this.questions = questionsRecords.stream().map(QuestionsDTO::new).collect(Collectors.toList());
        String eventRecord = ExtractProperty.safeCastJSONString(record, "event");
        this.event = (eventRecord != null) ? new EventDTO(eventRecord) : null;
        String gameRecord = ExtractProperty.safeCastJSONString(record, "game");
        this.game = (gameRecord != null)? new GameDTO(gameRecord) : null;

    }

    public static class PrizeDTO implements Serializable {
        public String value;
        public List<ExceptionsDTO> exceptions;

        public PrizeDTO(String record) {
            this.value = ExtractProperty.safeCastUtf8String(record, "value");
            this.exceptions = ExtractProperty.safeCastDTOArray(record, "exceptions").stream().map(ExceptionsDTO::new).collect(Collectors.toList());
        }
        
        @JsonIgnore
        public TableRow toTableRow() {
            TableRow row = new TableRow();
            row.set("value", value);
            row.set("exceptions", exceptions != null ? exceptions.stream().map(ExceptionsDTO::toTableRow).collect(Collectors.toList()) : "");
            return row;
        }
    }

    public static class ExceptionsDTO implements Serializable {
        public String value;
        public List<String> regionIds;

        public ExceptionsDTO(String record) {
            this.value = ExtractProperty.safeCastUtf8String(record, "value");
            this.regionIds = ExtractProperty.safeCastArray(record, "regionIds", String.class);
        }
        
        @JsonIgnore
        public TableRow toTableRow() {
            TableRow row = new TableRow();
            row.set("value", value);
            row.set("regionIds", regionIds != null ? regionIds : null);
            return row;
        }
    }

    public static class GameDTO implements Serializable {
        public String id;

        public GameDTO(String record) {
            this.id = ExtractProperty.safeCastUtf8String(record, "id");
        }

        @JsonIgnore
        public TableRow toTableRow() {
            TableRow row = new TableRow();
            row.set("ID", id);
            return row;
        
        } 
    }

    public static class EventDTO implements Serializable {
        public String id;
        public String name;
        public Long startTime;
        public CompetitionDTO competition;

        public EventDTO(String record) {
            this.id = ExtractProperty.safeCastUtf8String(record, "id");
            this.name = ExtractProperty.safeCastUtf8String(record, "name");
            this.startTime = ExtractProperty.safeCast(record, "startTime", Long.class);
            String compRecord = ExtractProperty.safeCastJSONString(record, "competition");
            this.competition = (compRecord != null) ? new CompetitionDTO(compRecord) : null;
        }

        @JsonIgnore
        public String epochToDate(Long time) {
            SimpleDateFormat isoFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            if (time != null) {
                Date date = new Date(time);
                return isoFormat.format(date);
            }
            return null;
        }

        @JsonIgnore
        public TableRow toTableRow() {
            TableRow row = new TableRow();
            row.set("ID", id);
            row.set("name", name);
            row.set("startTime", epochToDate(startTime));
            row.set("competition", competition != null ? competition.toTableRow() : null);
            return row;
        }


        public static class CompetitionDTO implements Serializable {
            public String id;
            public String name;

            public CompetitionDTO(String record) {
                this.id = ExtractProperty.safeCastUtf8String(record, "id");
                this.name = ExtractProperty.safeCastUtf8String(record, "name");
            }

            @JsonIgnore
            public TableRow toTableRow() {
                TableRow row = new TableRow();
                row.set("ID", id);
                row.set("name", name);
                return row;
            }
        }
    }

    public static class QuestionsDTO implements Serializable {
        public String id;
        public String marketId;
        public String type;
        public String name;
        public Boolean tieBreaker;
        public List<NoneDTO> none;
        public List<OptionsDTO> options;
        public List<pOptionsDTO> playerOptions;

        public QuestionsDTO(String record) {
            this.id = ExtractProperty.safeCastUtf8String(record, "id");
            this.marketId = ExtractProperty.safeCastUtf8String(record, "marketId");
            this.type = ExtractProperty.safeCastUtf8String(record, "type");
            this.name = ExtractProperty.safeCastUtf8String(record, "name");
            this.tieBreaker = ExtractProperty.safeCast(record, "tieBreaker", Boolean.class);

            List<String> noneRecords = ExtractProperty.safeCastDTOArray(record, "none");
            this.none = noneRecords.stream().map(NoneDTO::new).collect(Collectors.toList());

            List<String> optionsRecords = ExtractProperty.safeCastDTOArray(record, "options");
            this.options = optionsRecords.stream().map(OptionsDTO::new).collect(Collectors.toList());;

            List<String> playerOptionsRecords = ExtractProperty.safeCastDTOArray(record, "playerOptions");
            this.playerOptions = playerOptionsRecords.stream().map(pOptionsDTO::new).collect(Collectors.toList());;
        }

        @JsonIgnore
        public TableRow toTableRow() {
            TableRow row = new TableRow();
            row.set("ID", id);
            row.set("marketID", marketId);
            row.set("type", type);
            row.set("name", name);
            row.set("tieBreaker", tieBreaker);
            row.set("none", none != null ? none.stream().map(NoneDTO::toTableRow).collect(Collectors.toList()) : new ArrayList<>());
            row.set("options", options != null ? options.stream().map(OptionsDTO::toTableRow).collect(Collectors.toList()) : new ArrayList<>());
            row.set("playerOptions", playerOptions != null ? playerOptions.stream().map(pOptionsDTO::toTableRow).collect(Collectors.toList()) : new ArrayList<>());
            return row;
        }
    }

    public static class NoneDTO implements Serializable {
        public String id;
        public String name;
        public Long points;
        public String selectionId;

        public NoneDTO(String record) {
            this.id = ExtractProperty.safeCastUtf8String(record, "id");
            this.name = ExtractProperty.safeCastUtf8String(record, "name");
            this.points = ExtractProperty.safeCast(record, "points", Long.class);
            this.selectionId = ExtractProperty.safeCastUtf8String(record, "selectionId");
        }

        @JsonIgnore
        public TableRow toTableRow() {
            TableRow row = new TableRow();
            row.set("ID", id);
            row.set("name", name);
            row.set("points", points);
            row.set("selectionId", selectionId);
            return row;
        }
    }

    public static class pOptionsDTO implements Serializable {
        public String id;
        public String firstName;
        public String lastName;
        public String nickName;
        public Long number;
        public String position;
        public String participantPos;
        public Long points;
        public String selectionId;

        public pOptionsDTO(String record) {
            this.id = ExtractProperty.safeCastUtf8String(record, "id");
            this.firstName = ExtractProperty.safeCastUtf8String(record, "firstName");
            this.lastName = ExtractProperty.safeCastUtf8String(record, "lastName");
            this.nickName = ExtractProperty.safeCastUtf8String(record, "nickName");
            this.number = ExtractProperty.safeCast(record, "number", Long.class);
            this.position = ExtractProperty.safeCastUtf8String(record, "position");
            this.participantPos = ExtractProperty.safeCastUtf8String(record, "participantPos");
            this.points = ExtractProperty.safeCast(record, "points", Long.class);
            this.selectionId = ExtractProperty.safeCastUtf8String(record, "selectionId");
        }

        @JsonIgnore
        public TableRow toTableRow() {
            TableRow row = new TableRow();
            row.set("ID", id);
            row.set("firstName", firstName);
            row.set("lastName", lastName);
            row.set("nickName", nickName);
            row.set("number", number);
            row.set("position", position);
            row.set("participantPos", participantPos);
            row.set("points", points);
            row.set("selectionId", selectionId);
            return row;
        }
    }

    public static class OptionsDTO implements Serializable {
        public String id;
        public String name;
        public Long points;
        public String selectionId;

        public OptionsDTO(String record) {
            this.id = ExtractProperty.safeCastUtf8String(record, "id");
            this.name = ExtractProperty.safeCastUtf8String(record, "name");
            this.points = ExtractProperty.safeCast(record, "points", Long.class);
            this.selectionId = ExtractProperty.safeCastUtf8String(record, "selectionId");
        }

        @JsonIgnore
        public TableRow toTableRow() {
            TableRow row = new TableRow();
            row.set("ID", id);
            row.set("name", name);
            row.set("points", points);
            row.set("selectionId", selectionId);
            return row;
        }
    }

}
