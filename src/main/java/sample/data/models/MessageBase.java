package sample.data.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import sample.data.util.ProcessingError;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;

abstract public class MessageBase implements Serializable {
    public String messageId;
    public Date _TIMESTAMP;
    public Date _BQ_TIMESTAMP;
    public String messageContent;

    public String domain;
    public String accountId;
    public Date creationTimestamp;

    public Date processingTime;

    public static TableSchema BqTableSchema;

    public Date getCreatedTimestamp()
    {
        return creationTimestamp;
    }

    public abstract Date getProcessingTime();
    public abstract String getMessageId();
    public abstract String getDomain();


    @JsonIgnore
    public Boolean isInLast24Hours() {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -1);
        Date yesterday = cal.getTime();
        return this.creationTimestamp.after(yesterday);
    }
    @JsonIgnore
    public Boolean isInLast24Hours(Date eventTimestamp) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(eventTimestamp);
        cal.add(Calendar.DATE, -1);
        Date yesterday = cal.getTime();
        return this.creationTimestamp.after(yesterday);
    }

    @JsonIgnore
    public Boolean isNotInLast24Hours() {
        return !isInLast24Hours();
    }
    @JsonIgnore
    public Boolean isNotInLast24Hours(Date eventTimestamp) {
        return !isInLast24Hours(eventTimestamp);
    }

    public ProcessingError toProcessingError(String reason) {
        String messageString = this.messageContent;
        String topic = this.domain;
        Date timestamp = this.creationTimestamp;
        String messageId = this.messageId;
        return new ProcessingError(topic, timestamp, messageId, reason, messageString);
    }
    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        // String outputMessage = null;
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }

    public abstract TableRow toTableRow();

}
