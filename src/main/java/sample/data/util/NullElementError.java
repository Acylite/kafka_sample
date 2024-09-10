package sample.data.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Date;

public class NullElementError implements Serializable {

    public String topic;
    public Date timestamp;
    public String messageId;
    public String errorReason;
    public String element;
    private static final Logger LOG = LoggerFactory.getLogger("NullElementErrorLogger");

    public NullElementError(String topic, Date timestamp, String id, String reason, String element) {
        this.topic = topic;
        this.timestamp = timestamp;
        this.messageId = id;
        this.errorReason = reason;
        this.element = element;
    }

    public NullElementError(String reason) {
        errorReason = reason;
    }

}


