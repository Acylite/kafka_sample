package sample.data.util;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Date;


public class ProcessingError implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger("ProcessingErrorLogger");

  public ProcessingError(String topic, Date timestamp, String id, String reason, String element) {
    this.topic = topic;
    this.timestamp = timestamp;
    this.messageId = id;
    this.errorReason = reason;
    this.element = element;
  }

  public ProcessingError(KafkaRecord<String, GenericRecord> elementInput, String reason) {
    topic = elementInput.getTopic();
    timestamp = new Date(elementInput.getTimestamp());
    messageId = Long.toString(elementInput.getOffset());
    errorReason = reason;
    element = elementInput.getKV().getValue().toString();
  }

  public String topic;
  public Date timestamp;
  public String messageId;
  public String errorReason;
  public String element;

}


