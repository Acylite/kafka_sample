package sample.data.util;


import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sample.data.models.MessageBase;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class PubSubMessageBuilder implements
        Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(PubSubMessageBuilder.class);

    private PubSubMessageBuilder() {}

    public static <T extends MessageBase> PubsubMessage build(final T element) { // Need to make this generic, as it is only used for BetTransactionDetail

        LOG.info("Building PubSub message for " + element);

        try {

            Date timestamp = element.getProcessingTime();

            SimpleDateFormat tsFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
            Map<String, String> attributes = new HashMap<>() {
                {
                    put("messageTimestamp", tsFormat.format(timestamp));
                    put("messageId", element.getMessageId());
                    put("sourceTopic", element.getDomain());
                }
            };

            // Log attributes
            LOG.info("Attributes: " + attributes);

            String payloadStr = String.valueOf(element);

            LOG.info("Payload: " + payloadStr);

            byte[] payload = payloadStr.getBytes();
            return new PubsubMessage(payload,
                    attributes,
                    element.getMessageId());
        } catch (Exception e) {
            String reason = "Could not convert message to PubSub message:\n" + e.getMessage();
            String messageId = element.getMessageId();
            Date timestamp = element.getCreatedTimestamp();
            ProcessingError processingError = new ProcessingError(element.getDomain(), timestamp, messageId, reason, String.valueOf(element));

            LOG.error(processingError.messageId + "::" + processingError.errorReason + "::" + processingError.element);
            return null;
        }
    }
}
