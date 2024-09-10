package sample.data.util;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.List;

public class AvroSchemaRegistry {

    public static Schema getSchemaAvro(String schemaRegistryUrl, String subject) throws RestClientException, IOException {
        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);
        return new Schema.Parser().parse(schemaRegistryClient.getLatestSchemaMetadata(subject).getSchema());
    }

    public static void listAllSchemas(String schemaRegistryUrl) throws RestClientException, IOException {

        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);

        List<String> subjects = (List<String>) schemaRegistryClient.getAllSubjects();
        for (String subject : subjects) {
            // Fetch the latest schema for each subject
            Schema latestSchema = new Schema.Parser().parse(schemaRegistryClient.getLatestSchemaMetadata(subject).getSchema());
            System.out.println("Subject: " + subject);
            System.out.println("Latest Schema: " + latestSchema.toString(true));
        }
    }
}
