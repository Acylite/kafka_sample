package sample.data.util;

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.Secret;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SecretHandler {

//    SecretManagerServiceClient secretClient = getSecretClient();
    private static final Logger LOG = LoggerFactory.getLogger(SecretHandler.class);

    public SecretHandler() {
    }
    public byte[] getSecretBytes(String projectId, String secretId) {

        SecretName secretName = SecretName.of(projectId, secretId);

        try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
            Secret secret = client.getSecret(secretName);

            String replication = "AUTOMATIC";
            LOG.info("Secret {}, replication {}\n", secret.getName(), replication);

            AccessSecretVersionResponse response = client.accessSecretVersion(secret.getName() + "/versions/latest"); //TODO add version handling
            return response.getPayload().getData().toByteArray();
        } catch (IOException e) {
            // Handle exception
            throw new RuntimeException("Failed to access secret version", e);
        }
    }

    public String getSecretEnv(String env) {
        return env.equals("staging") ? "live" : env;
    }

    public String getSecretProject(String env) {
        return "ls-data-" + getSecretEnv(env);
    }

    public Map<String, byte[]> getSecretsPayload(String env) {

        Map<String, byte[]> secretsPayload = new HashMap<>();
        String secretEnv = getSecretEnv(env);
        String projectId = getSecretProject(env);

        String keyStoreSecretId = "kafka_client_" + env + "_keystore";
        String trustStoreSecretId = "kafka_client_" + env + "_truststore";

        if (secretEnv.equals("live")) {
            String keyStoreDestination = "/tmp/DWH_VirginBet_227_LIVE.keystore.jks";
            String trustStoreDestination = "/tmp/kafka.client.truststore.jks";

            byte[] keyStoreBytes = getSecretBytes(projectId, keyStoreSecretId);
            byte[] trustStoreBytes = getSecretBytes(projectId, trustStoreSecretId);

            secretsPayload.put(keyStoreDestination, keyStoreBytes);
            secretsPayload.put(trustStoreDestination, trustStoreBytes);

        } else {
            String keyStoreDestination = "/tmp/DWH_Dev.keystore.jks";

            byte[] keyStoreBytes = getSecretBytes(projectId, keyStoreSecretId);
            secretsPayload.put(keyStoreDestination, keyStoreBytes);
        }
        return secretsPayload;
    }

    public void loadSecrets(String env) {

        Map<String, byte[]> secretsPayload = getSecretsPayload(env);

        secretsPayload.forEach((key, value) -> {
            InputStream kafkaFileStreamRead = new ByteArrayInputStream(value);
            try {
                FileHandler.copyFileToLocal(kafkaFileStreamRead, key);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

    }
}
