package sample.data.util;

import org.apache.beam.sdk.io.gcp.firestore.FirestoreIO;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.WriteSuccessSummary;
import com.google.firestore.v1.Write;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WriteToFirestore extends PTransform<PCollection<Write>, PCollection<WriteSuccessSummary>> {

    private static final Logger LOG = LoggerFactory.getLogger(WriteToFirestore.class);

    public WriteToFirestore() {}

    @Override
    public PCollection<WriteSuccessSummary> expand(PCollection<Write> doc) {
        
        LOG.info("Writing to firestore...");
        
        PCollection<WriteSuccessSummary> firestoreRecords = doc
                // .apply(Create.of(doc))
                .apply("Write user details to Firestore",
                    FirestoreIO.v1()
                    .write()
                    .batchWrite()
                    .build());
        
        return firestoreRecords;
    }
}
