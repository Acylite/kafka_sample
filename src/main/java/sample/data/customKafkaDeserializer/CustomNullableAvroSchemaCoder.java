package sample.data.customKafkaDeserializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.util.EmptyOnDeserializationThreadLocal;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class CustomNullableAvroSchemaCoder extends AvroCoder<GenericRecordWithSchemaId>{
    private static final EncoderFactory ENCODER_FACTORY = EncoderFactory.get();
    private static final DecoderFactory DECODER_FACTORY = DecoderFactory.get();
    private final EmptyOnDeserializationThreadLocal<BinaryDecoder> decoder;
    private final EmptyOnDeserializationThreadLocal<BinaryEncoder> encoder;
    private static final ThreadLocal<GenericRecordWithSchemaId> recordThreadLocal = new ThreadLocal<>();

    public CustomNullableAvroSchemaCoder() {
        super(GenericRecordWithSchemaId.class, new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"EmptyRecord\",\"fields\":[]}"));
        this.decoder = new EmptyOnDeserializationThreadLocal<BinaryDecoder>();
        this.encoder = new EmptyOnDeserializationThreadLocal<BinaryEncoder>();
    }

    public void encode(GenericRecordWithSchemaId value, OutputStream outStream) throws IOException {
        if (value != null) {
            // Example: Prepend the schema ID (as an int) to the output
            int schemaId = value.getSchemaId();
            ByteBuffer buffer = ByteBuffer.allocate(4); // Integers are 4 bytes
            buffer.putInt(schemaId);
            outStream.write(buffer.array()); // Write the schema ID bytes to the stream

            // Proceed with serializing the GenericRecord part
            GenericRecord avroRecord = value.getRecord();
            BinaryEncoder encoderInstance = ENCODER_FACTORY.directBinaryEncoder(outStream, null);
            this.encoder.set(encoderInstance);
            recordThreadLocal.set(value);
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(avroRecord.getSchema());
            writer.write(avroRecord, encoderInstance);
        }
    }


    public GenericRecordWithSchemaId decode(InputStream inStream) throws IOException {
        if (inStream.available() > 0) {
            // Read the prepended schema ID (assuming it's an int)
            byte[] idBytes = new byte[4]; // Integers are 4 bytes
            inStream.read(idBytes); // Read the schema ID bytes from the stream
            ByteBuffer wrapped = ByteBuffer.wrap(idBytes);
            int schemaId = wrapped.getInt();

            // Now, proceed with deserializing the GenericRecord part
            BinaryDecoder decoderInstance = DECODER_FACTORY.directBinaryDecoder(inStream, null);
            // You will need the schema for the GenericRecord here, possibly fetched using the schemaId
            DatumReader<GenericRecord> reader = new GenericDatumReader<>(recordThreadLocal.get().getRecord().getSchema());
            GenericRecord avroRecord = reader.read(null, decoderInstance);

            // Construct and return your GenericRecordWithSchemaId object
            return new GenericRecordWithSchemaId(avroRecord, schemaId);
        }
        return null;
    }


//    public GenericRecordWithSchemaId decode(InputStream inStream) throws IOException {
//        if(inStream.available()==0){
//            return null;
//        }
//        BinaryDecoder decoderInstance = DECODER_FACTORY.directBinaryDecoder(inStream, this.decoder.get());
//        this.decoder.set(decoderInstance);
//        DatumReader<GenericRecordWithSchemaId> datumReader = new GenericDatumReader<>(recordThreadLocal.get().getRecord().getSchema());
//        return datumReader.read(null, decoderInstance);
//    }
}
