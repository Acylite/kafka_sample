package sample.data.customKafkaDeserializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.*;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.util.EmptyOnDeserializationThreadLocal;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class CustomNullableAvroCoder extends AvroCoder<GenericRecord>{
    private static final EncoderFactory ENCODER_FACTORY = EncoderFactory.get();
    private static final DecoderFactory DECODER_FACTORY = DecoderFactory.get();
    private final EmptyOnDeserializationThreadLocal<BinaryDecoder> decoder;
    private final EmptyOnDeserializationThreadLocal<BinaryEncoder> encoder;
    private static final ThreadLocal<GenericRecord> recordThreadLocal = new ThreadLocal<>();

    public CustomNullableAvroCoder() {
        super(GenericRecord.class, new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"EmptyRecord\",\"fields\":[]}"));
        this.decoder = new EmptyOnDeserializationThreadLocal<BinaryDecoder>();
        this.encoder = new EmptyOnDeserializationThreadLocal<BinaryEncoder>();
    }

    public void encode(GenericRecord value, OutputStream outStream) throws IOException {
        if (value != null) {
            BinaryEncoder encoderInstance = ENCODER_FACTORY.directBinaryEncoder(outStream, this.encoder.get());
            this.encoder.set(encoderInstance);
            recordThreadLocal.set(value);
            new GenericDatumWriter<GenericRecord>(value.getSchema()).write(value, encoderInstance);
        }
    }

    public GenericRecord decode(InputStream inStream) throws IOException {
        if(inStream.available()==0){
            return null;
        }
        BinaryDecoder decoderInstance = DECODER_FACTORY.directBinaryDecoder(inStream, this.decoder.get());
        this.decoder.set(decoderInstance);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(recordThreadLocal.get().getSchema());
        return datumReader.read(null, decoderInstance);
    }
}
