package org.cg.phoenix.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.log4j.Logger;


public class AvroDesrializer <V> {

	private static Logger logger = Logger.getLogger(AvroDesrializer.class);
	private Class<V> type;
	
	public AvroDesrializer (Class<V> type){
		this.type = type;		
	}
	
	public V byteToEvent(byte[] data){

		SpecificDatumReader<V> reader = new SpecificDatumReader<V>(
				type);
		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
		V event = null;
		try {
			event = reader.read(null, decoder);
		} catch (IOException e) {
			logger.error("Fail to parse bytes to event" + e);
		}
		return event;

	}


	public byte[] eventToByte(V event){
		SpecificDatumWriter<V> writer = new SpecificDatumWriter<V>(type);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		try {
			writer.write(event, encoder);
			encoder.flush();
			out.close();
		} catch (IOException e) {
			logger.error("Fail to write event to bytes" + e);
		}
		byte[] serializedBytes = out.toByteArray();
		return serializedBytes;

	}

}
