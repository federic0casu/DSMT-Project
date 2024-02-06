package it.unipi.dsmt.serializers;

import it.unipi.dsmt.models.Violation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

import jakarta.websocket.EncodeException;
import jakarta.websocket.Encoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ViolationEncoder implements Encoder.Text<Violation> {
    private static final Logger logger = LoggerFactory.getLogger(ViolationEncoder.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public String encode(Violation violation) throws EncodeException {
        try {
            return mapper.writeValueAsString(violation);
        } catch (JsonProcessingException e) {
            logger.error("Error during encoding of Violation: " + e.getMessage());
            throw new EncodeException(violation, "Json processing error in ViolationEncoder", e);
        }
    }
}
