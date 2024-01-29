package it.unipi.dsmt.serializers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import it.unipi.dsmt.DTO.TripEventDTO;
import jakarta.websocket.Encoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TripEventDTOEncoder implements Encoder.Text<TripEventDTO> {
    private static final Logger logger = LoggerFactory.getLogger(TripEventDTO.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    @Override
    public String encode(TripEventDTO eventDTO) {
        String json = null;
        try {
            json = mapper.writeValueAsString(eventDTO);
        } catch (JsonProcessingException e) {
            logger.error("Error during TripEventDTO.encode(): " + e.getMessage());
        }
        return json;
    }
}
