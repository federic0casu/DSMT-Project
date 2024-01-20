package it.unipi.dsmt.serializers;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

import it.unipi.dsmt.DTO.FraudEventDTO;
import it.unipi.dsmt.DTO.GeoLocalizationDTO;
import jakarta.websocket.EncodeException;
import jakarta.websocket.Encoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FraudEventDTOEncoder implements Encoder.Text<FraudEventDTO> {
    private static final Logger logger = LoggerFactory.getLogger(FraudEventDTO.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    @Override
    public String encode(FraudEventDTO eventDTO) {
        String json = null;
        try {
            json = mapper.writeValueAsString(eventDTO);
        } catch (JsonProcessingException e) {
            logger.error("Error during GeoLocalizationDTO.encode(): " + e.getMessage());
        }
        return json;
    }

}