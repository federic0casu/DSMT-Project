package it.unipi.dsmt.serializers;

import it.unipi.dsmt.DTO.GeoLocalizationDTO;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

import jakarta.websocket.Encoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GeoLocalizationDTOEncoder implements Encoder.Text<GeoLocalizationDTO> {
    private static final Logger logger = LoggerFactory.getLogger(GeoLocalizationDTO.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    @Override
    public String encode(GeoLocalizationDTO eventDTO) {
        String json = null;
        try {
            json = mapper.writeValueAsString(eventDTO);
        } catch (JsonProcessingException e) {
            logger.error("Error during GeoLocalizationDTO.encode(): " + e.getMessage());
        }
        return json;
    }
}
