package it.unipi.dsmt.models;

import io.jenetics.jpx.Latitude;
import io.jenetics.jpx.Longitude;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class GeoLocalizationEvent {
    @JsonProperty("car")
    public Car car;
    @JsonProperty("lat")
    public Latitude lat;    // Latitude
    @JsonProperty("lon")
    public Longitude lon;   // Longitude
    @JsonProperty("type")
    public Type type;
    @JsonProperty("timestamp")
    public long timestamp;

    public enum Type {
        HEADQUARTER,
        START,
        MOVE,
        END
    }
}