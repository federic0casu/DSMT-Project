package it.unipi.dsmt.Models;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.jenetics.jpx.Latitude;
import io.jenetics.jpx.Longitude;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class GeoLocalization {
    @JsonProperty("car")
    public Car car;
    @JsonProperty("lat")
    public Latitude lat; // Latitude
    @JsonProperty("lon")
    public Longitude lon; // Longitude
    @JsonProperty("type")
    public Type type;

    public long timestamp;

    public enum Type {
        HEADQUARTER,
        START,
        MOVE,
        END
    }
    public GeoLocalization(Car car, double latitude, double longitude, Type type, Long timestamp) {
        this.car = car;
        this.lat = Latitude.ofDegrees(latitude);
        this.lon = Longitude.ofDegrees(longitude);
        this.type = type;
        this.timestamp = timestamp;
    }
}
