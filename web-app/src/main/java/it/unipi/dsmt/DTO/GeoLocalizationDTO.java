package it.unipi.dsmt.DTO;

import it.unipi.dsmt.models.Car;

import io.jenetics.jpx.Latitude;
import io.jenetics.jpx.Longitude;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class GeoLocalizationDTO {
    public Car car;
    public Latitude lat; // Latitude
    public Longitude lon; // Longitude
    public Type type;
    public enum Type {
        HEADQUARTER,
        START,
        MOVE,
        END
    }
    public GeoLocalizationDTO(Car car, double latitude, double longitude, Type type) {
        this.car = car;
        this.lat = Latitude.ofDegrees(latitude);
        this.lon = Longitude.ofDegrees(longitude);
        this.type = type;
    }
}