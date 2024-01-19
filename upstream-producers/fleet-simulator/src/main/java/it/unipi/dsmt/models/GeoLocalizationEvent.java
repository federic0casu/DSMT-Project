package it.unipi.dsmt.models;

import io.jenetics.jpx.Latitude;
import io.jenetics.jpx.Longitude;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class GeoLocalizationEvent {
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
}