package it.unipi.dsmt.Models;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TripState {
    public Car car;
    public GeoLocalization lastPoint = null;
    public double currentDistance = (float) 0;
    public double currentConsumption = (float) 0;
}
