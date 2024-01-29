package it.unipi.dsmt.Models;


import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TripReport {
    public Car car;
    public double consumption;
    public double distance;
}
