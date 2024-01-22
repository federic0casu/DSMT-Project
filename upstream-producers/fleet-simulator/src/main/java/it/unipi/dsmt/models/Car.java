package it.unipi.dsmt.models;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Car {
    public String vin; // Vehicle Identification Number
    public String manufacturer;
    public String model;
    public String fuelType;
    public String color;
}
