package it.unipi.dsmt.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Car {
    public String vin; // Vehicle Identification Number
    public String manufacturer;
    public String model;
    public String fuelType;
    public String color;
}

