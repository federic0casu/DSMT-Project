package it.unipi.dsmt.models;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Car {
    @JsonProperty("vin")
    private String vin;      // Vehicle Identification Number
    @JsonProperty("manufacturer")
    private String manufacturer;
    @JsonProperty("model")
    private String model;
    @JsonProperty("fuelType")
    private String fuelType;
    @JsonProperty("color")
    private String color;
}

