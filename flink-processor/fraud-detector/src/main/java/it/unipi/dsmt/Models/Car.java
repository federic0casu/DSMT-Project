package it.unipi.dsmt.Models;

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
    public String vin;

    @JsonProperty("manufacturer")
    public String manufacturer;

    @JsonProperty("model")
    public String model;

    @JsonProperty("fuelType")
    public String fuelType;

    @JsonProperty("color")
    public String color;
}

