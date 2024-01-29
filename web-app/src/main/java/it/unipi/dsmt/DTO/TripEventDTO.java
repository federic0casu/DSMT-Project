package it.unipi.dsmt.DTO;

import it.unipi.dsmt.models.Car;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor


public class TripEventDTO {


        public Car car;
        public double consumption;
        public double distance;
}
