package it.unipi.dsmt.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Violation {
    public long violationTs;    // Timestamp dell'evento
    public String vin;          // Vehicle Identification Number

    // da SpeedingViolation
    public int userSpeed;
    public SpeedLimit speedLimit;

    // da InactivityViolation
    public long tsLastActivity;

    public Violation(String vin, long violationTs){
        this.vin = vin;
        this.violationTs = violationTs;
    }
    public Violation(String vin, long violationTs, int userSpeed, SpeedLimit speedLimit){
        this.vin = vin;
        this.violationTs = violationTs;
        this.userSpeed = userSpeed;
        this.speedLimit = speedLimit;
    }
    public Violation(String vin, long violationTs, long tsLastActivity){
        this.vin = vin;
        this.violationTs = violationTs;
        this.tsLastActivity = tsLastActivity;
    }
}
