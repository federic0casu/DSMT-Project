package it.unipi.dsmt.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SpeedLimit {
    public int maxSpeed;
    public long wayId;
    public String wayName;
}
