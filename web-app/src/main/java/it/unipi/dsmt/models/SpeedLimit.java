package it.unipi.dsmt.models;

import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SpeedLimit {
    public int maxSpeed;
    public long wayId;
    public String wayName;
}
