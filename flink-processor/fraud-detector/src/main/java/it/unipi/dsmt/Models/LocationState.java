package it.unipi.dsmt.Models;

// Class that is used to mantain state about order coming from different locations
// in a short period of time

public class LocationState {
    public String lastOrderLocation;
    public long lastOrderTimestamp;
}
