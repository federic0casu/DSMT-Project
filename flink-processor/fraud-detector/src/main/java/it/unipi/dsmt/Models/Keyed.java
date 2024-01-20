package it.unipi.dsmt.Models;

public class Keyed <IN, KEY, ID> {
    private IN wrapped;
    private KEY key;
    private ID id;

    public KEY getKey() {
        return key;
    }

}
