package it.unipi.dsmt.Models;


import jdk.jfr.DataAmount;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Collection;

@Data
@AllArgsConstructor
public class Keyed <IN, KEY, ID> {
    private IN wrapped;
    // chiave di partizionamento
    private KEY key;
    // id della regola che ha creato l'istanza di KEYED
    private ID id;
}
