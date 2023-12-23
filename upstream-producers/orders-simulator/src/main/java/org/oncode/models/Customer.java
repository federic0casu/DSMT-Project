package org.oncode.models;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Customer {
    String id;
    String name;
    String email;
    String country;
}
