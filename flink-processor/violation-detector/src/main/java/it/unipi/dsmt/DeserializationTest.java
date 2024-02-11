package it.unipi.dsmt;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.unipi.dsmt.models.GeoLocalizationEvent;
import java.io.IOException;

public class DeserializationTest {

    public static void main(String[] args) {
        // Esempio di stringa JSON che rappresenta un messaggio Kafka
        String jsonExample = "{\"car\":{\"vin\":\"V9P32CAWUPTW09023\",\"manufacturer\":\"Aston Martin\",\"model\":\"8C Spider\",\"fuelType\":\"Compressed Natural Gas\",\"color\":\"Red\"},\"lat\":45.5118,\"lon\":9.28647,\"type\":\"MOVE\",\"timestamp\":1707304922788}";

        // Crea un'istanza di ObjectMapper per la deserializzazione
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            // Deserializza la stringa JSON nell'oggetto GeoLocalizationEvent
            GeoLocalizationEvent event = objectMapper.readValue(jsonExample, GeoLocalizationEvent.class);

            // Stampa il risultato per verificare la corretta deserializzazione
            System.out.println("Deserializzazione completata con successo!");
            System.out.println("VIN: " + event.getCar().getVin());
            System.out.println("Latitudine: " + event.getLat());
            System.out.println("Longitudine: " + event.getLon());
            System.out.println("Tipo: " + event.getType());
            System.out.println("Timestamp: " + event.getTimestamp());

        } catch (IOException e) {
            // Gestisce eventuali eccezioni durante la deserializzazione
            System.err.println("Errore durante la deserializzazione: " + e.getMessage());
        }
    }
}
