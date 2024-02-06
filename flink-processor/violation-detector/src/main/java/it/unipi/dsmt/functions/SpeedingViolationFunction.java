package it.unipi.dsmt.functions;

import io.jenetics.jpx.Latitude;
import io.jenetics.jpx.Longitude;
import it.unipi.dsmt.models.Violation;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import it.unipi.dsmt.models.SpeedLimit;
import it.unipi.dsmt.models.GeoLocalizationEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedList;
import org.apache.commons.lang3.tuple.Pair;

public class SpeedingViolationFunction extends KeyedProcessFunction<String, GeoLocalizationEvent, Violation> {
    private static final Logger LOG = LoggerFactory.getLogger(SpeedingViolationFunction.class);
    private transient ValueState< Pair<LinkedList<GeoLocalizationEvent>, SpeedLimit> > state;
    // LinkedList<GeoLocalizationEvent> : last two events
    // SpeedLimit : last speed limit

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>(
                "mySpeedLimitState", TypeInformation.of(new TypeHint< Pair<LinkedList<GeoLocalizationEvent>, SpeedLimit> >() {})
        ));
    }

    @Override
    public void processElement(GeoLocalizationEvent event,
                               Context ctx,
                               Collector<Violation> out) throws Exception {

        // Se l'evento NON è di tipo MOVE, non viene fatto nulla
        if (event.getType() != GeoLocalizationEvent.Type.MOVE) {
            return;
        }

        Pair<LinkedList<GeoLocalizationEvent>, SpeedLimit> stateValue = state.value();

        LinkedList<GeoLocalizationEvent> lastTwoEvents;
        SpeedLimit lastSpeedingViolation;

        if (stateValue != null) {
            lastTwoEvents = stateValue.getLeft();
            lastSpeedingViolation = stateValue.getRight();
        }
        else {
            // Se lo stato è null, inizializzo lastTwoEvents e lastSpeedLimit con valori appropriati
            lastTwoEvents = new LinkedList<>();
            lastSpeedingViolation = null;
        }

        lastTwoEvents.add(event);           // aggiungo l'evento corrente in coda alla lista
        if (lastTwoEvents.size() > 2) {
            lastTwoEvents.removeFirst();
        }

        if(lastTwoEvents.size() == 1) {
            state.update(Pair.of(lastTwoEvents, null));
            return;
        }
        if (lastTwoEvents.size() == 2) {
            // ora ho in lastTwoEvents gli ultimi due eventi:
            // - lastTwoEvents[0] è il penultimo evento
            // - lastTwoEvents[1] è l'ultimo evento

            double distance = calculateDistance_Geotools(lastTwoEvents.get(0).getLat(),
                                                        lastTwoEvents.get(0).getLon(),
                                                        lastTwoEvents.get(1).getLat(),
                                                        lastTwoEvents.get(1).getLon());   // in km
            double time = (double)(lastTwoEvents.get(1).getTimestamp() - lastTwoEvents.get(0).getTimestamp()) / 1000;   // in seconds
            int speed = (time != 0) ? (int)((distance / time) * 3600) : 0;  // Converto da km/s a km/h

            SpeedLimit currentSpeedLimit = getSpeedLimit_OpenStreet(
                    lastTwoEvents.get(0).getLat().doubleValue(),
                    lastTwoEvents.get(0).getLon().doubleValue());

            LOG.info("last_event: timestamp {} - coordinates: {} , {}", lastTwoEvents.get(0).getTimestamp(), lastTwoEvents.get(0).getLat().doubleValue(), lastTwoEvents.get(0).getLon().doubleValue());
            LOG.info("current_event: timestamp {} - coordinates: {} , {}", lastTwoEvents.get(1).getTimestamp(), lastTwoEvents.get(1).getLat().doubleValue(), lastTwoEvents.get(1).getLon().doubleValue());
            LOG.info("distance: {} - time: {} -> speed: {}, currentSpeedLimit: {}", distance, time, speed, currentSpeedLimit);

            if (currentSpeedLimit != null){
                // se sono riuscito a recuperare il limite di velocità della strada che si sta percorrendo

                if (speed > currentSpeedLimit.getMaxSpeed() && speed>0 && speed<251){
                    // se c'è stata una violazione di velocità

                    if(lastSpeedingViolation == null || !currentSpeedLimit.getWayName().equals(lastSpeedingViolation.getWayName())){
                        // se non è già stata segnalata una violazione per la stessa strada

                        state.update(Pair.of(lastTwoEvents, currentSpeedLimit));
                        out.collect(new Violation(event.getCar().getVin(), lastTwoEvents.get(1).getTimestamp(), speed, currentSpeedLimit));
                    }
                    else {
                        // se è già stata segnalata una violazione per la stessa strada
                        // (lastSpeedingViolation è sicuramente != null)
                        state.update(Pair.of(lastTwoEvents, lastSpeedingViolation));
                    }

                }
                else {
                    // se NON c'è stata una violazione di velocità
                    state.update(Pair.of(lastTwoEvents, null));
                }
            }
            else{
                // NON sono riuscito a recuperare il limite di velocità della strada che si sta percorrendo
                // di conseguenza NON so se c'è stata una violazione di velocità, quindi quello che faccio è
                // lasciare invariato lo stato
                state.update(Pair.of(lastTwoEvents, lastSpeedingViolation));
            }
        }
    }

//    // HAVERSINE FORMULA TO CALCULATE DISTANCE BETWEEN TWO POSITIONS
//    // https://en.wikipedia.org/wiki/Haversine_formula
//    // Better to use some library with more precision, but good for now
//    double calculateDistance(double startLat, double startLong, double endLat, double endLong) {
//        int EARTH_RADIUS = 6371;
//        double dLat = Math.toRadians((endLat - startLat));
//        double dLong = Math.toRadians((endLong - startLong));
//
//        startLat = Math.toRadians(startLat);
//        endLat = Math.toRadians(endLat);
//
//        double a = haversine(dLat) + Math.cos(startLat) * Math.cos(endLat) * haversine(dLong);
//        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
//
//        return EARTH_RADIUS * c;
//    }
//
//    double haversine(double val) {
//        return Math.pow(Math.sin(val / 2), 2);
//    }

    /**
     * Calcola la distanza approssimativa in chilometri tra due punti geografici.
     *
     * Questa funzione utilizza la libreria Geotools per creare geometrie di punti
     * dai dati di latitudine e longitudine e calcolare la distanza tra questi punti
     * in gradi. La distanza in gradi viene poi convertita in chilometri.
     *
     * La conversione assume che un grado sulla superficie della Terra sia
     * approssimativamente uguale a 111,32 chilometri. Questa approssimazione
     * funziona bene per brevi distanze, ma può essere meno precisa per distanze
     * più lunghe o quando è richiesta un'alta precisione.
     *
     * @param startLat La latitudine del punto di partenza.
     * @param startLong La longitudine del punto di partenza.
     * @param endLat La latitudine del punto di arrivo.
     * @param endLong La longitudine del punto di arrivo.
     * @return La distanza approssimativa in chilometri tra i due punti geografici.
     * @throws IllegalArgumentException se i parametri di latitudine o longitudine non sono validi.
     */
    public double calculateDistance_Geotools(Latitude startLat, Longitude startLong, Latitude endLat, Longitude endLong) {
        GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();

        Coordinate coordStart = new Coordinate(startLong.doubleValue(), startLat.doubleValue());
        Coordinate coordEnd = new Coordinate(endLong.doubleValue(), endLat.doubleValue());

        Geometry pointStart = geometryFactory.createPoint(coordStart);
        Geometry pointEnd = geometryFactory.createPoint(coordEnd);

        double distanceInDegrees = pointStart.distance(pointEnd);
        return distanceInDegrees * 111.32; // Conversione da gradi a km
    }

    /**
     * Recupera il limite di velocità per una data posizione geografica utilizzando l'API Overpass di OpenStreetMap.
     *
     * Questa funzione invia una richiesta all'API Overpass di OpenStreetMap per ottenere il limite di velocità
     * della via più vicina alla posizione specificata. La posizione è definita dalla latitudine e longitudine fornite.
     * La richiesta viene formulata per cercare vie entro un raggio di 10 metri dalla posizione specificata.
     *
     * La risposta dell'API viene analizzata per estrarre i limiti di velocità delle vie trovate. La funzione poi
     * seleziona il limite di velocità massimo tra quelli trovati come rappresentativo per la posizione data.
     *
     * Nota: La funzione si aspetta che l'API Overpass restituisca una risposta in formato XML. Inoltre, considera
     * solo i limiti di velocità espressi in numeri interi.
     *
     * @param lat La latitudine del punto per cui si desidera conoscere il limite di velocità.
     * @param lon La longitudine del punto per cui si desidera conoscere il limite di velocità.
     * @return Il limite di velocità massimo trovato per la posizione specificata, o null se nessun limite è stato trovato.
     * @throws Exception Se si verifica un errore nella connessione all'API Overpass o nell'analisi della risposta.
     */
    public static SpeedLimit getSpeedLimit_OpenStreet(double lat, double lon) throws Exception {
        String query = "way(around:10," + lat + "," + lon + ")[\"maxspeed\"];out;";
        String urlString = "http://overpass-api.de/api/interpreter?data=" + java.net.URLEncoder.encode(query, "UTF-8");

        URL url = new URL(urlString);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");

        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer content = new StringBuffer();
        while ((inputLine = in.readLine()) != null) {
            content.append(inputLine);
        }
        in.close();

        // Analizzo la risposta per estrarre il limite di velocità
        ArrayList<SpeedLimit> speedLimits = parseOSMXML(content.toString());
        return getMaxSpeedLimit(speedLimits);
    }

    /**
     * Analizza una stringa XML proveniente dall'API Overpass di OpenStreetMap per estrarre i limiti di velocità.
     *
     * Questa funzione prende come input una stringa XML, che è il formato di risposta tipico dell'API Overpass,
     * e analizza il contenuto per estrarre informazioni sui limiti di velocità delle strade. Le informazioni
     * estratte includono il valore del limite di velocità, l'identificativo della via (wayId) e il nome della via.
     *
     * La funzione cerca gli elementi 'way' nel documento XML e per ciascuno di essi esamina i suoi elementi 'tag'
     * per trovare i tag 'maxspeed' e 'name', che rappresentano rispettivamente il limite di velocità della via
     * e il suo nome. I limiti di velocità vengono aggiunti a una lista, che viene poi restituita.
     *
     * Nota: La funzione presuppone che il limite di velocità sia sempre un numero intero e ignora le vie senza
     * un limite di velocità definito o con un formato di limite di velocità non numerico.
     *
     * @param xmlString La stringa XML da analizzare.
     * @return Una lista di oggetti SpeedLimit, ciascuno rappresentante un limite di velocità trovato nel documento XML.
     * @throws Exception Se si verifica un errore nel parsing del documento XML.
     */
    public static ArrayList<SpeedLimit> parseOSMXML(String xmlString) throws Exception {
        if(xmlString.isEmpty()) {
            return new ArrayList<>();
        }

        ArrayList<SpeedLimit> speedLimits = new ArrayList<>();

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(new InputSource(new StringReader(xmlString)));

        NodeList wayList = document.getElementsByTagName("way");
        for (int i = 0; i < wayList.getLength(); i++) {
            Node wayNode = wayList.item(i);
            if (wayNode.getNodeType() == Node.ELEMENT_NODE) {
                Element wayElement = (Element) wayNode;

                long wayId = Long.parseLong(wayElement.getAttribute("id"));
                String wayName = "";
                int maxSpeed = -1;

                NodeList tagList = wayElement.getElementsByTagName("tag");
                for (int j = 0; j < tagList.getLength(); j++) {
                    Node tagNode = tagList.item(j);
                    if (tagNode.getNodeType() == Node.ELEMENT_NODE) {
                        Element tagElement = (Element) tagNode;
                        String key = tagElement.getAttribute("k");
                        String value = tagElement.getAttribute("v");

                        if ("maxspeed".equals(key)) {
                            maxSpeed = Integer.parseInt(value);
                        } else if ("name".equals(key)) {
                            wayName = value;
                        }
                    }
                }

                if (maxSpeed != -1) {
                    speedLimits.add(new SpeedLimit(maxSpeed, wayId, wayName));
                }
            }
        }

        return speedLimits;
    }

    /**
     * Determina il limite di velocità massimo da una lista di oggetti SpeedLimit.
     *
     * Questa funzione esamina una lista di oggetti SpeedLimit e restituisce quello con il valore
     * massimo di limite di velocità. Se la lista fornita è vuota, restituisce null.
     *
     * @param speedLimits La lista di oggetti SpeedLimit da cui determinare il limite massimo.
     * @return Il limite di velocità massimo trovato nella lista fornita, o null se la lista è vuota.
     */
    private static SpeedLimit getMaxSpeedLimit(ArrayList<SpeedLimit> speedLimits) {
        if (speedLimits.isEmpty()) {
            return null;
        }

        SpeedLimit maxSpeedLimit = speedLimits.get(0);
        for (SpeedLimit speedLimit : speedLimits) {
            if (speedLimit.getMaxSpeed() > maxSpeedLimit.getMaxSpeed()) {
                maxSpeedLimit = speedLimit;
            }
        }

        return maxSpeedLimit;
    }

}
