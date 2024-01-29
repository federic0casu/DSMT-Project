package it.unipi.dsmt.Functions;

import it.unipi.dsmt.Models.GeoLocalization;
import it.unipi.dsmt.Models.LocationState;
import it.unipi.dsmt.Models.TripReport;
import it.unipi.dsmt.Models.TripState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class TripReportProcessFunction extends KeyedProcessFunction<String,
        GeoLocalization, TripReport> {


    private ValueState<TripState> tripState;
    private final double litersPerKm = 0.1;

    static final Map<String, Double> fuelCosts = new HashMap<>();



    public void open(Configuration parameters) throws Exception {
        tripState = getRuntimeContext().getState(new ValueStateDescriptor<>(
                "myTripState", TripState.class));
        fuelCosts.put("Compressed Natural Gas", 0.08);
        fuelCosts.put("Diesel", 0.08);
        fuelCosts.put("E-85/Gasoline", 0.07);
        fuelCosts.put("Electric", 0.03);
        fuelCosts.put("Gasoline", 0.09);
        fuelCosts.put("Ethanol", 0.07);
    }

    // this function processes all geoloc point for a certain vin, and computes total
    // distance and fuel consumption for a certain car for a whole trip
    @Override
    public void processElement(GeoLocalization geoloc, Context ctx, Collector<TripReport> out) throws Exception {
        if (geoloc.getType().equals(GeoLocalization.Type.START)) {
            // inizialize the state for the start of the trip
            TripState myState = new TripState(geoloc.getCar(), geoloc,0,0);
            tripState.update(myState);
        }
        else if (geoloc.getType().equals(GeoLocalization.Type.MOVE)) {
            // compute the distance travelled between the new point and the last
            // recorded point in the stream
            if (tripState.value() != null) {

                TripState myState = new TripState();

                GeoLocalization lastPoint = tripState.value().getLastPoint();
                myState.setLastPoint(lastPoint);
                myState.setCar(tripState.value().getCar());

                double diff = haversineDistance(lastPoint.getLat().doubleValue(),
                        lastPoint.getLon().doubleValue(), geoloc.getLat().doubleValue(),
                        geoloc.getLon().doubleValue());

                double newDistance =
                        tripState.value().currentDistance + diff;
                myState.setCurrentDistance(newDistance);


                double newConsumption =
                        newDistance * litersPerKm * fuelCosts.get("Gasoline");
                myState.setCurrentConsumption(newConsumption);

                tripState.update(myState);

                // collect while moving to downstream
                out.collect(new TripReport(tripState.value().getCar(),
                        tripState.value().getCurrentConsumption(),
                        tripState.value().getCurrentDistance()));
            }
        }
        else if (geoloc.getType().equals(GeoLocalization.Type.END)){
            if (tripState.value() != null) {

                out.collect(new TripReport(tripState.value().getCar(),
                        tripState.value().getCurrentConsumption(),
                        tripState.value().getCurrentDistance()));
                tripState.clear();
            }
        }


    }


    private static double haversineDistance(double latA, double lonA, double latB, double lonB) {
        // Convert latitude and longitude from degrees to radians
        final double EARTH_RADIUS = 6356.752;

        latA = Math.toRadians(latA);
        lonA = Math.toRadians(lonA);
        latB = Math.toRadians(latB);
        lonB = Math.toRadians(lonB);

        // Calculate differences
        double dLat = latB - latA;
        double dLon = lonB - lonA;

        // Haversine formula
        double a = Math.pow(Math.sin(dLat / 2), 2) + Math.cos(latA) * Math.cos(latB) * Math.pow(Math.sin(dLon / 2), 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        // Distance in kilometers
        return EARTH_RADIUS * c;
    }
}
