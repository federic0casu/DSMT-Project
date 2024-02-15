package it.unipi.dsmt.functions;

import it.unipi.dsmt.models.Violation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import it.unipi.dsmt.models.GeoLocalizationEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InactivityViolationFunction extends KeyedProcessFunction<String, GeoLocalizationEvent, Violation> {
    private static final Logger LOG = LoggerFactory.getLogger(SpeedingViolationFunction.class);
    private transient ValueState<Long> lastUpdateTs_state;
    // Mantiene l'ultimo timestamp di aggiornamento per ogni auto. (specifico per ogni chiave)

    private transient ValueState<Long> timerExpiration_state;
    // Salva l'identificativo del timer corrente per ogni auto. (specifico per ogni chiave)



    // La funzione open viene chiamata una volta all'inizio della vita di un'istanza di una funzione
    // o di un operatore, prima che vengano processati qualsiasi elementi o eventi.
    @Override
    public void open(Configuration parameters) {
        lastUpdateTs_state = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastUpdate", Long.class));
        timerExpiration_state = getRuntimeContext().getState(
                new ValueStateDescriptor<>("timer", Long.class));
    }

    // Invocato automaticamente ogni volta che un evento GeoLocalizationEvent arriva
    @Override
    public void processElement(GeoLocalizationEvent event, Context ctx, Collector<Violation> out) throws Exception {
        // In Apache Flink, i timer non hanno un identificativo esposto che possa essere salvato o gestito direttamente.
        // Al contrario, i timer in Flink sono gestiti internamente e possono essere impostati o cancellati sulla base
        // del loro timestamp di scadenza.

        // Aggiorna il timestamp dell'ultimo evento.
        long currentEventTime = event.getTimestamp();
        lastUpdateTs_state.update(currentEventTime);

        // Essendo arrivato un nuovo evento, se c'era un timer attivo per l'auto, lo cancello.
        Long currentTimer = timerExpiration_state.value();
        if (currentTimer != null) {
            ctx.timerService().deleteProcessingTimeTimer(currentTimer);
        }

        // Ora controllo il tipo di evento e agisco di conseguenza: se impostare un nuovo timer oppure no.
        GeoLocalizationEvent.Type eventType = event.getType();
        if (eventType == GeoLocalizationEvent.Type.MOVE || eventType == GeoLocalizationEvent.Type.START) {
            // Se l'evento è di tipo MOVE o START, imposta un nuovo timer per 10 secondi e aggiorna lo stato del timer.

            long newTimer = currentEventTime + 10000; // 10 seconds in the future
            ctx.timerService().registerProcessingTimeTimer(newTimer);
            timerExpiration_state.update(newTimer);
        }
        else if (eventType == GeoLocalizationEvent.Type.END) {
            // Se l'evento è di tipo END, cancella il timer e azzera lo stato del timer.

            timerExpiration_state.clear();
        }
    }

    // La funzione onTimer viene chiamata quando un timer precedentemente registrato raggiunge il suo tempo di scadenza.
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Violation> out) throws Exception {
        // Recuperare il valore dell'ultimo aggiornamento per la chiave corrente
        Long lastUpdate = lastUpdateTs_state.value();

        // Creare una nuova violazione utilizzando il terzo costruttore
        Violation violation = new Violation(timestamp, ctx.getCurrentKey(), (lastUpdate==null)?-1:lastUpdate);
        out.collect(violation);

        // Loggare l'emissione della violazione
        LOG.info("Timer expired for key: {}, violationTs: {}, tsLastActivity: {}.", ctx.getCurrentKey(), violation.getViolationTs(), violation.getTsLastActivity());

        // Azzera lo stato del timer dopo che il timer si è attivato.
        timerExpiration_state.clear();
    }
}
