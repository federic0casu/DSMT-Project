package it.unipi.dsmt.functions;

import it.unipi.dsmt.models.Violation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import it.unipi.dsmt.models.GeoLocalizationEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

//      TEORIA
// Keyed Context:
// Poiché stai utilizzando una KeyedProcessFunction, il contesto è "chiaviato". Significa che tutte le
// operazioni eseguite all'interno della processElement o onTimer sono specifiche per la chiave corrente
// (il vin dell'auto in questo caso).

// Stato e Timer per Chiave:
// Flink mantiene uno stato e i timer separati per ogni chiave. Quando processi un elemento o quando scade
// un timer, hai accesso allo stato e ai timer specifici per quella chiave. Ciò significa che ogni auto
// (ogni vin) ha il proprio stato indipendente e timer.

public class InactivityViolationFunction extends KeyedProcessFunction<String, GeoLocalizationEvent, Violation> {
    private transient ValueState<Long> lastUpdateTs_state;
    // Mantiene l'ultimo timestamp di aggiornamento per ogni auto. (specifico per ogni chiave)

    private transient ValueState<Long> timerExpiration_state;
    // Salva l'identificativo del timer corrente per ogni auto. (specifico per ogni chiave)


    // Long : last update timestamp
    // String : vin


    // La funzione open viene chiamata una volta all'inizio della vita di un'istanza di una funzione
    // o di un operatore, prima che vengano processati qualsiasi elementi o eventi.
    @Override
    public void open(Configuration parameters) {
        lastUpdateTs_state = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastUpdate", Long.class));
        timerExpiration_state = getRuntimeContext().getState(
                new ValueStateDescriptor<>("timer", Long.class));
    }

    // invocato automaticamente ogni volta che un evento GeoLocalizationEvent arriva
    @Override
    public void processElement(GeoLocalizationEvent event, Context ctx, Collector<Violation> out) throws Exception {
        // In Apache Flink, i timer non hanno un identificativo esposto che possa essere salvato o gestito direttamente.
        // Al contrario, i timer in Flink sono gestiti internamente e possono essere impostati o cancellati sulla base
        // del loro timestamp di scadenza.

        // Aggiorna il timestamp dell'ultimo evento.
        long currentEventTime = event.getTimestamp();
        lastUpdateTs_state.update(currentEventTime);

        GeoLocalizationEvent.Type eventType = event.getType();

        if (eventType == GeoLocalizationEvent.Type.MOVE || eventType == GeoLocalizationEvent.Type.START) {
            // Imposta o aggiorna il timer per eventi di tipo MOVE e START
            Long currentTimer = timerExpiration_state.value();

            // Controlla se esiste già un timer e lo cancella se presente.
            if (currentTimer != null) {
                ctx.timerService().deleteEventTimeTimer(currentTimer);
            }
            // Imposta un nuovo timer per 100 secondi nel futuro e aggiorna lo stato del timer.
            long newTimer = currentEventTime + 100000; // 100 seconds in the future
            ctx.timerService().registerEventTimeTimer(newTimer);
            timerExpiration_state.update(newTimer);
        }
        else if (eventType == GeoLocalizationEvent.Type.END) {
            // Cancella il timer esistente per eventi di tipo END
            Long currentTimer = timerExpiration_state.value();
            if (currentTimer != null) {
                ctx.timerService().deleteEventTimeTimer(currentTimer);
                timerExpiration_state.clear();
            }
        }
    }

    // La funzione onTimer viene chiamata quando un timer precedentemente registrato raggiunge il suo tempo di scadenza.
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Violation> out) throws Exception {
        Long lastUpdate = lastUpdateTs_state.value();

        // Se il timer scade e non sono stati ricevuti eventi per 100 secondi, genera un evento InactivityViolation.
        if (lastUpdate != null && timestamp - lastUpdate >= 100000) {
            Violation violation = new Violation(ctx.getCurrentKey(), lastUpdate);
            violation.setTsLastActivity(lastUpdate);
            out.collect(violation);
        }

        // Azzera lo stato del timer dopo che il timer si è attivato.
        timerExpiration_state.clear(); // Reset the timer state after the timer fires
    }
}
