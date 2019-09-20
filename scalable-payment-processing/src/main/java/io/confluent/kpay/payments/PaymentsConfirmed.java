package io.confluent.kpay.payments;

import io.confluent.kpay.payments.model.ConfirmedStats;
import io.confluent.kpay.payments.model.Payment;
import io.confluent.kpay.rest_iq.WindowKTableResourceEndpoint;
import io.confluent.kpay.rest_iq.WindowKVStoreProvider;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Properties;

public class PaymentsConfirmed {
    public static final String STORE_NAME = "confirmed";
    long ONE_DAY = 24 * 60 * 60 * 1000L;

    private static final Logger log = LoggerFactory.getLogger(PaymentsConfirmed.class);
    private final KTable<Windowed<String>, ConfirmedStats> confirmedKTable;
    private WindowKTableResourceEndpoint<String, ConfirmedStats> microRestService;

    private final Topology topology;
    private Properties streamsConfig;
    private KafkaStreams streams;



    public PaymentsConfirmed(String paymentsCompleteTopic, String paymentsConfirmedTopic, Properties streamsConfig){
        this.streamsConfig = streamsConfig;

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Payment> complete = builder.stream(paymentsCompleteTopic);

        /**
         * Data flow; emit the payments as Confirmed once they have been processed
         */
        complete.transform(new CompleteTransformer()).to(paymentsConfirmedTopic);

        Materialized<String, ConfirmedStats, WindowStore<Bytes, byte[]>> confirmedStore = Materialized.as(STORE_NAME);
        Materialized<String, ConfirmedStats, WindowStore<Bytes, byte[]>> confirmedWindowStore = confirmedStore.withKeySerde(new StringSerde()).withValueSerde(new ConfirmedStats.Serde());

        /**
         * Confirmation processing
         */
        confirmedKTable = complete
                .groupBy((key, value) -> Integer.toString(key.hashCode() % 10)) // redistribute to restricted key-set
                .windowedBy(TimeWindows.of(ONE_DAY))
                .aggregate(
                        ConfirmedStats::new,
                        (key, value, aggregate) -> aggregate.update(value),
                        confirmedWindowStore
                );

        topology = builder.build();

    }
    public Topology getTopology() {
        return topology;
    }

    public void start() {
        streams = new KafkaStreams(topology, streamsConfig);
        streams.start();

        log.info(topology.describe().toString());

        microRestService = new WindowKTableResourceEndpoint<String, ConfirmedStats>(new WindowKVStoreProvider<>(streams, confirmedKTable)) {
        };
        microRestService.start(streamsConfig);
    }

    public void stop() {
        streams.close();
        streams.cleanUp();
        microRestService.stop();
    }

    public Collection<StreamsMetadata> allMetaData() {
        return streams.allMetadata();
    }


    public ReadOnlyWindowStore<String, ConfirmedStats> getStore() {
        return streams.store(confirmedKTable.queryableStoreName(), QueryableStoreTypes.windowStore());
    }

    public KafkaStreams streams() {
        return streams;
    }

    public WindowKTableResourceEndpoint<String, ConfirmedStats> getMicroRestService() {
        return microRestService;
    }

    /**
     * Used to by InflightProcessor to either 1) change payment from 'incoming' --> 'debit' 2) ignore/filter 'complete' payments
     */
    static public class CompleteTransformer implements TransformerSupplier<String, Payment, KeyValue<String, Payment>> {
        @Override
        public org.apache.kafka.streams.kstream.Transformer<String, Payment, KeyValue<String, Payment>> get() {
            return new org.apache.kafka.streams.kstream.Transformer<String, Payment, KeyValue<String, Payment>>() {
                private ProcessorContext context;

                @Override
                public void init(ProcessorContext context) {
                    this.context = context;
                }

                @Override
                public KeyValue<String, Payment> transform(String key, Payment payment) {
                    if (payment.getState() == Payment.State.complete) {
                        payment.setStateAndId(Payment.State.confirmed);
                        return new KeyValue<>(key, payment);
                    } else {
                        // exception handler will report to DLQ
                        throw new RuntimeException("Invalid Payment state, expecting debit or credit but got" + payment.getState() + ": "+ payment.toString());
                    }
                }

                @Override
                public void close() {

                }
            };
        }
    };
}
