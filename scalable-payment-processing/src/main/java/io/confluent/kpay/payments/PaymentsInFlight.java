package io.confluent.kpay.payments;

import com.landoop.lenses.topology.client.AppType;
import com.landoop.lenses.topology.client.NodeType;
import com.landoop.lenses.topology.client.Representation;
import com.landoop.lenses.topology.client.TopologyBuilder;
import com.landoop.lenses.topology.client.TopologyClient;
import com.landoop.lenses.topology.client.kafka.metrics.KafkaTopologyClient;
import com.landoop.lenses.topology.client.kafka.metrics.TopologyKafkaStreamsClientSupplier;
import io.confluent.kpay.control.Controllable;
import io.confluent.kpay.payments.model.InflightStats;
import io.confluent.kpay.payments.model.Payment;
import io.confluent.kpay.rest_iq.WindowKTableResourceEndpoint;
import io.confluent.kpay.rest_iq.WindowKVStoreProvider;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;

public class PaymentsInFlight {
    private static final Logger log = LoggerFactory.getLogger(PaymentsInFlight.class);
    public static final String STORE_NAME = "inflight";

    private static long ONE_DAY = 24 * 60 * 60 * 1000L;

    private final KTable<Windowed<String>, InflightStats> paymentStatsKTable;
    private final TopologyKafkaStreamsClientSupplier lensesTopologyClient;
    private WindowKTableResourceEndpoint<String, InflightStats> microRestService;

    private final Topology topology;
    private Properties streamsConfig;
    private KafkaStreams streams;


    public PaymentsInFlight(String paymentsIncomingTopic, String paymentsInflightTopic, String paymentsCompleteTopic, Properties streamsConfig, Controllable controllable){
        this.streamsConfig = streamsConfig;

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Payment> inflight = builder.stream(Arrays.asList(paymentsIncomingTopic, paymentsCompleteTopic));

        // emit the  payments as Debits on the 'inflight' stream
        Materialized<String, InflightStats, WindowStore<Bytes, byte[]>> inflightFirst = Materialized.as(STORE_NAME);
        Materialized<String, InflightStats, WindowStore<Bytes, byte[]>> inflightWindowStore = inflightFirst.withKeySerde(new StringSerde()).withValueSerde(new InflightStats.Serde());

        /**
         * Inflight processing
         */
        paymentStatsKTable = inflight
                .filter((key, value) -> controllable.pauseMaybe())
                .groupBy((key, value) -> Integer.toString(key.hashCode() % 10))// reduce event key space for cross event aggregation
                .windowedBy(TimeWindows.of(ONE_DAY))
                .aggregate(
                        InflightStats::new,
                        (key, value, aggregate) -> aggregate.update(value),
                        inflightWindowStore
                );

        /**
         * Data flow processing; flip incoming --> debit and filter complete events
         */
        inflight.map((KeyValueMapper<String, Payment, KeyValue<String, Payment>>) (key, value) -> {
            if (value.getState() == Payment.State.incoming) {
                value.setStateAndId(Payment.State.debit);
            }
            return new KeyValue<>(value.getId(), value);
        }).filter((key, value) -> value.getState() == Payment.State.debit).to(paymentsInflightTopic);
        
        topology = builder.build();
        lensesTopologyClient = createTopology(streamsConfig, paymentsIncomingTopic, paymentsInflightTopic, topology);
    }
    public Topology getTopology() {
        return topology;
    }

    public void start() {

        streams = new KafkaStreams(this.topology, streamsConfig, lensesTopologyClient);
        streams.start();

        log.info(this.topology.describe().toString());

        microRestService = new WindowKTableResourceEndpoint<String, InflightStats>(new WindowKVStoreProvider<>(streams,
                paymentStatsKTable)) {
        };
        microRestService.start(streamsConfig);
    }

    private TopologyKafkaStreamsClientSupplier createTopology(Properties streamsConfig, String paymentsIncomingTopic, String outputTopic, Topology topology) {

        /**
         * it would be possible to build a app-topology definition for registration
         */
        TopologyDescription describe = topology.describe();
        Set<TopologyDescription.Subtopology> subtopologies = describe.subtopologies();
        TopologyDescription.Subtopology next = subtopologies.iterator().next();
        int id = next.id();
        Set<TopologyDescription.Node> nodes = next.nodes();
        for (TopologyDescription.Node node : nodes) {
            String name = node.name();
//            System.out.println("NODE_NAME: "+ node.name() + " |" + node.predecessors() + " NEXT_NODE>>" + node.successors() + "<<");

        }

        Properties topologyProps = new Properties();
        topologyProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, streamsConfig.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        TopologyClient client = KafkaTopologyClient.create(topologyProps);
        com.landoop.lenses.topology.client.Topology lensesTopology = TopologyBuilder.start(AppType.KafkaStreams, getClass().getCanonicalName())
                .withTopic(paymentsIncomingTopic)
                .withDescription("Incoming Payments")
                .withRepresentation(Representation.STREAM)
                .endNode()
                .withNode("groupby", NodeType.GROUPBY)
                .withDescription("Group by TxnId")
                .withRepresentation(Representation.TABLE)
                .withParent(paymentsIncomingTopic)
                .endNode()
                .withNode("stats", NodeType.AGGREGATE)
                .withDescription("Payments stats for those inflight")
                .withRepresentation(Representation.TABLE)
                .withParent("groupby")
                .endNode()
                .withTopic(outputTopic)
                .withParent("stats")
                .withDescription("Send to Inflights topic")
                .withRepresentation(Representation.STREAM)
                .endNode()
                .build();

        try {
            client.register(lensesTopology);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new TopologyKafkaStreamsClientSupplier(client, lensesTopology);
    }

    public void stop() {
        streams.close();
        streams.cleanUp();
        microRestService.stop();
    }

    public ReadOnlyWindowStore<String, InflightStats> getStore() {
        return streams.store(paymentStatsKTable.queryableStoreName(), QueryableStoreTypes.windowStore());
    }

    public KafkaStreams getStreams() {
        return streams;
    }

    public KafkaStreams streams() {
        return streams;
    }

    public WindowKTableResourceEndpoint<String, InflightStats> getMicroRestService() {
        return microRestService;
    }
}
