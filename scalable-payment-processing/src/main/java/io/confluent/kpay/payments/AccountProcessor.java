package io.confluent.kpay.payments;

import com.landoop.lenses.topology.client.AppType;
import com.landoop.lenses.topology.client.Representation;
import com.landoop.lenses.topology.client.TopologyBuilder;
import com.landoop.lenses.topology.client.TopologyClient;
import com.landoop.lenses.topology.client.kafka.metrics.KafkaTopologyClient;
import com.landoop.lenses.topology.client.kafka.metrics.TopologyKafkaStreamsClientSupplier;
import io.confluent.kpay.payments.model.AccountBalance;
import io.confluent.kpay.payments.model.Payment;
import io.confluent.kpay.rest_iq.KTableResourceEndpoint;
import io.confluent.kpay.rest_iq.KVStoreProvider;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

public class AccountProcessor {
    public static final String STORE_NAME = "account";
    private final KTable<String, AccountBalance> accountBalanceKTable;
    private final Topology topology;

    private static final Logger log = LoggerFactory.getLogger(AccountProcessor.class);
    private final TopologyKafkaStreamsClientSupplier lensesTopologyClient;

    private Properties streamsConfig;
    private KafkaStreams streams;
    private KTableResourceEndpoint<String, AccountBalance> microRestService;


    public AccountProcessor(String paymentsInflightTopic, String paymentsCompleteTopic, Properties streamsConfig){

        // Note:  need consistent naming for global streamMetaDataDiscovery support
        // streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, AccountProcessor.class.getCanonicalName());

        this.streamsConfig = streamsConfig;

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Payment> inflight = builder.stream(paymentsInflightTopic);

        Materialized<String, AccountBalance, KeyValueStore<Bytes, byte[]>> account = Materialized.as(STORE_NAME);
        Materialized<String, AccountBalance, KeyValueStore<Bytes, byte[]>> accountStore = account.withKeySerde(new StringSerde()).withValueSerde(new AccountBalance.Serde());


        /**
         * Debit & credit processing
         */
        accountBalanceKTable = inflight.groupByKey()
                .aggregate(
                        AccountBalance::new,
                        (key, value, aggregate) -> aggregate.handle(key, value),
                        accountStore
                );

        Predicate<String, Payment> isCreditRecord =  (key, value) -> value.getState() == Payment.State.credit;
        Predicate<String, Payment> isCompleteRecord =  (key, value) -> value.getState() == Payment.State.complete;

        /**
         * Data flow and state processing
         */
        KStream<String, Payment>[] branch = inflight
                .map((KeyValueMapper<String, Payment, KeyValue<String, Payment>>) (key, value) -> {
                    if (value.getState() == Payment.State.debit) {
                        value.setStateAndId(Payment.State.credit);
                    } else if (value.getState() == Payment.State.credit) {
                        value.setStateAndId(Payment.State.complete);
                    } else if (value.getState() == Payment.State.complete) {
                        log.error("Invalid payment:{}", value);
                        throw new RuntimeException("Invalid payment state:" + value);
                    }
                    return new KeyValue<>(value.getId(), value);
                })
                .branch(isCreditRecord, isCompleteRecord);


        branch[0].to(paymentsInflightTopic);
        branch[1].to(paymentsCompleteTopic);

        topology = builder.build();
        lensesTopologyClient = createTopology(streamsConfig, paymentsInflightTopic, paymentsCompleteTopic, topology);

    }
    public Topology getTopology() {
        return topology;
    }

    public void start() {
        streams = new KafkaStreams(topology, streamsConfig, lensesTopologyClient);
        streams.start();

        log.info(topology.describe().toString());

        microRestService = new KTableResourceEndpoint<String, AccountBalance>(new KVStoreProvider<>(streams, accountBalanceKTable)){};
        microRestService.start(streamsConfig);
    }

    private TopologyKafkaStreamsClientSupplier createTopology(Properties streamsConfig, String paymentsInflightTopic, String outputTopic, Topology topology) {

        Properties topologyProps = new Properties();
        topologyProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, streamsConfig.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        TopologyClient client = KafkaTopologyClient.create(topologyProps);
        com.landoop.lenses.topology.client.Topology lensesTopology = TopologyBuilder.start(AppType.KafkaStreams, getClass().getCanonicalName())
                .withTopic(paymentsInflightTopic)
                .withDescription("Inflight Payments")
                .withRepresentation(Representation.STREAM)
                .endNode()
                .withTopic(outputTopic)
                .withParent(paymentsInflightTopic)
                .withDescription("Send to complete topic")
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

    public ReadOnlyKeyValueStore<String, AccountBalance> getStore() {
        return streams.store(accountBalanceKTable.queryableStoreName(), QueryableStoreTypes.keyValueStore());
    }
    public Collection<StreamsMetadata> allMetaData() {
        return streams.allMetadata();
    }

    public KafkaStreams streams() {
        return streams;
    }

    public KTableResourceEndpoint<String, AccountBalance> getMicroRestService() {
        return microRestService;
    }
}
