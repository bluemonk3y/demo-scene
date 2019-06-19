package io.confluent.kpay;

import com.landoop.lenses.topology.client.kafka.metrics.MicroserviceTopology;
import io.confluent.common.utils.TestUtils;
import io.confluent.kpay.control.ControlProperties;
import io.confluent.kpay.control.Controllable;
import io.confluent.kpay.control.PauseControllable;
import io.confluent.kpay.control.StartStopController;
import io.confluent.kpay.metrics.PaymentsThroughput;
import io.confluent.kpay.metrics.model.ThroughputStats;
import io.confluent.kpay.payments.AccountProcessor;
import io.confluent.kpay.payments.PaymentProperties;
import io.confluent.kpay.payments.PaymentsConfirmed;
import io.confluent.kpay.payments.PaymentsInFlight;
import io.confluent.kpay.payments.model.AccountBalance;
import io.confluent.kpay.payments.model.ConfirmedStats;
import io.confluent.kpay.payments.model.InflightStats;
import io.confluent.kpay.payments.model.Meta;
import io.confluent.kpay.payments.model.Payment;
import io.confluent.kpay.rest_iq.KTableResourceEndpoint;
import io.confluent.kpay.rest_iq.WindowKTableResourceEndpoint;
import io.confluent.kpay.util.KafkaTopicClient;
import io.confluent.kpay.util.KafkaTopicClientImpl;
import io.confluent.kpay.util.Pair;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.Collections.sort;

public class KPayAllInOneImpl implements KPay {
    private static final Logger log = LoggerFactory.getLogger(KPayAllInOneImpl.class);


    private String bootstrapServers;

    private KafkaTopicClient topicClient;

    private PaymentsInFlight paymentsInFlight;
    private AccountProcessor paymentAccountProcessor;
    private PaymentsConfirmed paymentsConfirmed;
    private PaymentsThroughput instrumentationThroughput;


    private AdminClient adminClient;
    private StartStopController controllerStartStop;
    private ScheduledFuture scheduledPaymentFuture;
    private PaymentRunnable paymentRunnable;

    public KPayAllInOneImpl(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    /**
     * Fire up all of the stream processors
     */
    public void startProcessors() throws UnknownHostException {

        Controllable pauseController = new PauseControllable();

        startPaymentPipeline(pauseController);

        startController(pauseController);

        startInstrumentation();

        log.info("MetaStores:" + getMetaStores());
    }

    public String getMetaStores() {
        Collection<StreamsMetadata> streamsMetadata = paymentAccountProcessor.allMetaData();
        return streamsMetadata.toString();
    }

    private void startController(Controllable pauseController) {
        controllerStartStop = new StartStopController(ControlProperties.controlStatusTopic, ControlProperties.get(StartStopController.class.getSimpleName(), bootstrapServers), pauseController);
        controllerStartStop.start();
    }

    private int instrumentationPortOffset = 21000;

    private void startInstrumentation() {
        instrumentationThroughput = new PaymentsThroughput(PaymentProperties.paymentsCompleteTopic, PaymentProperties.get(PaymentsThroughput.class.getSimpleName(), bootstrapServers, instrumentationPortOffset++));
        instrumentationThroughput.start();
    }

    private int paymentsPortOffset = 20000;

    private void startPaymentPipeline(Controllable pauseController) {
        paymentsInFlight = new PaymentsInFlight(PaymentProperties.paymentsIncomingTopic, PaymentProperties.paymentsInflightTopic, PaymentProperties.paymentsCompleteTopic, PaymentProperties.get(PaymentsInFlight.class.getSimpleName(), bootstrapServers, paymentsPortOffset++), pauseController);
        paymentsInFlight.start();

        paymentAccountProcessor = new AccountProcessor(PaymentProperties.paymentsInflightTopic, PaymentProperties.paymentsCompleteTopic, PaymentProperties.get(AccountProcessor.class.getSimpleName(), bootstrapServers, paymentsPortOffset++));
        paymentAccountProcessor.start();

        paymentsConfirmed = new PaymentsConfirmed(PaymentProperties.paymentsCompleteTopic, PaymentProperties.paymentsConfirmedTopic, PaymentProperties.get(PaymentsConfirmed.class.getSimpleName(), bootstrapServers, paymentsPortOffset++));
        paymentsConfirmed.start();
    }

    @Override
    public String status() {
        return controllerStartStop.status();
    }
    @Override
    public String pause() {
        controllerStartStop.pause();
        return controllerStartStop.status();
    }
    @Override
    public String resume() {
        controllerStartStop.resume();
        return controllerStartStop.status();
    }

    @Override
    public String shutdown() {
        controllerStartStop.pause();
        paymentsInFlight.stop();
        paymentsConfirmed.stop();
        paymentAccountProcessor.stop();
        instrumentationThroughput.stop();

        return "shutdown processors complete";
    }

    //https://github.com/Landoop/lenses-topology-example/blob/a82ab5fe00922d53203ef6afc31b60dc83998736/lenses-topology-example-microservice/src/main/java/io/lenses/topology/example/microservice/S3StorageService.java#L59
    private void createTopology(String sourceName, Properties properties, KafkaProducer<?, ?> producer, String topic) {
        try {
            MicroserviceTopology.fromProducer(sourceName, producer, Collections.singletonList(topic), properties);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void generatePayments(final int ratePerSecond) {

        KafkaProducer<String, Payment> producer =
                new KafkaProducer<>(properties(), new StringSerializer(), new Payment.Serde().serializer());

        createTopology("kpay.payments.PaymentGenerator", properties(), producer, PaymentProperties.paymentsIncomingTopic);

        KafkaProducer<String, Meta> metaProducer =
                new KafkaProducer<>(properties(), new StringSerializer(), new Meta.Serde().serializer());

        createTopology("kpay.payments.MetaGenerator", properties(), metaProducer, "kpay.payment.meta");


        ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        final String header = "pay1-";


        paymentRunnable = new PaymentRunnable() {

            AtomicInteger position = new AtomicInteger();
            String[] from = new String[]{"larry", "joe", "bob", "moe", "joshua", "harry"};
            String[] to = new String[]{"ali", "jen", "lu", "issy", "sophia", "mary"};

            @Override
            public void run() {

                try {
                    long time = System.currentTimeMillis();
                    Payment payment = new Payment(header + position, position.toString(), from[position.get() % from.length], to[position.get() % from.length], new BigDecimal(Math.round((Math.random() * 100.0) * 100.0) / 100.0), Payment.State.incoming, time);
                    log.info("Send:" + payment);
                    producer.send(buildRecord(PaymentProperties.paymentsIncomingTopic, time, payment.getId(), payment));
                    metaProducer.send(new ProducerRecord<String, Meta>("kpay.payment.meta", payment.getTxnId(), new Meta(payment.getTxnId(), position.toString(), "username is " + payment.getFrom())));
                    position.incrementAndGet();
                    producer.flush();
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
            public void stop() {
                log.info("Stopping payment generation");
                producer.close();
            }
        };

        log.info("Generating Payments at:" + ratePerSecond);
        scheduledPaymentFuture = scheduledExecutor.scheduleAtFixedRate(paymentRunnable, 1000, ratePerSecond > 1000 ? 1 : 1000/ratePerSecond, TimeUnit.MILLISECONDS);
    }


    @Override
    public void stopPayments() {
        log.info("Stopping Payment generation");

        scheduledPaymentFuture.cancel(false);
        paymentRunnable.stop();
        scheduledPaymentFuture = null;
    }

    @Override
    public boolean isGeneratingPayments() {
        return scheduledPaymentFuture != null;
    }

    private <V> ProducerRecord<String, Payment> buildRecord(String topicName,
                                                            Long timestamp,
                                                            String key, Payment payment) {
        return new ProducerRecord<>(topicName, null, timestamp,  key, payment);
    }

    @Override
    public ThroughputStats viewMetrics() {
        return instrumentationThroughput.getStats();
    }


    @Override
    public List<AccountBalance> listAccounts() {
        KTableResourceEndpoint<String, AccountBalance> microRestService = paymentAccountProcessor.getMicroRestService();
        List<String> accountKeys = new ArrayList<>(microRestService.keySet());
        sort(accountKeys);
        List<Pair<String, AccountBalance>> pairs = microRestService.get(accountKeys);
        return pairs.stream().map(p -> p.getV()).collect(Collectors.toList());
    }

    public Pair<InflightStats, ConfirmedStats> getPaymentPipelineStats() {
        WindowKTableResourceEndpoint<String, InflightStats> paymentsInFlightSvc = paymentsInFlight.getMicroRestService();

        List<Pair<String, InflightStats>> inflightStats = paymentsInFlightSvc.get(new ArrayList<>(paymentsInFlightSvc.keySet()));

        WindowKTableResourceEndpoint<String, ConfirmedStats> paymentsConfirmedSvc = paymentsConfirmed.getMicroRestService();

        List<Pair<String, ConfirmedStats>> confirmedStats = paymentsConfirmedSvc.get(new ArrayList<>(paymentsConfirmedSvc.keySet()));

        if (inflightStats.size() == 0 || confirmedStats.size() == 0) return new Pair<>(new InflightStats(), new ConfirmedStats());

        Iterator<Pair<String, InflightStats>> iterator = inflightStats.iterator();
        InflightStats inflightStatsValue = iterator.next().getV();
        while (iterator.hasNext()) {
            inflightStatsValue.add(iterator.next().getV());
        }

        Iterator<Pair<String, ConfirmedStats>> confirmedIterator = confirmedStats.iterator();
        ConfirmedStats confirmedStatsValue = confirmedIterator.next().getV();
        while (confirmedIterator.hasNext()) {
            confirmedStatsValue.add(confirmedIterator.next().getV());
        }

        return new Pair<>(inflightStatsValue, confirmedStatsValue);
    }

    @Override
    public String showAccountDetails(String accountName) {
        return null;
    }


    public void initializeEnvironment() {
        PaymentProperties.initializeEnvironment(getTopicClient());
        ControlProperties.initializeEnvironment(getTopicClient());
    }

    private KafkaTopicClient getTopicClient() {
        if (topicClient == null) {
            Map<String, Object> configMap = new HashMap<>();
            configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            configMap.put("application.id", "KPAY");
            configMap.put("commit.interval.ms", 0);
            configMap.put("cache.max.bytes.buffering", 0);
            configMap.put("auto.offset.reset", "earliest");
            configMap.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());

            this.adminClient = AdminClient.create(configMap);
            this.topicClient = new KafkaTopicClientImpl(adminClient);
        }
        return this.topicClient;
    }

    private Properties properties() {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        return producerConfig;
    }

    private interface PaymentRunnable extends Runnable {
        void stop();

    }

}
