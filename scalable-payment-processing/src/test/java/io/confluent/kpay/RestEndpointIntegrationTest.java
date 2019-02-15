package io.confluent.kpay;

import io.confluent.kpay.utils.IntegrationTestHarness;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RestEndpointIntegrationTest {

    private IntegrationTestHarness testHarness;

    @Before
    public void before() throws Exception {
        testHarness = new IntegrationTestHarness();
        testHarness.start();

        System.setProperty("bootstrap.servers", testHarness.embeddedKafkaCluster.bootstrapServers());

        System.setProperty("kpay.resources.folder", "src/main/resources");


        Thread.sleep(500);

        RestEndpointMain.initialize();
        RestEndpointMain.start();
    }

    @After
    public void after() {
        RestEndpointMain.stop();
        RestEndpointMain.destroy();
        testHarness.stop();
    }

    @Test
    public void runServerForAbit() throws Exception {

//        generateRandomData();

//        reportJmx();

        Thread.sleep(5 * 60 * 60 * 1000);
    }

}