package io.confluent.kpay;

import io.confluent.kpay.utils.IntegrationTestHarness;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

public class RestEndpointIntegrationTest {

    private IntegrationTestHarness testHarness;


    /**
     * Look at adopting: https://www.testcontainers.org/modules/kafka
     *
     * @throws Exception
     */
    @Before
    public void before() throws Exception {
        testHarness = new IntegrationTestHarness();
        testHarness.start("localhost:9092");

        System.setProperty("bootstrap.servers", testHarness.bootstrapServers());

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
        generatePaymentData();

        Thread.sleep(5 * 60 * 60 * 1000);
    }

    private void generatePaymentData() {

        Client client = ClientBuilder.newClient();

        WebTarget tsTarget = client.target("http://localhost:8080").path("/kpay/payments/startProcessors");

        String response =
                tsTarget.request(MediaType.APPLICATION_JSON_TYPE)
                        .post(Entity.entity("4", MediaType.APPLICATION_JSON_TYPE),
                                String.class);

        System.out.println("TEST DATA:" + response);
    }

}
