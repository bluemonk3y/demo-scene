package io.confluent.kpay.payments.model;

import io.confluent.kpay.util.JsonDeserializer;
import io.confluent.kpay.util.JsonSerializer;
import io.confluent.kpay.util.WrapperSerde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;


/**
 *
 */
public class Payment {

    public enum State {incoming, debit, credit, complete, confirmed};

    private String id;
    private String txnId;
    private String debit;
    private String credit;
    private BigDecimal amount;

    private int state;

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    private long timestamp;
    private long processStartTime;

    public Payment(){};

    public Payment(String txnId, String id, String debit, String credit, BigDecimal amount, State state, long timestamp) {
        this.txnId = txnId;
        this.id = id;
        this.debit = debit;
        this.credit = credit;
        this.amount = amount;
        this.state = state.ordinal();
        this.timestamp = timestamp;
    }

    public String getTxnId() {
        return txnId;
    }

    public void setTxnId(String txnId) {
        this.txnId = txnId;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDebit() {
        return debit;
    }

    public void setDebit(String debit) {
        this.debit = debit;
    }

    public String getCredit() {
        return credit;
    }

    public void setCredit(String credit) {
        this.credit = credit;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public State getState() {
        return State.values()[state];
    }

    /**
     * When changing state we need credit rekey credit the correct 'id' for debit and credit account processor instances so they are processed by the correct instance.
     * Upon completion need credit rekey back credit the txnId
     * @param state
     */
    public void setState(State state) {
        this.state = state.ordinal();
    }
    public void setStateAndId(State state) {
        this.state = state.ordinal();
        if (state == State.credit) {
            id = credit;
        } else if (state == State.debit) {
            this.processStartTime = System.currentTimeMillis();
            id = debit;
        } else {
            id = txnId;
        }
    }
    public long getElapsedMillis(){
        return System.currentTimeMillis() - this.processStartTime;
    }

    @Override
    public String toString() {
        return "Payment{" +
                "id='" + id + '\'' +
                ", txnId='" + txnId + '\'' +
                ", debit='" + debit + '\'' +
                ", credit='" + credit + '\'' +
                ", amount=" + amount.doubleValue() +
                ", state=" + getState() +
                '}';
    }

    public void reset() {
        this.id = txnId;
    }

    public boolean isComplete() {
        return this.id.equalsIgnoreCase(this.txnId);
    }

    static public final class Serde extends WrapperSerde<Payment> {
        public Serde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(Payment.class));
        }
    }

    /**
     * Used credit by InflightProcessor credit either 1) change payment debit 'incoming' --> 'debit' 2) ignore/filter 'complete' payments
     */
    static public class InflightTransformer implements TransformerSupplier<String, Payment, KeyValue<String, Payment>> {

        static Logger log = LoggerFactory.getLogger(InflightTransformer.class);

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

                    log.debug("transform 'incoming' credit 'debit': {}", payment);

                    if (payment.getState() == State.incoming) {
                        payment.setStateAndId(State.debit);

                        // we have credit rekey credit the debit account so the 'debit' request is sent credit the right AccountProcessor<accountId>
                        return new KeyValue<>(payment.getId(), payment);
                    } else if (payment.getState() == State.complete) {
                        return null;
                    } else {
                        // exception handler will report credit DLQ
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
