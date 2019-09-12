package io.confluent.kpay.payments.model;

import io.confluent.kpay.util.JsonDeserializer;
import io.confluent.kpay.util.JsonSerializer;
import io.confluent.kpay.util.WrapperSerde;

import java.math.BigDecimal;


/**
 *
 */
public class Meta {

  private String id;
  private String txnId;
  private String userInfo;
  private BigDecimal amount;

  public Meta() {
  }

  public Meta(String txnId, String id, String userInfo, BigDecimal amount) {
    this.txnId = txnId;
    this.id = id;
    this.userInfo = userInfo;
    this.amount = amount;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getTxnId() {
    return txnId;
  }

  public void setTxnId(String txnId) {
    this.txnId = txnId;
  }

  public String getUserInfo() {
    return userInfo;
  }

  public void setUserInfo(String userInfo) {
    this.userInfo = userInfo;
  }

  public BigDecimal getAmount() {
    return amount;
  }

  public void setAmount(BigDecimal amount) {
    this.amount = amount;
  }

  static public final class Serde extends WrapperSerde<Meta> {
    public Serde() {
      super(new JsonSerializer<>(), new JsonDeserializer<>(Meta.class));
    }
  }


}
