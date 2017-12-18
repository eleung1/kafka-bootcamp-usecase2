package com.eric.serde;

import com.rbc.cloud.hackathon.data.Transactions;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public class TransactionSerdes extends WrapperSerde<Transactions> {

  public TransactionSerdes(Serializer<Transactions> serializer, Deserializer<Transactions> deserializer) {
    super(serializer, deserializer);
  }
}
