package com.eric.kafkabootcampstream.streamer;

import java.util.Properties;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import kafka.consumer.ConsumerConfig;

@Component
public class MyStreamer {

  private Logger logger = LoggerFactory.getLogger(MyStreamer.class);
  
  @Autowired
  KafkaStreams kafkaStreams;

  @PostConstruct
  public void runStream() {
    logger.info("*** STARTING STREAM!!! ***");
    kafkaStreams.start();
  }

  @PreDestroy
  public void closeStream() {
    kafkaStreams.close();
  }
}