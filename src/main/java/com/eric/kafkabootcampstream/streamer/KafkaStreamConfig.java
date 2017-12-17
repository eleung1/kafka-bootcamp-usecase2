package com.eric.kafkabootcampstream.streamer;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Resource;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;

//import org.apache.kafka.streams.StreamsBuilder;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

@Configuration
@EnableKafka
//@EnableKafkaStreams
@PropertySource(value = "classpath:application.properties")
public class KafkaStreamConfig {

  private Logger logger = LoggerFactory.getLogger(KafkaStreamConfig.class);
  
  @Resource
  private Environment env;
  
  @Autowired
  private ResourceLoader resourceLoader;
  
  @Bean
  public StreamsConfig kStreamsConfigs() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "ericUseCase2Streams");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, env.getProperty("kafka.bootstrap.servers"));
    //properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    //properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
    properties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
    
    // props from bootcamp starter examples - START
    properties.put("schema.registry.url",env.getProperty("schema.registry.url"));
    properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

    //properties.put("enable.auto.commit",env.getProperty("enable.auto.commit"));
    properties.put("session.timeout.ms",env.getProperty("session.timeout.ms"));
    properties.put("auto.offset.reset",env.getProperty("auto.offset.reset"));
    properties.put("fetch.max.wait.ms",env.getProperty("fetch.max.wait.ms"));
    properties.put("max.partition.fetch.bytes",env.getProperty("max.partition.fetch.bytes"));
    properties.put("max.poll.records",env.getProperty("max.poll.records"));

    properties.put("group.id",env.getProperty("group.id"));
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,org.apache.kafka.common.serialization.StringDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
    try {
        properties.put("client.id", InetAddress.getLocalHost().getHostName());
    } catch (Exception e) {
        logger.error("Could not set client.id - {}",e.getMessage());
    }


    properties.put("sasl.jaas.config", env.getProperty("sasl.jaas.config") );
    properties.put("sasl.mechanism", env.getProperty("sasl.mechanism") );
    properties.put("security.protocol", env.getProperty("security.protocol") );

    String writableDir=env.getProperty("writable.dir");
    String jaasFile=null;
    try {
        jaasFile= Util.writeJaasFile(new File(writableDir), env.getProperty("kafka.username"), env.getProperty("kafka.password"));
    }
    catch (Exception e) {
        String message="Error trying to write Jaas file - {}"+e.getMessage();
        logger.error(message);
        e.printStackTrace();
        throw new RuntimeException(message);
    }

    try {
        System.setProperty("java.security.auth.login.config",resourceLoader.getResource("file:/"+jaasFile).getURI().toString() );
    } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
    }
    //props from bootcamp starter examples - END
    
    
    return new StreamsConfig(properties);
  }

  @Bean
  public KafkaStreams kafkaStreams(StreamsConfig kStreamsConfigs) {
    
    // Always start with a KStreamBuilder to create a processing topology
    KStreamBuilder kStreamBuilder = new KStreamBuilder();
    
    // Define our input stream
    KStream<Integer, String> trxnStream = kStreamBuilder.stream("CARMELLA-Transactions");
    
    // TODO: Filter here
    
    // Write results back to another kafka topic
    trxnStream.to("CARMELLA-over-1000");
    
    KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, kStreamsConfigs);
    return kafkaStreams;
  }
}
