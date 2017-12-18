package com.eric.stream;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Resource;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
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

import com.eric.serde.TransactionSerdes;
import com.rbc.cloud.hackathon.data.Transactions;

//import org.apache.kafka.streams.StreamsBuilder;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

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
    
    // -- props from bootcamp starter examples - START
    properties.put("schema.registry.url",env.getProperty("schema.registry.url"));
    properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

    //properties.put("enable.auto.commit",env.getProperty("enable.auto.commit"));
    properties.put("session.timeout.ms",env.getProperty("session.timeout.ms"));
    properties.put("auto.offset.reset",env.getProperty("auto.offset.reset"));
    properties.put("fetch.max.wait.ms",env.getProperty("fetch.max.wait.ms"));
    properties.put("max.partition.fetch.bytes",env.getProperty("max.partition.fetch.bytes"));
    properties.put("max.poll.records",env.getProperty("max.poll.records"));

    properties.put("group.id",env.getProperty("group.id"));
    //properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,org.apache.kafka.common.serialization.StringDeserializer.class);
    
    // This way working
    //properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
    //properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
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
    // -- props from bootcamp starter examples - END
    
    
    return new StreamsConfig(properties);
  }

  @Bean
  public KafkaStreams kafkaStreams(StreamsConfig kStreamsConfigs) {
    
    // Minimum transaction amount that we are keeping.  Throw away trxns less than this amount.
    BigInteger MIN_TRXN_AMT = new BigInteger("30000");
    
    // Serial/Deserializers
    // When you want to override serdes explicitly/selectively
    final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
        env.getProperty("schema.registry.url"));
    final Serde<Transactions> valueSpecificAvroSerde = new SpecificAvroSerde<>();
    valueSpecificAvroSerde.configure(serdeConfig, false); // `false` for record values
    
    // Always start with a KStreamBuilder to create a processing topology
    KStreamBuilder kStreamBuilder = new KStreamBuilder();
    //StreamsBuilder kStreamBuilder = new StreamsBuilder();
    
    // Define our input stream
    //KStream<String, Transactions> trxnStream = kStreamBuilder.stream("CARMELLA-Transactions", Consumed.with(Serdes.String(), valueSpecificAvroSerde));
    KStream<String, Transactions> trxnStream = kStreamBuilder.stream(Serdes.String(), valueSpecificAvroSerde, "CARMELLA-Transactions");
    
    /*
    // Debug
    trxnStream.foreach(new ForeachAction<String, Transactions>() {
       public void apply(String key, Transactions value) {
          System.out.println(key + ":[" + value+"]");
      }
   });
   */
    
    // Filter: only keep trxn >= $1000
    KStream<String, Transactions> over1000 = trxnStream.filter((key, value) -> StringUtils.isNumeric(value.getTransactionAmount()))
                                                       .filter((key, value) -> MIN_TRXN_AMT.compareTo(
                                                           new BigInteger(value.getTransactionAmount().toString())) < 0); 
    
    // Write results back to another kafka topic
    over1000.to("CARMELLA-over-1000");
    
    // Debug
    over1000.foreach(new ForeachAction<String, Transactions>() {
      public void apply(String key, Transactions value) {
         System.out.println(key + ":[" + value+"]");
     }
  });
    
    KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, kStreamsConfigs);
    return kafkaStreams;
  }
}
