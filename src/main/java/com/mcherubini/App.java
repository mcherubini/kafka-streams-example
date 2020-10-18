package com.mcherubini;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public class App
{
    public static void main( String[] args )
    {
        final Logger logger = LoggerFactory.getLogger(App.class);
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Set how to serialize key/value pairs
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.setProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081/");

        KafkaProducer<String,Weather> producer = new KafkaProducer<>(props);
        Weather weather = Weather.newBuilder()
                .setStation("Spring")
                .setTemp(20)
                .setTime(15102020)
                .build();

        ProducerRecord<String,Weather> record = new ProducerRecord<>("TEST_INPUT_TOPIC",weather);
        producer.send(record);

        weather.setStation("Autum");
        weather.setTemp(10);

        producer.send(record);
        producer.flush();
        logger.info("sending message: " + record.toString());

        //kafka streams
        Properties streamsConfig = new Properties();
        // The name must be unique on the Kafka cluster
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-example");
        // Brokers
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Weather> stream = builder.stream("TEST_INPUT_TOPIC");

        stream.to("TEST_OUTPUT_TOPIC");

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);

        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
