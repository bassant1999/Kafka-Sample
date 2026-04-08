package com.kafka.example.Kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class RevenueTracker {
    public static void main(String[] args) {
    	// 1. Configuration
    	Properties props = new Properties();
    	props.put(StreamsConfig.APPLICATION_ID_CONFIG, "merchant-revenue-v1");
    	props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    	props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    	props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    	// Use ConsumerConfig for the offset reset
    	props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        // 2. The Logic: Read -> Count -> Print
        KStream<String, String> transactionStream = builder.stream("transactions");

        KTable<String, Long> merchantCounts = transactionStream
            .groupByKey() // Uses the Merchant name sent by Python as the Key
            .count(Materialized.as("merchant-inventory-store"));

        // 3. The "Print" Logic (Side Effect)
        merchantCounts.toStream().foreach((merchant, total) -> {
            System.out.println("📊 [LIVE UPDATE] Merchant: " + merchant + " | Total Transactions: " + total);
        });

        // 4. Sink to a new topic (For downstream apps or ACE)
        merchantCounts.toStream().to("merchant-totals", Produced.with(Serdes.String(), Serdes.Long()));

        // 5. Start the Engine
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        
        System.out.println("🚀 Kafka Streams App starting...");
        streams.start();
        System.out.println("🚀 Kafka Streams App Started! Current State: " + streams.state());
        // Cleanup on exit
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
