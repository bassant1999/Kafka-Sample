package com.kafka.example.Kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;

public class TransactionProducer {
    public static void main(String[] args) {
        // 1. Setup Configuration
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ObjectMapper objectMapper = new ObjectMapper();
        
        List<String> merchants = Arrays.asList("Starbucks", "Jumia", "Vodafone", "TotalEnergies", "Zara");
        Random random = new Random();

        System.out.println("🚀 Sending transactions...");

        // 2. Graceful Shutdown Hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Stopping Producer...");
            producer.flush();
            producer.close();
        }));

        try {
            while (true) {
                String merchant = merchants.get(random.nextInt(merchants.size()));
                
                // Create Data Map (Equivalent to Python Dict)
                Map<String, Object> data = new HashMap<>();
                data.put("merchant", merchant);
                data.put("amount", random.nextInt(491) + 10); // random.randint(10, 500)
                data.put("timestamp", System.currentTimeMillis() / 1000.0);

                // Serialize to JSON String
                String jsonValue = objectMapper.writeValueAsString(data);

                // 3. Send Record (Topic, Key, Value)
                ProducerRecord<String, String> record = new ProducerRecord<>("transactions", merchant, jsonValue);
                producer.send(record);

                System.out.println("✅ Sent " + merchant);
                Thread.sleep(500);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
