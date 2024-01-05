package org.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Main {

    public static void main(String[] args) throws IOException {
        // Configuración del productor
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> System.out.println("Deteniendo la aplicación...")));
        // Crear el productor fuera del bucle
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            int i = 0;
            int maxMessages = 10000; // Número máximo de mensajes para enviar

            while (i < maxMessages) {
                int bssMap = (int) (Math.random() * 128);
                int causa_interna = (int) (Math.random() * 19);
                int ranap = (int) (Math.random() * 256);

                int nrn = (int) (Math.random() * 970213) + 10000;
                String dato4S = String.format("%06d", nrn);
                nrn = Integer.parseInt(dato4S);

                List<Integer> numbers = extractFirstColumn("src/main/resources/operadores.txt");

                int operadores = numbers.get((int) (Math.random() * numbers.size()));
                int test = (int) (Math.random() * 4) + 1;

                String message = bssMap + ";" + causa_interna + ";" + ranap + ";" + nrn + ";" + operadores + ";" + test;

                // Enviar mensaje
                producer.send(new ProducerRecord<>("tfg", message));
                i++;
            }
        }
    }

    public static List<Integer> extractFirstColumn(String filePath) throws IOException {
        List<Integer> numbers = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(";");
                if (parts.length > 0) {
                    try {
                        numbers.add(Integer.parseInt(parts[0]));
                    } catch (NumberFormatException e) {
                        // Ignorar líneas no numéricas
                    }
                }
            }
        }
        return numbers;
    }
}
