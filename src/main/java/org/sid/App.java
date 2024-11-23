package org.sid;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public class App {
    public static void main(String[] args) {
        // 1. Configuration Kafka-Streams
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-data-analysis");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        // 2. Construire le flux Kafka
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // 3. Lire les données météorologiques depuis le topic Kafka 'weather-data'
        KStream<String, String> weatherData = streamsBuilder.stream("weather-data");

        //4. Filtrer les données de température élevée
        KStream<String, String> highTemperature = weatherData.filter((key, value) -> {
            String[] data = value.split(",");
            double temperature = Double.parseDouble(data[1]);
            return temperature > 30;
        });

        // 5. Convertir les températures en Fahrenheit
        KStream<String, String> fahrenheitTemperature = highTemperature.mapValues(value -> {
            String[] parts = value.split(",");
            String station = parts[0];
            double celsius = Double.parseDouble(parts[1]);
            double fahrenheit = (celsius * 9 / 5) + 32;
            String humidity = parts[2];
            return station + "," + fahrenheit + "," + humidity;
        });

        // 6. Grouper les données par station
        KGroupedStream<String,String> groupedByStation = fahrenheitTemperature.groupBy(
                (key, value) -> value.split(",")[0],
                Grouped.with(Serdes.String(), Serdes.String())
        );

        // 7. Somme des températures et de taux d'humidité pour chaque station.
        KTable<String,String> sumParameters = groupedByStation.aggregate(
                () -> "0,0,0", // Initialisation : sommeTempérature, sommeHumidité, nombreRelevés
                (station, newValue, aggValue) -> {
                    String[] newParts = newValue.split(",");
                    String[] aggParts = aggValue.split(",");

                    double newTemp = Double.parseDouble(newParts[1]);
                    double newHumidity = Double.parseDouble(newParts[2]);
                    double sumTemp = Double.parseDouble(aggParts[0]) + newTemp;
                    double sumHumidity = Double.parseDouble(aggParts[1]) + newHumidity;
                    long count = Long.parseLong(aggParts[2]) + 1;

                    return sumTemp + "," + sumHumidity + "," + count;
                },
                Materialized.with(Serdes.String(), Serdes.String())
        );

        // 8. Moyenne des températures et de taux d'humidité pour chaque station.
        KTable<String, String> averageParameters = sumParameters.mapValues((key, value) ->{
             String[] data = value.split(",");

             double count = Double.parseDouble(data[2]);
             double avgTemperature = Math.round((Double.parseDouble(data[0]) / count) * 100.0) / 100.0;
             double avgHumidity = Math.round((Double.parseDouble(data[1]) / count) * 100.0) / 100.0;

            return key + ": Température moyenne = " + avgTemperature + "°F, Humidité moyenne = " + avgHumidity + "%";
        });

        // 9. Publiez les résultats dans le topic Kafka 'station-averages'.
        averageParameters.toStream().to("station-averages", Produced.with(Serdes.String(), Serdes.String()));

        // 10. Démarrer Kafka-Streams
        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties);

        // Arret Propre
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        streams.start();
    }
}

