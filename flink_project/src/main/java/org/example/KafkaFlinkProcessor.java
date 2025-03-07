package org.example;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
//import com.fasterxml.jackson.databind.node.ObjectNode;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import java.util.*;

public class KafkaFlinkProcessor {

    public static void main(String[] args) throws Exception {

// Initialize Flink Streaming Environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// first source logic(from source to sinking)

        // Kafka Source (Reading from one Kafka tpoic)
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092") // Replace with your Kafka broker
                .setTopics("second_topic") // Replace with your topic name
                .setGroupId("flink-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Create a DataStream from Kafka Source
        DataStream<String> stream = env.fromSource(source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source");


        // Transformation Logic (e.g., Uppercasing the Data)
        DataStream<String> transformedStream = stream.map(String::toUpperCase);

        // Kafka Sink (Writing to Another Kafka Topic)
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092") // Replace with your destination Kafka broker
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("output_topic") // Replace with your topic name
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build(); // REMOVE setDeliveryGuarantee()

        // Write Data to Kafka Sink
        transformedStream.sinkTo(sink);


// kafka seconds's source, reading the data as the json

        KafkaSource<ObjectNode> jsonSource = KafkaSource.<ObjectNode>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("third_topic")
                .setGroupId("flink-json-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(TypeInformation.of(ObjectNode.class)))
                .build();


        DataStream<ObjectNode> jsonStream = env.fromSource(
                jsonSource, WatermarkStrategy.noWatermarks(), "Kafka JSON Source");

        DataStream<Double> revenueSum = jsonStream
                .map(json -> json.get("revenue").asDouble())
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce(Double::sum);

        DataStream<String> revenueSumJson = revenueSum.map(sum -> new ObjectMapper().writeValueAsString(sum));


        // 4️⃣ Kafka Sink: Write processed data to Kafka
        KafkaSink<String> sink2 = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("output_agg_topic")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        revenueSumJson.sinkTo(sink2);


        // Execute the Flink Job
        env.execute("Kafka Flink Processor");
    }
}