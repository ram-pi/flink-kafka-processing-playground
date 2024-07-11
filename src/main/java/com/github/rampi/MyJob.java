package com.github.rampi;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public class MyJob {

  static final String INPUT = "input";
  static final String OUTPUT_1 = "output-1";
  static final String OUTPUT_2 = "output-2";
  static final String OUTPUT_3 = "output-3";
  static final String BOOTSTRAP_SERVERS = "localhost:9091";

  public static void main(String[] args) {
    runJob();
  }

  @SneakyThrows
  static void runJob() {

    // define source
    KafkaSource<String> source =
        KafkaSource.<String>builder()
            .setBootstrapServers(BOOTSTRAP_SERVERS)
            .setTopics(INPUT)
            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
            .setProperty("group.id", "my-flink-reader")
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment().enableCheckpointing(60000);

    // define workflows
    //    defineWorkflow(env, source);
    //    defineWorkflow2(env, source);
    defineWorkflow3(env, source);
    //    simplePrint(env, source);

    // run
    env.execute("MyJob");
  }

  static void defineWorkflow(StreamExecutionEnvironment env, Source<String, ?, ?> source) {

    final DataStreamSource<String> kafka =
        env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), "Kafka");

    // define sink
    KafkaSink<String> sink =
        KafkaSink.<String>builder()
            .setBootstrapServers(BOOTSTRAP_SERVERS)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(OUTPUT_1)
                    .setValueSerializationSchema(new SimpleStringSchema())
                    .build())
            .setTransactionalIdPrefix("trx-sink-1")
            .setProperty("enable.idempotence", "true")
            .setProperty("transaction.timeout.ms", "60000")
            .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
            .build();

    kafka.map(String::toUpperCase).sinkTo(sink).name("sink-to-kafka-1");
  }

  static void simplePrint(StreamExecutionEnvironment env, Source<String, ?, ?> source) {
    env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), "Kafka").print();
  }

  static void defineWorkflow2(StreamExecutionEnvironment env, Source<String, ?, ?> source) {
    final DataStreamSource<String> kafka =
        env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), "Kafka");

    // define sink
    KafkaSink<String> sink =
        KafkaSink.<String>builder()
            .setBootstrapServers(BOOTSTRAP_SERVERS)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(OUTPUT_2)
                    .setValueSerializationSchema(new SimpleStringSchema())
                    .build())
            .setTransactionalIdPrefix("trx-sink-2")
            .setProperty("enable.idempotence", "true")
            .setProperty("transaction.timeout.ms", "60000")
            .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
            .build();

    kafka.filter(s -> s.length() > 4).sinkTo(sink).name("sink-to-kafka-2");
  }

  static void defineWorkflow3(StreamExecutionEnvironment env, Source<String, ?, ?> source) {
    final DataStreamSource<String> kafka =
        env.fromSource(
            source,
            WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withIdleness(Duration.ofSeconds(5)),
            "Kafka");

    // define sink
    JsonSerializationSchema<AggregatedEvent> jsonFormat = new JsonSerializationSchema<>();
    KafkaSink<AggregatedEvent> sink =
        KafkaSink.<AggregatedEvent>builder()
            .setBootstrapServers(BOOTSTRAP_SERVERS)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(OUTPUT_3)
                    .setValueSerializationSchema(jsonFormat)
                    .build())
            .setProperty("enable.idempotence", "true")
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

    // counting elements with size > 4 in a 60 seconds window
    SingleOutputStreamOperator<AggregatedEvent> soso =
        kafka
            .filter(s -> s.length() > 4)
            //        .keyBy(s -> s.length() % 4)
            //        .window(TumblingProcessingTimeWindows.of(Time.of(5, TimeUnit.SECONDS)))
            .windowAll(TumblingEventTimeWindows.of(Time.of(60, TimeUnit.SECONDS)))
            .process(
                new ProcessAllWindowFunction<String, AggregatedEvent, TimeWindow>() {
                  @Override
                  public void process(
                      ProcessAllWindowFunction<String, AggregatedEvent, TimeWindow>.Context context,
                      Iterable<String> iterable,
                      Collector<AggregatedEvent> collector)
                      throws Exception {
                    int count = 0;
                    for (String i : iterable) {
                      count++;
                    }
                    collector.collect(
                        new AggregatedEvent(
                            context.window().getStart(), context.window().getEnd(), count));
                  }
                })
            .name("sink-to-kafka-3");

    // sink
    soso.sinkTo(sink);
  }
}
