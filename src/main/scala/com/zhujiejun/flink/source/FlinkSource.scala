package com.zhujiejun.flink.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

case class SensorReding(id: String, timestamp: Long, temperature: Double)

object FlinkSource {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        //1.Collection
        /*val stream1 = env.fromCollection(List(
            SensorReding("semsor_1", 1547718191, 35.47241),
            SensorReding("semsor_2", 1547718192, 35.47242),
            SensorReding("semsor_3", 1547718193, 35.47243),
            SensorReding("semsor_4", 1547718194, 35.47244),
            SensorReding("semsor_5", 1547718195, 35.47245)
        ))*/
        //stream1.print("stream1").setParallelism(1)

        //println("\n\n\n")

        //2.File
        //val stream2 = env.readTextFile("/opt/workspace/scala/flink-base/.gitignore")
        //stream2.print("stream2").setParallelism(1)

        //3.Kafka
        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "node101:9092")
        properties.setProperty("group.id", "flink-group")
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("auto.offset.reset", "latest")

        val stream3 = env.addSource(new FlinkKafkaConsumer[String]("sensor", new SimpleStringSchema(), properties))
        stream3.print("stream3").setParallelism(1)

        env.execute("flink source")
    }
}
