package com.zhujiejun.flink.spring

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

object SpringBootHandler {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "node101:9092, node102:9092, node103:9092")
        properties.setProperty("group.id", "spring-boot-group")
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("auto.offset.reset", "latest")
        val sourceStream = env.addSource(new FlinkKafkaConsumer[String]("spring-boot-producer", new SimpleStringSchema(), properties))
        sourceStream.print("spring-boot-consumer:")

        val sinkStream = sourceStream.map {
            data => {
                val dataArray = data.split(",")
                dataArray(0).trim.concat("-").concat(dataArray(1).trim).trim
            }
        }

        sinkStream.addSink(new FlinkKafkaProducer[String]("node101:9092, node102:9092, node103:9092", "spring-boot-consumer", new SimpleStringSchema()))
        sinkStream.print("spring-boot-producer:")

        env.execute("spring-boot-handler")
    }
}
