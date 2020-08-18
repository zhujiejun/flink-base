package com.zhujiejun.flink.sink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

object FlinkSink {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "node101:9092")
        properties.setProperty("group.id", "flink-group")
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("auto.offset.reset", "latest")

        val sourceDS = env.addSource(new FlinkKafkaConsumer[String]("sensor", new SimpleStringSchema(), properties))
        val sinkDS = sourceDS.map {
            data => {
                val dataArray = data.split(",")
                dataArray(0).trim.concat("-").concat(dataArray(1).trim).trim
            }
        }

        sinkDS.addSink(new FlinkKafkaProducer[String]("node101:9092", "sensorSink", new SimpleStringSchema()))
        //sinkDS.addSink(new FlinkKafkaProducer[String]("sensorSink",, properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
        //sinkDS.print("flink sink")
        env.execute("flink sink")
    }
}
