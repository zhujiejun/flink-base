package com.zhujiejun.flink

import org.apache.flink.streaming.api.scala._

object StreamWordCount {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val dataStream = env.socketTextStream("localhost", 18888)
        val wordCountDataStream = dataStream.flatMap(_.split(" "))
            .filter(_.nonEmpty)
            .map((_, 1))
            .keyBy(0)
            .sum(1)
        wordCountDataStream.print("|").setParallelism(2)
        env.execute("stream word count job")
    }
}
