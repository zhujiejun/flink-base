package com.zhujiejun.flink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
    def main(args: Array[String]): Unit = {
        val param = ParameterTool.fromArgs(args)
        val host = param.get("host")
        val port = param.get("port")

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val dataStream = env.socketTextStream(host, port.toInt)
        val wordCountDataStream = dataStream.flatMap(_.split(" "))
            .filter(_.nonEmpty)
            .map((_, 1))
            .keyBy(0)
            .sum(1)
        wordCountDataStream.print("|").setParallelism(2)
        env.execute("stream word count job")
    }
}
