package wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")

    // 创建流处理执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 接收一个text文本流
    val dataStream = env.socketTextStream(host, port)

    // 对每条数据处理
    val wordCountDataStream = dataStream.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    wordCountDataStream.print().setParallelism(2)

    // 启动executor
    env.execute("stream word count job")
  }
}
