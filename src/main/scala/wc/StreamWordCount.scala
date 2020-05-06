package wc

import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    // 创建流处理执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 接收一个text文本流
    val dataStream = env.socketTextStream("127.0.0.1", 7788)

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
