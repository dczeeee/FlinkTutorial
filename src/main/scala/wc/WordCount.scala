package wc

import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 读取数据
    val inputPath = "/home/wanghy/Workspace/IDEA/FlinkTutorial/src/main/resources/hello.txt"
    val inputData = env.readTextFile(inputPath)

    // wordCount
    val wordCountDataSet = inputData.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    wordCountDataSet.print()

  }
}
