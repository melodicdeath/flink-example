package flink.apachedoc

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector


/**
 * @Description
 * @author ZhangTing
 * @create 2020-06-02 16:37
 **/
object ExampleCountWindowAverage2 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    env.fromCollection(List(
      (1L, 3L),
      (1L, 5L),
      (1L, 7L),
      (1L, 4L),
      (1L, 2L),
      (2L, 3L),
      (2L, 5L),
      (2L, 7L),
      (2L, 4L),
      (2L, 2L)
    )).keyBy(_._1)
      .flatMap(new CountWindowAverage2())
      .setParallelism(2)
      .print()
    // the printed output will be (1,4) and (1,5)
    env.execute("ExampleManagedState")
  }
}

class CountWindowAverage2 extends RichFlatMapFunction[(Long, Long), (Long, Long)] {

  private var sum: ValueState[(Long, Long)] = _

  private var i: Int = 0

  override def flatMap(input: (Long, Long), out: Collector[(Long, Long)]): Unit = {

    i = i + 1;
    println("[" + Thread.currentThread().getId + "]" + input._1 + "-" + i)

    // access the state value
    val tmpCurrentSum = sum.value

    // If it hasn't been used before, it will be null
    val currentSum = if (tmpCurrentSum != null) {
      tmpCurrentSum
    } else {
      (0L, 0L)
    }

    // update the count
    val newSum = (currentSum._1 + 1, currentSum._2 + input._2)

    // update the state
    sum.update(newSum)

    // if the count reaches 2, emit the average and clear the state
    if (newSum._1 >= 2) {
      out.collect((input._1, newSum._2 / newSum._1))
      sum.clear()
    }
  }

  override def open(parameters: Configuration): Unit = {
    sum = getRuntimeContext.getState(
      new ValueStateDescriptor[(Long, Long)]("average", createTypeInformation[(Long, Long)])
    )
  }
}
