package watermark1

import java.text.SimpleDateFormat

import org.apache.flink.api.scala._
import com.alibaba.fastjson.JSON
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer

/**
 * @Description
 * @author ZhangTing
 * @create 2020-05-26 23:17
 **/
object Test1 {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  //便于测试，并行度设置为1
  env.setParallelism(1)

  //env.getConfig.setAutoWatermarkInterval(9000)

  //设置为事件时间
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  //设置source 本地socket
  val input: DataStream[String] = env.socketTextStream("localhost", 9000)

  val lateText = new OutputTag[(String, String, Long, Long)]("late_data")

  val value = input.filter(!_.isEmpty)
    .flatMap((in: String, out: Collector[EventObj]) => {
      out.collect(JSON.parseObject(in, classOf[EventObj]))
    })
    .assignTimestampsAndWatermarks(new MyWaterMark)
    .map(x => (x.name, x.datetime, x.timestamp, 1L))
    .keyBy(_._1)
    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    .sideOutputLateData(lateText)

    //.sum(2)
    .process(new MyProcessWindowFunction)

  value.getSideOutput(lateText).map(x => {
    "延迟数据|name:" + x._1 + "|datetime:" + x._2
  }).print()

  value.print()

  env.execute("watermark test")
}

class MyWaterMark extends AssignerWithPeriodicWatermarks[EventObj] {

  val maxOutOfOrderness = 10000L // 3.0 seconds
  var currentMaxTimestamp = 0L

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  /**
   * 用于生成新的水位线，新的水位线只有大于当前水位线才是有效的
   *
   * 通过生成水印的间隔（每n毫秒）定义 ExecutionConfig.setAutoWatermarkInterval(...)。
   * getCurrentWatermark()每次调用分配器的方法，如果返回的水印非空并且大于先前的水印，则将发出新的水印。
   *
   * @return
   */
  override def getCurrentWatermark: Watermark = {
    new Watermark(this.currentMaxTimestamp - this.maxOutOfOrderness)
  }

  /**
   * 用于从消息中提取事件时间
   *
   * @param element                  EventObj
   * @param previousElementTimestamp Long
   * @return
   */
  override def extractTimestamp(element: EventObj, previousElementTimestamp: Long): Long = {
    currentMaxTimestamp = Math.max(element.timestamp, currentMaxTimestamp)

    val id = Thread.currentThread().getId
    println("currentThreadId:" + id + ",key:" + element.name + ",eventTime:[" + element.datetime + "],currentMaxTimestamp:[" + sdf.format(currentMaxTimestamp) + "],watermark:[" + sdf.format(getCurrentWatermark().getTimestamp) + "]")

    element.timestamp
  }
}

class MyProcessWindowFunction extends ProcessWindowFunction[EventObj, String, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[EventObj], out: Collector[String]): Unit = {

//    val arr = ArrayBuffer[(String, Long)]()
    val iterator = elements.iterator
    while (iterator.hasNext) {
      val value = iterator.next()

//      println(value._1, DateUtils.getNewFormatDateString(new Date(value._2)))
//      arr += value
    }
//    println(arr)
//    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
//    val timeWindow = context.window
//    out.collect(key + "," + arr.size + "," + format.format(arr.head._2) + "," + format.format(arr.last._2) + "," + format.format(timeWindow.getStart) + "," + format.format(timeWindow.getEnd))
  }

}

class EventObj(val name: String, val timestamp: Long, val datetime: String){

}
