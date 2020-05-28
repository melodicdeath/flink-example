package watermark1

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId, ZoneOffset}

import org.apache.flink.api.scala._
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

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

  val lateText = new OutputTag[EventObj]("late_data")

  val value = input.filter(!_.isEmpty)
    .flatMap((in: String, out: Collector[EventObj]) => {
      out.collect(JSON.parseObject(in, classOf[EventObj]))
    })
    .assignTimestampsAndWatermarks(new MyWaterMark)
    //    .map(x => (x.name, x.datetime, x.timestamp, 1L))
    .keyBy(_.name)
    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    .sideOutputLateData(lateText)

    //.sum(2)
    .process(new MyProcessWindowFunction)

  value.getSideOutput(lateText).map(x => {
    "延迟数据|name:" + x.name + "|datetime:" + x.datetime
  }).print()

  value.print()

  env.execute("watermark test")
}

class MyWaterMark extends AssignerWithPeriodicWatermarks[EventObj] {
  val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  val maxOutOfOrderness = 0L // 3.0 seconds
  var currentMaxTimestamp = 0L

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
    println("currentThreadId:" + id + ",key:" + element.name + ",eventTime:[" + element.datetime
      + "],currentMaxTimestamp:[" + LocalDateTime.ofInstant(Instant.ofEpochMilli(currentMaxTimestamp),
      ZoneId.systemDefault()).format(dateTimeFormatter)) + "],watermark:[" +
      LocalDateTime.ofInstant(Instant.ofEpochMilli(getCurrentWatermark().getTimestamp),
        ZoneId.systemDefault()).format(dateTimeFormatter) + "]"

    element.timestamp
  }
}

class MyProcessWindowFunction extends ProcessWindowFunction[EventObj, String, String, TimeWindow] {
  val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  override def process(key: String, context: Context, elements: Iterable[EventObj], out: Collector[String]): Unit = {
    val iterator = elements.iterator
    while (iterator.hasNext) {
      val value = iterator.next()
      println(value)
      //      println(value._1, DateUtils.getNewFormatDateString(new Date(value._2)))
      //      arr += value
    }

        out.collect(key + "," +
          LocalDateTime.ofInstant(Instant.ofEpochMilli(context.window.getStart),
            ZoneId.systemDefault()).format(dateTimeFormatter) + "," +
          LocalDateTime.ofInstant(Instant.ofEpochMilli(context.window.getEnd),
            ZoneId.systemDefault()).format(dateTimeFormatter))
    //    println(arr)
    //    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    //    val timeWindow = context.window
    //    out.collect(key + "," + arr.size + "," + simpleDateFormat.format(arr.head._2) + "," + simpleDateFormat.format(arr.last._2) + "," + simpleDateFormat.format(timeWindow.getStart) + "," + simpleDateFormat.format(timeWindow.getEnd))
  }

}

class EventObj(val name: String, val datetime: String) {
  def timestamp: Long = LocalDateTime.parse(this.datetime,
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    .toInstant(ZoneOffset.ofHours(8)).toEpochMilli;

    override def toString: String = JSON.toJSONString(this,SerializerFeature.PrettyFormat)
}
