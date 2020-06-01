package watermark1

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId, ZoneOffset}

import org.apache.flink.api.scala._
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.commons.lang3.time.{DateFormatUtils, DateUtils}
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot
import org.apache.flink.api.java.typeutils.runtime.kryo.Serializers
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.beans.BeanProperty

/**
 * @Description nc -l 9000
 *              {"name":"1","datetime":"2020-06-01 15:00:06"}
 * @author ZhangTing
 * @create 2020-05-26 23:17
 **/
object Test1 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.getConfig.addDefaultKryoSerializer(classOf[LocalDateTime],classOf[LocalDateTimeSerializer])
    //便于测试，并行度设置为1env.getConfig().enableForceKryo();
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

}

class MyWaterMark extends AssignerWithPeriodicWatermarks[EventObj] {
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
    currentMaxTimestamp = Math.max(element.getTimestamp, currentMaxTimestamp)

    val id = Thread.currentThread().getId
    println("currentThreadId:" + id + ",key:" + element.name + ",eventTime:[" + element.datetime
      + "],currentMaxTimestamp:[" +
      DateFormatUtils.format(currentMaxTimestamp, "yyyy-MM-dd HH:mm:ss") + "],watermark:[" +
      DateFormatUtils.format(getCurrentWatermark().getTimestamp, "yyyy-MM-dd HH:mm:ss") + "]")

    element.getTimestamp
  }
}

class MyProcessWindowFunction extends ProcessWindowFunction[EventObj, String, String, TimeWindow] {

  override def process(key: String, context: Context, elements: Iterable[EventObj], out: Collector[String]): Unit = {
    val iterator = elements.iterator
    while (iterator.hasNext) {
      val value = iterator.next()
      println(value)
      //      println(value._1, DateUtils.getNewFormatDateString(new Date(value._2)))
      //      arr += value
    }

    out.collect(key + "," +
      DateFormatUtils.format(context.window.getStart, "yyyy-MM-dd HH:mm:ss") + "," +
      DateFormatUtils.format(context.window.getEnd, "yyyy-MM-dd HH:mm:ss"))
    //    println(arr)
    //    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    //    val timeWindow = context.window
    //    out.collect(key + "," + arr.size + "," + simpleDateFormat.format(arr.head._2) + "," + simpleDateFormat.format(arr.last._2) + "," + simpleDateFormat.format(timeWindow.getStart) + "," + simpleDateFormat.format(timeWindow.getEnd))
  }

}

class EventObj {

  @BeanProperty var name: String = _
  @BeanProperty var datetime: String = _

  def getTimestamp = {
    DateUtils.parseDate(datetime, "yyyy-MM-dd HH:mm:ss").getTime()
  }

  override def toString: String = JSON.toJSONString(this, false)
}
