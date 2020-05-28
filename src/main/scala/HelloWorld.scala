import java.text.SimpleDateFormat
import java.time.ZoneId
import java.time.format.DateTimeFormatter

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.BeanProperty

/**
 * @Description
 * @author ZhangTing
 * @create 2020-05-17 01:40
 **/
object HelloWorld {

  private def twice(op: Double => Double, x: Double) = op(op(x))

  println(twice(_ + 2, 5))

  var assertionEnable = false

  def myAssert(predicat: () => Boolean) =
    if (assertionEnable && !predicat())
      throw new AssertionError

  //不会立即计算，会先创建函数，再调用apply方法对5/0求值。短路判断之后不会抛出异常
  def myAssert2(predicat: => Boolean) =
    if (assertionEnable && !predicat)
      throw new AssertionError

  //会直接抛出异常
  def myAssert1(predicat: Boolean) =
    if (assertionEnable && !predicat)
      throw new AssertionError

  val x = 5
  myAssert(() => 5 / 0 == 0)
  myAssert2(5 / 0 == 0)
  //  myAssert1(5 / 0 == 0)

  List(1, 2, 3, 4).filter(_ > 0)

  import java.time.Instant
  import java.time.LocalDate
  import java.time.LocalDateTime
  import java.time.ZoneOffset

  val longtimestamp: Long = System.currentTimeMillis
  val localDate: LocalDate = Instant.ofEpochMilli(longtimestamp).atZone(ZoneOffset.ofHours(8)).toLocalDate
  val localDateTime: LocalDateTime = Instant.ofEpochMilli(longtimestamp).atZone(ZoneOffset.ofHours(8)).toLocalDateTime
  println(localDateTime)

  println(LocalDateTime.ofInstant(Instant.ofEpochMilli(longtimestamp), ZoneId.systemDefault()).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))


  val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  println(simpleDateFormat.format(1590683982620L))

  case class a(val a: String, val b: Int)

  val a1: a = a("1", 1)
  println(a1.a)

  val list: List[Int] = List(1, 2, 3)
  val cnm = JSON.toJSONString(a1, SerializerFeature.WriteMapNullValue)
  println(cnm)

  def main(args: Array[String]): Unit = {

  }
}
