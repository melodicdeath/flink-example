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
  myAssert1(5 / 0 == 0)

  List(1, 2, 3, 4).filter(_ > 0)
}
