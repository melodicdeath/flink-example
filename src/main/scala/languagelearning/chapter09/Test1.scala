package languagelearning.chapter09

/**
 * @Description
 * @author ZhangTing
 * @create 2020-06-03 14:55
 **/
object Test1 extends App {
  def twice(op: Double => Double, x: Double) = op(op(x))

  println(twice(x => x + 1, 5))
  println(twice(_ + 1, 5))

}
