package languagelearning.chapter10

/**
 * @Description
 * @author ZhangTing
 * @create 2020-06-03 23:49
 **/
object Test1 extends App {

  class Cat {
    val dangerous = false
  }

  class Tiger(
               override val dangerous: Boolean,
               private var age: Int
             ) extends Cat
}

