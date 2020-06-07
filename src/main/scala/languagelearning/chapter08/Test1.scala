package languagelearning.chapter08

/**
 * @Description
 * @author ZhangTing
 * @create 2020-06-03 09:30
 **/
object Test1 extends App {
  println("--------1----------")
  var f = (_: Int) + (_: Int)
  println(f(1, 2))
  val a = f _
  println(a)

  def sum(a: Int, b: Int) = a + b

  val b = sum _
  println(b(1, 2))

  val b2 = sum(1, _: Int)
  println(b2(2))

  println("--------2----------")

  /*闭包是一种特殊的函数值，闭包中封闭或绑定了在另一个作用域或上下文中定义的变量。
  特征：
  1.闭包中有一个特殊的函数；
  2.存在一个封闭的作用域，函数就在这个封闭的作用域中；
  3.在封闭作用域中存在一个函数作用域之外的变量（即自由变量）；
  4.闭包函数绑定了这个自由变量。*/

  var more = 1
  val addMore = (x: Int) => x + more
  println(addMore(10))

  more = 9999
  println(addMore(10))

  def makeIncreaser(more: Int) = (x: Int) => x + more

  def nomalIncreaser(x: Int, more: Int) = x + more

  val inc1 = makeIncreaser(1)
  val inc9999 = makeIncreaser(9999)
  val incNormal = nomalIncreaser(1,10)

  println(inc1(10))
  println(inc9999(10))
  println(incNormal)


  def foo(): Int => Int = {
    val i = 1

    def bar(num: Int) = {
      i + num
    }

    bar
  }

  val func = foo()
  println(func(2))

  println("--------3----------")
  def boom(x: Int): Int =
    if (x == 0) throw new Exception("boom!")
    else boom(x - 1) + 1

//  boom(3)

  def bang(x: Int): Int =
    if (x == 0) throw new Exception("bang!")
    else bang(x - 1)
//  bang(5)

  //闭包简化代码
  private def filesHere = (new java.io.File(".")).listFiles

  //-----1-----
  //  def filesEnding(query: String) =
  //    filesMatching(query, (fileName, query) => fileName.endsWith(query))
  //  //    filesMatching(query, _.endsWith(_))
  //
  //  def filesContaining(query: String) =
  //    filesMatching(query, _.contains(_))
  //
  //  def filesRegex(query: String) =
  //    filesMatching(query, _.matches(_))

  //  def filesMatching(query: String,
  //                    matcher: (String, String) => Boolean) = {
  //
  //    for (file <- filesHere; if matcher(file.getName, query))
  //      yield file
  //  }
  //----------

  def filesEnding(query: String) =
    filesMatching(_.endsWith(query))

  def filesContaining(query: String) =
    filesMatching(_.contains(query))

  def filesRegex(query: String) =
    filesMatching(_.matches(query))

  def filesMatching(matcher: String => Boolean) = {
    for (file <- filesHere; if matcher(file.getName))
      yield file
  }
}


