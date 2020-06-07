package languagelearning.chapter16

/**
 * @Description
 * @author ZhangTing
 * @create 2020-06-07 00:12
 **/
object Test1 extends App {

  def isort(xs: List[Int]): List[Int] =
    if (xs.isEmpty) Nil
    else insert(xs.head, isort(xs.tail))

  def insert(x: Int, xs: List[Int]): List[Int] =
    if (xs.isEmpty || x <= xs.head) x :: xs
    else xs.head :: insert(x, xs.tail)

  //insert(3,insert(2,insert(1,nil)))

  //insert(2,[1]) =>1::insert(2,nil)=>[1,2]

  //insert(3,[1,2]) =>insert(1,insert(3,[2]) => insert(1,insert(2::insert(3,nil))) =>(1,2,3)


  val foo = List(3, 2, 1)
  println(isort(foo))
  println(foo.head)

  println(foo.toString)

  def flattenLeft[T](xss: List[List[T]]) =
    (List[T]() /: xss) (_ ::: _)

  def flattenRight[T](xss: List[List[T]]) =
    (xss :\ List[T]()) (_ ::: _)

  val list1 = List(1, 2, 3)
  val list2 = List(4, 5, 6)

  val list3 = List(list1, list2)

  println(flattenRight(list3))

  println(List.tabulate(5)(n => n * n))
  List.tabulate(5)(_ + 1).foreach(print)
  println()

  val multiplication = List.tabulate(5, 5)((i, j) => {
    println(s"${i},${j}")
    i * j
  })
  println(multiplication)
}
