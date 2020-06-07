package languagelearning.chapter12

/**
 * @Description
 * @author ZhangTing
 * @create 2020-06-04 09:46
 **/

package p {
  class Super {
    private[p] def f() = { println("f") }
  }
  class Sub extends Super {
    f()
  }
  class Other {
    (new Super).f()  // error: f is not accessible
  }
}

