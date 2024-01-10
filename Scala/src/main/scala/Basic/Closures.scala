package Basic


/*
Closure is a function which uses one or more variables from outside of the function
*/
object Closures {

  def main(args: Array[String]): Unit = {

    val y = 15

    def add(x:Int) = x + y
    println(add(5))



    val b = 50

    def sum(x: Int, y: Int) = x + b

    print(sum(56, b))
  }

}
