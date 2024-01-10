package Basic

/*
Closure is a function which uses one or more variables from outside of the function
*/
object Scala_Array {
  def main(args: Array[String]): Unit = {


    val myarray = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    // directly we cant print array by using println

    for (a <- myarray) {
      println(a)

      val stringArray = new Array[String](5)
      stringArray(0) = "ilaya0"
      stringArray(1) = "ilaya1"
      stringArray(2) = "ilaya2"
      stringArray(3) = "ilaya3"
      stringArray(4) = "ilaya4"

      for (i <- stringArray) {
        println(i)
      }
      for (i <- 1 to stringArray.length - 1) {
        println(stringArray(i))
      }

      println(myarray.length)
      println(myarray.min)
      println(myarray(5))

    }

  }

}
