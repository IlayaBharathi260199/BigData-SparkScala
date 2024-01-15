package Basic

/*
Array
=====
1.Array is a data structure in scala
2.Mutable and ordered collection
3.we can modify element of array
4.Index value is stating from (0)
5.we can store duplicate elements in Array
6.it can store elements of same datatype (while creating Array we can define)
*/
object Scala_Array {
  def main(args: Array[String]): Unit = {


    val myarray: Array[Int] = Array(1, 2, 3, 4, 4, 5, 5, 6, 7, 8, 9)
    // directly we cant print array by using println

    // mutable ,so we can change element
    myarray(0) = 10

    //  printing array by using for loop
    for (a <- myarray) {
      println(a)
    }
    val stringArray = new Array[String](5)
    stringArray(0) = "ilaya0"
    stringArray(1) = "ilaya1"
    stringArray(2) = "ilaya2"
    stringArray(3) = "ilaya3"
    stringArray(4) = "ilaya4"

    for (i <- stringArray) {
      println(i)
    }


    // Printing StringArray based on its length
    for (i <- 1 to stringArray.length - 1) {
      println(stringArray(i))
    }

    println(myarray.length)
    println(myarray.min)
    println(myarray(5))

    // Multiplication with Array
    val mul = myarray.map(_ * 2) // '_' wildcard represents "myarray"
    mul.foreach(println)

    // Filter with Array
    val fil = myarray.filter(_ > 4)
    fil.foreach(println)
  }
}
