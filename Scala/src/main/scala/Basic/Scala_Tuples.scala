package Basic

/*
Tuple
=====
1.Tuple is a data structure in scala
2.Immutable and ordered collection
3.we can't modify the element of Tuple . because it is immutable
4.Index value is stating from ._1
5.we can store duplicate elements in Tuple
6.it can store elements of same datatype (while creating Tuple we can define)
*/
object Scala_Tuples {
  def main(args: Array[String]): Unit = {

   
    val mytuple =(1,2,3,"ilaya", "bharathi", "murugan",2)
    val mytuple1 =(11,21,31,(1,2)) // Inside one tuple, we can create another tuple.

     println(mytuple._2)
    println(mytuple)
    println(mytuple1._4)
    println(mytuple1._4._2)  // calling Nested tuple.

    // productIterator used to iterate each and every element from mytuple
    mytuple.productIterator.foreach(
      i=> println("hello " + i)
    )

  }


}
