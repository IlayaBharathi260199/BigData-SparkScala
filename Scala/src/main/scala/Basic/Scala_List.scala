package Basic

/*
List
=====
1.List is a data structure in scala
2.Immutable and ordered collection
3.we can't modify the element of list . because it is immutable
4.Index value is stating from (0)
5.we can store duplicate elements in List
6.it can store elements of same datatype (while creating List we can define)
*/
object Scala_List {
  def main(args: Array[String]): Unit = {

    val mylist: List[Int] = List(1, 2, 3, 4, 4, 5, 5, 6, 7, 8, 9)
    val mylist2: List[Int] = List(12, 34, 45, 56, 78, 91)
    val mylist3: List[String] = List("ilaya", "bharathi", "murugan")


    // immutable ,so we can't change element
    //  mylist(0) = 10

    println(mylist)
    println(mylist.length)
    println(mylist.min)
    println(mylist(5))
    println(mylist.isEmpty)
    println(mylist.contains(2))
    println(mylist.reverse)
    println(mylist.max)
    println(mylist ++ mylist2 ++ mylist3) // List concat

    // Multiplication with List
    val mul = mylist.map(_ * 2) // '_' wildcard represents "mylist" 
    mul.foreach(println)

    // Filter with List
    val fil = mylist.filter(_ > 4)
    fil.foreach(println)
  }
}
