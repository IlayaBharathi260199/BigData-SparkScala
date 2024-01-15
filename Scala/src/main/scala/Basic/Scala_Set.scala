package Basic

/*
set
=====
1.Set is a data structure in scala
2.Immutable(default) and unordered collection
3.we can't modify element of Set . because it is immutable
5.set wont allow us to store duplicate elements, it should be unique
6.it can store elements of same datatype (while creating Set we can define)
*/
object Scala_Set {
  def main(args: Array[String]): Unit = {

    val myset: Set[Int] = Set(1, 2, 3, 4, 4, 5, 5, 6, 7, 8, 9)
    val myset2: Set[String] = Set("ilaya","bharathi","murugan")
    val myset3: Set[Int] = Set(11, 92, 31, 42, 44, 6, 56, 8, 76, 86)



    // immutable ,so we can't change element
    //  myset(0) = 10

    println(myset)
    println(myset.min)
    println(myset(5))
    println(myset.isEmpty)
    println(myset.contains(2))
    println(myset.max)
    println(myset ++ myset2) // set concat
    println(myset.intersect(myset3))


    // Multiplication with Set
    val mul = myset.map(_ * 2) // '_' wildcard represents "myset" 
    mul.foreach(println)

    // Filter with Set
    val fil = myset.filter(_ > 4)
    fil.foreach(println)

    for (i <- myset2) {
      println("Hi...!!! "+i)
    }

  }
}
