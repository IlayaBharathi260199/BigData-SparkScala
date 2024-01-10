package Basic

object PatternMatching {

  def main(args: Array[String]): Unit = {

    //Patten Matching
    val a: Any = "ilaya"
    a match {
      case _: String => println("a is string")
      case _: Int => println("a is integer")
      case _ => println("a Other datatype")
    }


    // patten matching with Function
    val b = 7
    b match {
      case n if n % 2 == 0 => println("Even")
      case _ => println("Odd")
    }


    // patten matching with Function
    def oddeve(a: Int): String = a match {
      case n if n % 2 == 0 => "Even"
      case _ => "Odd"
    }

    println(oddeve(98))

    // extract only string elements from list
    val list: List[Any] = List("Ilaya", 78, "bharathi", 26)
    val StringList: List[String] = list.collect {
      case s: String => s
    }
    println(StringList)


    //List flatten
    val flat = StringList.flatMap(x => x.split(","))
    val str = flat.map(x => x + " String")
    flat.foreach(println)
    str.foreach(println)


    //Patten matching with list Iteration
    val lista: List[Any] = List("Ilaya", 3453, "bharathi", 6456456)
    lista.foreach {
      case s: String => println("String " + s)
      case i: Int => println("Int " + i)


    }

  }

}
