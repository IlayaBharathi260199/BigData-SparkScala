package Basic

/*
Option
======
1.Option is Container, that returns 2 types "Some" and "None"

          Option[T]
              ^
              |
      +-------+------+
      |              |
      |              |
    Some[T]        None[T]

2.if the condition satisfy option, it will return "Some",  else "None"
3.will be used for Pattern matching
4.Options are helpful while handling with Null Values
5.It will be help full to map raw case class to Final Case class
 */
object Scala_Option {

  def main(args:Array[String]) {

    val list = List(1, 2, 3, 4, 5)
    val list1 = List(1, 2, 3, 4, 5,"ilsyay")

    val mymap = Map(1 -> "ilaya", 2 -> "Bharathi", 3 -> "Murugan")
    val opt: Option[String] = Some("ilaya")
    val opt1: Option[String] = None

    val a = list.filter(_ > 2)
    a.foreach(println)

    println(list.find(_ > 2))
    println(list.find(_ > 56))
    println(mymap.get(12).getOrElse("Null"))
    println(opt.get)
    println(opt.isEmpty)
    println(opt.nonEmpty)
    println(opt.isDefined)

    list1.foreach {
    case i:Int => println(" hey i am  Integer" + i)
    case s:String => println("hey i am a string" + s)
  }

  }
}
