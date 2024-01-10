package Basic


/*
Currying Function
=================
In scala ,
Currying is technique which transforms a function from that takes multiple arguments into single argument.
Also its supports the partial applications
 */
object CurryingFunction {
  def summ (x:Int)(y:Int) = x + y

  def email(id:String) (name:String) = {
    println(name + id)
  }
  def main(args:Array[String]): Unit ={
    //val a =add(5)_
    val b =summ(5)_ // here that '_' used to fill the second argument for future
    println(b(25))

    val fixed = email ("@gmail")_
    fixed("ilaya")
    fixed("sdfsd")
    fixed("sdfsdf")
    fixed("bbnmbnm")
    fixed("uikghjf")



  }

}
