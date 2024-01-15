package OOPS

class InheritClass {

  def sum(a:Int,b:Int) = a + b
}

class InheritClass2 extends InheritClass {
  def sub(x:Int,y:Int) = x-y
}
object ilaya {
  def main (args:Array[String]): Unit = {

    val objin = new InheritClass2
    println(objin.sum(35,43))
    println(objin.sub(55,5))

  }

}

