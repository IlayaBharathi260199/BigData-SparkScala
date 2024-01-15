package Basic


object Recursive_Scala {

  def addi(a:String,b:Int)= {
    val x = a +" "+  b
    println(x)
  }

  def factorial(a:Int):Int = {
    if (a <=0) 1
    else a * factorial(a-1)

  }

  def fact(n:Int) ={
    var a = 0
    for (i <- 1 to n) {
      a = a + i
    }
    println(a)
  }

  def rec(a: Int): Int = {
    if (a <= 0) 0
    else  a + rec(a - 1)
  }



  def main(args:Array[String]): Unit = {
    addi("ilsy",3)

       println(rec(8))

       fact(8)








    val mylist =List(1,2,3,4,5)
    var res=0
    for (i <- mylist) {
      res = i + res
    }
     println(res)

    def listRec(a:List[Int]):Unit = {
      if (a.nonEmpty) {
        val x = a.map(_ * 2)
        listRec(a.tail)

      }

    }


    println(listRec(mylist))
  }

}















