package Basic

/*
1.Anonymous_Function is also called as "lambda function" ("=>")
2.we can define anonymous functions without name
3.Anonymous functions are useful when you need to pass a short piece of functionality as an argument to higher-order functions
4.The basic syntax for an anonymous function in Scala uses the => (rocket) symbol
5.wherever we see '=>', we can say it is a Anonymous_Function
6.we can assign it in variable
*/
object Anonymous_Function {

  def main(args: Array[String]): Unit = {

    val ano = (x: Int, y: Int) => x + y
    println(ano(2, 3))

    val list = List(1,2.3,4.5)
    val list1 = list.map(x => x + 2) //"x => x + 2" is Anonymous Function
    println(list1)

    def square(a:Int,b:Int,fun:(Int,Int) => Int) : Int = fun(a,b)

    println(square(5,5,(x,y) => x * y)) // In this line "(x,y) => x * y)" is anonymous function

  }

}
