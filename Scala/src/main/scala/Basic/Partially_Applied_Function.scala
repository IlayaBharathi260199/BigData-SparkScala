package Basic

import java.util.Date


/*
Partially Applied Function
=========================

1.In scala we can partially apply function with underscore and data types('_:datatype')
2.if any of our parameter is fixed means we can partially apply with that parameter
3.we can use partially applied function in higher order function, it will work well in higher order function

*/

object Partially_Applied_Function {

  def log(date:Date,message:String) ={
    println(date +" "+ message)
  }
  def main(args:Array[String]) {

    val date = new Date
    val finalLog = log(date,_:String)
    finalLog("success")


    def email(name:String,id:String) = {
      println(name + id)
    }

      val finalemail = email(_:String,"@gmail.com")
    finalemail("ilayabharathi")
    }
}
