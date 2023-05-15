package Concurrent
/**
 * @author ${user.name}
 */
import scala.util.Random
import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global
object App {
  
 // def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  
def main(args : Array[String]) {
  var  fifo: Channel[Int]=null
  //println( "Hello World!" )
   // println("concat arguments = " + foo(args))
   fifo= new Channel[Int]
   val a= 5
   val b=6
   val c= 7
   fifo.write(a)
   fifo.write(b)
   fifo.write(c)
   for( i<-0 to 3)
     println(fifo.read)
  
  val first:Future[Int]=Future{
    var  fifo2: Channel[Int]=null
    val rand = new scala.util.Random
    val rand1= (rand.nextInt()).toInt
    fifo2.write(rand1)
    rand1
  }
  for (i<-0 to 10000)
    fifo.write(a)
 Await.result(first, 10.seconds) 
  val obtained: Future[Int] = for {
  res <- first
  
  } println( fifo.read)

  }
}