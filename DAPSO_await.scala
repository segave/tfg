package DU

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.CollectionAccumulator
import org.apache.spark.util.DoubleAccumulator
import scala.util.Random
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.async.Async._
import akka.actor.{Actor, ActorSystem, Props}
import scala.util.{Success, Failure}


/**
 * @author ${user.name}
 */

class Lote(private val S: Int) {
  private var lote: Array[Array[Double]] = Array.ofDim[Array[Double]](S)
  private var index: Int = 0

  def agregar(elemento: Array[Double]): Unit = {
    if (index < S) {
      lote(index) = elemento
      index += 1
    } else {
      throw new IllegalStateException("El lote está lleno")
    }
  }

  def estaCompleto: Boolean = index == S

  def obtenerLote: Array[Array[Double]] = lote.clone()

  def clean(): Unit = {
    lote = Array.ofDim[Array[Double]](S)
    index = 0
  }
}

class Aggregator(private val S: Int, private val srch: Channel[Lote]) {
  private val lote = new Lote(S)

  def recibir(datos: Array[Double]): Unit = {
    if(lote.estaCompleto) {
      srch.write(lote)
      lote.clean()
    }
      lote.agregar(datos)
  }
}




object App {
  val conf = new SparkConf()
    .setAppName("PSO Distribuido")
    .setMaster("local[*]")
  val sc = SparkContext.getOrCreate(conf)
  val rand = new Random()
  val V_max = 10.0
  val W = 1.0
  val c_1 = 0.8
  val c_2 = 0.2
  val objetivo_ = Array[Double](50, 50, 50)
  val objetivo = sc.broadcast(objetivo_)

  // Número dimensiones de los vectores
  val n = 3
  // Número de partículas
  val m = 10
  //número de partículas por lote
  val S = 5
  // Número de iteraciones
  val I = 10000

  var acabado: Boolean = false

  var particulas = Array.empty[Array[Double]]
  var mejor_pos_global_arr = Array.fill(n)(0.0)
  // maximum float
  var best_global_fitness = Double.MaxValue
  var mejor_pos_global = mejor_pos_global_arr


  // Definición de los Channel
  val srch = new Channel[Lote]()
  val fuch = new Channel[Array[Array[Double]]]()

  //Genera un uniform entre -a y a
  def Uniform(a: Double): Double = {
    val num = rand.nextDouble() * 2 * a // genera un número aleatorio entre 0.0 y 2a
    val ret = num - a
    ret
  }

  def MSE(y: Array[Double], pred: Broadcast[Array[Double]]): Double = {
    val n = y.length
    if (n != pred.value.length) {
      println("error: datos y predicción de distintos tamaños")
      return -1
    }
    var resultado = 0.0
    for (i <- 0 until n) {
      resultado += math.pow(y(i) - pred.value(i), 2)
    }
    resultado /= n
    resultado
  }
  def InitParticles(N: Int, M: Int):(Double,Array[Double],Array[Array[Double]]) ={
    var parts_ = Array.empty[Array[Double]]


    for (j <- 0 until M) {
      val posicion = Array.fill(N)(Uniform(100))
      val velocidad = Array.fill(N)(Uniform(100))
      val fit = MSE(posicion, objetivo)
      val part_ = posicion ++ velocidad ++ posicion ++ Array(fit)

      //best_local_fitness_arr = best_local_fitness_arr :+ fit
      if (fit < best_global_fitness) {
        best_global_fitness = fit
        //accum.setValue(fit)
        mejor_pos_global = posicion
      }
      parts_ = parts_ :+ part_
    }
    (best_global_fitness, mejor_pos_global, parts_)
  }
  def fitnessEval(part: Array[Double], N: Int):Array[Double] ={
    val best_fit_local = part(3*N)
    val filas = part.slice(0, N)
    val fit = MSE(filas, objetivo)
    if (fit < best_fit_local) {
      part(3*N) = fit
      for (k <- 0 until N) {
        part(2*N + k) = filas(k)
      }
    }
    part
  }

  def modifyAccum(part: Array[Double], N: Int, local_accum_pos:CollectionAccumulator[Array[Double]], local_accum_fit: CollectionAccumulator[Double]): Unit ={
    local_accum_pos.add((part.slice(2*N, 3*N)))
    local_accum_fit.add(part(3*N))
  }
  def posEval(part: Array[Double], mpg: Array[Double], N: Int):Array[Double] ={
    // global ind (no es necesario en Scala)
    val velocidades = part.slice(N, 2*N)
    val mpl = part.slice(2*N, 3*N)
    val r_1 = rand.nextDouble()
    val r_2 = rand.nextDouble()
    for (k <- 0 until N) {
      velocidades(k) = W*velocidades(k) + c_1*r_1*(mpl(k) - part(k)) + c_2*r_2*(mpg(k) - part(k))
      if (velocidades(k) > V_max) {
        velocidades(k) = V_max
      } else if (velocidades(k) < -V_max) {
        velocidades(k) = -V_max
      }
      part(k) = part(k) + velocidades(k)
      part(N+k) = velocidades(k)
    }
    part
  }

  def parallelFitness(srch: Channel[Lote], n: Int) = Future {
    val batch = srch.read
    val RDD = sc.parallelize(batch.obtenerLote)
    val psfu = RDD.map(x => fitnessEval(x, n)).collect()
    psfu
  }

  def awaitFitness (srch: Channel[Lote], n: Int) = async {
    await(parallelFitness(srch, n))
  }


  // Definir un actor para evaluar el fitness de forma asíncrona
  class FitnessEvalActor(srch: Channel[Lote], fuch: Channel[Array[Array[Double]]], n: Int) extends Actor {
    def receive: Receive = {
      case "start" =>
        while (!acabado) {
          // Esperar un elemento del canal srch
          val batch = srch.read
          val RDD = sc.parallelize(batch.obtenerLote)
          val psfu = RDD.map(x => fitnessEval(x, n)).collect()
          print("longitud " + batch.obtenerLote.length)
          //val psfu = batch.obtenerLote.map(x => fitnessEval(x, n))

          fuch.write(psfu)
        }
    }
  }

  // Definir un actor para evaluar el mejor global y actualizar de forma asíncrona
  class GlobalEvalActor(fuch: Channel[Array[Array[Double]]], N: Int, S: Int, aggr: Aggregator) extends Actor {
    var best_global_fitness = Double.MaxValue
    var mejor_pos_global: Array[Double] = _

    def receive: Receive = {
      case "start" =>
        val iters = I * m //: S
        for (i <- 0 until iters) {
          print(i)
          // Esperar un elemento del canal fuch
          val sr = fuch.read

          for (par <- sr) {
            val pos = par.slice(0, N)
            val velocidad = par.slice(N, 2 * N)
            val mpl = par.slice(2 * N, 3 * N)
            val fit = par(3 * N)

            if (fit < best_global_fitness) {
              best_global_fitness = fit
              mejor_pos_global = pos
            }
            val newPar = posEval(par, mejor_pos_global, N)
            aggr.recibir(newPar)
          }
          acabado = true
        }
    }
  }


  def main(args : Array[String]) {

    println( "Hello World!" )
    //println("concat arguments = " + foo(args))
    val num= 3.1416
    println("Generación uniforme de número = " + Uniform(num))
    var y0= rand.nextDouble()*50
    var y1= rand.nextDouble()*50
    var y2= rand.nextDouble()*50
    val datos= Array[Double](y0,y1,y2)
    println("datos y= ", y0, y1, y2)
    val result= MSE(datos,objetivo)
    println("resultado= ",result)

    //Inicialización de las partículas
    var resultado: (Double, Array[Double], Array[Array[Double]])=InitParticles(n,m)
    val (double1, array2, arrayDeArrays) = resultado
    best_global_fitness = double1
    mejor_pos_global = array2
    particulas = arrayDeArrays

    //Definimos el aggregator
    val aggr = new Aggregator(S,srch)

    //añadimos las partículas al aggregator
    for (i <- 0 until m){
      aggr.recibir(particulas(i))
    }

    var rdd_master = sc.parallelize(particulas)
    var tiempo_fitness = 0.0
    var tiempo_poseval = 0.0
    var tiempo_global = 0.0
    var tiempo_collect = 0.0
    var tiempo_foreach = 0.0

    val start = System.nanoTime()

    // Crear el sistema de actores
    val system = ActorSystem("MyActorSystem")

    // Crear los actores
    //val fitnessEvalActor = system.actorOf(Props(new FitnessEvalActor(srch, fuch, n)), "fitnessEvalActor")
    val globalEvalActor = system.actorOf(Props(new GlobalEvalActor(fuch, n, S, aggr)), "globalEvalActor")

    while (!acabado) {
      // Esperar un elemento del canal srch
      awaitFitness(srch, n).onComplete {
        case Success(psfu) => fuch.write(psfu)
      }

    }

    // Iniciar los actores
    //fitnessEvalActor ! "start"
    globalEvalActor ! "start"
    print("Hola")
































    if (acabado){
      val end = System.nanoTime()
      val tiempo = (end - start) / 1e9
      val resultado_final = rdd_master.collect()
      println(s"Tiempo de ejecucion(s): $tiempo")
      println(s"Tiempo de ejecucion fitness(s): $tiempo_fitness")
      println(s"Tiempo de ejecucion poseval(s): $tiempo_poseval")
      println(s"Tiempo de ejecucion global fitness(s): $tiempo_global")
      println(s"Tiempo de ejecucion collect(s): $tiempo_collect")
      println(s"Tiempo de ejecucion foreach(s): $tiempo_foreach")
      println(s"mejor_pos_global-> ${mejor_pos_global.mkString("[", ", ", "]")}")
      println(s"mejor fitness global-> $best_global_fitness, ${MSE(mejor_pos_global, objetivo)}")
    }
  }
}

