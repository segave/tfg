package DSPSO

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.CollectionAccumulator
import org.apache.spark.util.DoubleAccumulator
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.Row

import scala.util.Random

object DU {
  val conf = new SparkConf().setAppName("PSO Distribuido").setMaster("local")
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
  // Número de iteraciones
  val I = 100
  var posiciones_ : Array[Double] = Array.empty[Double]
  var mejor_posiciones_locales_ : Array[Double] = Array.empty[Double]
  var velocidades_ : Array[Double] = Array.empty[Double]
  var particulas = Array.empty[Array[Double]]
  var mejor_pos_global_arr = Array.fill(n)(0.0)
  var best_local_fitness_arr : Array[Double] = Array.empty[Double]
  // maximum float
  val max = Double.MaxValue
  var best_global_fitness = max
  val accum = sc.doubleAccumulator("accum")
  // mejor_pos_global = sc.broadcast(mejor_pos_global_arr)
  var mejor_pos_global = mejor_pos_global_arr
  var best_local_fitness = best_local_fitness_arr

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


  def InitParticles(N: Int, M: Int, bgf: Double, blf: Array[Double]):(Array[Double],Double,Array[Double],Array[Array[Double]]) ={
    var parts_ = Array.empty[Array[Double]]

    for (j <- 0 until M) {
      val posicion = Array.fill(N)(Uniform(100))
      val velocidad = Array.fill(N)(Uniform(100))
      val fit = MSE(posicion, objetivo)
      val part_ = posicion ++ velocidad ++ posicion ++ Array(fit)

      best_local_fitness_arr = best_local_fitness_arr :+ fit
      if (fit < best_global_fitness) {
        best_global_fitness = fit
        //accum.setValue(fit)
        mejor_pos_global = posicion
      }
      parts_ = parts_ :+ part_
    }

    (best_local_fitness_arr, bgf, mejor_pos_global, parts_)
  }


  def fitnessEval(part: Array[Double], N: Int):Array[Double] ={
    val best_fit_local = part.slice(3*N, 4*N)
    val filas = part.slice(0, N)
    val fit = MSE(filas, objetivo)
    if (fit < best_fit_local(0)) {
      part(3*N) = fit
      for (k <- 0 until N) {
        part(2*N + k) = filas(k)
      }
      if (fit < best_global_fitness) {
        accum.add(fit - best_global_fitness)
        best_global_fitness = fit
        mejor_pos_global = filas
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
    //return part,velocidades (no es necesario en Scala)
    return part
  }


  def main(args: Array[String]): Unit = {

    //best_local_fitness,best_global_fitness,mejor_pos_global,particulas
    var resultado: (Array[Double], Double, Array[Double], Array[Array[Double]])=InitParticles(n,m,best_global_fitness,best_local_fitness)

    val (array1, double1, array2, arrayDeArrays) = resultado
    best_local_fitness = array1
    best_global_fitness = double1
    mejor_pos_global = array2
    particulas = arrayDeArrays

    var rdd_master = sc.parallelize(particulas)
    var tiempo_fitness = 0.0
    var tiempo_poseval = 0.0
    var tiempo_global = 0.0
    var tiempo_collect = 0.0
    var tiempo_foreach = 0.0

    //println("rdd_master antes:", rdd_master.collect())
    //println("antes:", mejor_pos_global)

    val start = System.nanoTime()

    for (i <- 0 until I) {
      val local_accum_pos: CollectionAccumulator[Array[Double]] = sc.collectionAccumulator[Array[Double]]("MejorPosLocales")
      //val local_accum_fit: DoubleAccumulator = sc.doubleAccumulator("MiAcumulador")
      val local_accum_fit: CollectionAccumulator[Double] = sc.collectionAccumulator[Double]("MejorPosLocales")

      val start_fitness = System.nanoTime()
      val rdd_fitness = rdd_master.map(x => fitnessEval(x, n))
      val end_fitness = System.nanoTime()

      tiempo_fitness += (end_fitness - start_fitness) / 1e9

      val start_foreach = System.nanoTime()
      rdd_fitness.foreach(x => modifyAccum(x, n, local_accum_pos,local_accum_fit))
      //val rdd_eval = sc.broadcast(rdd_fitness.collect())
      val end_foreach = System.nanoTime()

      tiempo_foreach += (end_foreach - start_foreach) / 1e9

      //println("local_accum: ", local_accum.value)

      val start_global = System.nanoTime()
      var best_global_fitness = Double.MaxValue
      var mejor_pos_global = Array.fill(n)(0.0)
      val blfs = local_accum_fit.value
      for (j <- 0 until m) {
        //println("local_accum(",j,"): ", local_accum.value(0)(j)(1))
        val blf = blfs.get(j)
        if (blf < best_global_fitness) {
          best_global_fitness = blf
          mejor_pos_global = local_accum_pos.value.get(j)
        }
      }
      val end_global = System.nanoTime()

      tiempo_global += (end_global - start_global) / 1e9

      val start_poseval = System.nanoTime()
      val resultado = rdd_fitness.map(x => posEval(x, mejor_pos_global, n))
      val end_poseval = System.nanoTime()

      tiempo_poseval += (end_poseval - start_poseval) / 1e9

      val start_collect = System.nanoTime()
      val resultado_collected = resultado.collect()
      val end_collect = System.nanoTime()

      tiempo_collect += (end_collect - start_collect) / 1e9

      //for (j <- 0 until m) {
      //  particulas(j) = resultado(j).clone()
      //}

      rdd_master = sc.parallelize(resultado_collected)
    }

    val end = System.nanoTime()

    //println("despues:", mejor_pos_global)
    //println("rdd_master despues:", rdd_master.collect())

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
    //println("partículas: ",particulas)
    //println(s"local_accum:",local_accum_pos)
  }
}
