package Examples.Aggregator

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.TypedColumn


/**
 * @author ${user.name}
 */
// input schema
//
case class RetailByCustomer(customerID: String, productID: String, retailCategory: String)

// output schema
case class AggRetailByCategory(retailCategory: String, customerCount: Int, productSet: Set[String])

// object that holds our specific data transformation method

object PreprocessData{
  // data transformation we want to perform
  // note the typing of the df parameter and expected output
  val spark = SparkSession.builder()
        .appName("aggregatorExample")
        .config("spark.master", "local")
        .getOrCreate()
  import spark.implicits._
  def createArrayAndCount(df: Dataset[RetailByCustomer]): Dataset[AggRetailByCategory] = {
    
    val transformedData: Dataset[AggRetailByCategory] =
      // using a groupByKey method to group by any Dataset with type defined as 'retailCategory'
      df.groupByKey(_.retailCategory)
      .agg(
        // our aggregator functions at work
        PreprocessData.distinctCustomerCountAggregator.name("customerCount"),
        PreprocessData.productIDAggregator.name("productIDs")
      ).map{
        // retailCategory(rc), customerCount(cc), productSet(ps)
        case(rc: String, cc: Int, ps: Set[String]) => AggRetailByCategory(rc, cc, ps)
      }
    transformedData
  }
import spark.implicits._
import Encoders._
  val distinctCustomerCountAggregator: TypedColumn[RetailByCustomer, Int] = new Aggregator[RetailByCustomer, Set[String], Int] {
    override def zero: Set[String] = Set[String]()
    override def reduce(es: Set[String], rbc: RetailByCustomer): Set[String] = es + rbc.customerID
    override def merge(wx: Set[String], wy: Set[String]): Set[String] = wx.union(wy)
    override def finish(reduction: Set[String]): Int = reduction.size
    override def bufferEncoder: Encoder[Set[String]] = implicitly(ExpressionEncoder[Set[String]])
    override def outputEncoder: Encoder[Int] = implicitly(Encoders.scalaInt)
    }.toColumn

  // Creating an array of productIDs from the dataset
  val productIDAggregator: TypedColumn[RetailByCustomer, Set[String]] = new Aggregator[RetailByCustomer, Set[String], Set[String]] {
    override def zero: Set[String] = Set[String]()
    override def reduce(es: Set[String], rbc: RetailByCustomer): Set[String] = es + rbc.productID
    override def merge(wx: Set[String], wy: Set[String]): Set[String] = wx.union(wy)
    override def finish(reduction: Set[String]): Set[String] = reduction
    override def bufferEncoder: Encoder[Set[String]] = implicitly(ExpressionEncoder[Set[String]])
    override def outputEncoder: Encoder[Set[String]] = implicitly(ExpressionEncoder[Set[String]])
    }.toColumn
    
}
object App {
  
//  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
import PreprocessData._
  
def main(args : Array[String]) {
  println( "Hello World!" )
//    println("concat arguments = " + foo(args))

val retailData = Seq(
("001", "zk67", "Lighting"),
("001", "gg89", "Paint"),
("002", "gg97", "Paint"),
("003", "gd01", "Gardening"),
("003", "af83", "A.C."),
("003", "af84", "A.C."),
("004", "gd77", "Gardening"),
("004", "gd73", "Gardening"),
("005", "cl55", "Cleaning"),
("005", "zk67", "Lighting"),
)
val rdd = spark.sparkContext.parallelize(retailData)
//retailData.toDF("customerID", "productID", "retailCategory")
val rddnew= rdd.map(x=>RetailByCustomer(x._1,x._2,x._3))
import spark.implicits._
val ds:Dataset[RetailByCustomer]= spark.createDataset[RetailByCustomer](rddnew)
ds.show()
val transformedRetailData = PreprocessData.createArrayAndCount(ds)
transformedRetailData.show()
  }
}
