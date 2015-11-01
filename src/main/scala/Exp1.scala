import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Exp1 {
  case class Person(id : Int, firstname : String, lastname : String)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local");
    val sc = new SparkContext(conf);

    val people = sc.textFile("src/main/resources/persons.csv").map(l => l.split(",")).map(l => Person(l(0) toInt, l(1), l(2))).map(p => (p.id, p));
    people.collect.foreach(println)

    val knowledge = sc.textFile("src/main/resources/knowledge.csv").map(l => l.split(",")).map(l => (l(0) toInt, l(1)));
    knowledge.collect.foreach(println)

    people.leftOuterJoin(knowledge.groupByKey).collect.foreach(println);
  }
}
