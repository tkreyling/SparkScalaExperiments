import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Exp1 {
  case class Person(id : Int, firstname : String, lastname : String, knowledge : List[String])

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local");
    val sc = new SparkContext(conf);

    val people = sc.textFile("src/main/resources/persons.csv").map(s => s.split(",")).map(s => Person(s(0) toInt, s(1), s(2), List())).map(p => (p.id, p));
    people.collect.foreach(println)

    val knowledge = sc.textFile("src/main/resources/knowledge.csv").map(s => s.split(",")).map(s => (s(0) toInt, s(1)));
    knowledge.collect.foreach(println)

    val peopleWithKnowledge = leftOuterJoinList[Int,Person,String](people, knowledge, (p,k) => p.copy(knowledge = k))
    println(peopleWithKnowledge.toDebugString)

    peopleWithKnowledge.collect.foreach(println);
  }

  def leftOuterJoinList[K,V1,V2](leftItems: RDD[(K, V1)], rightItems: RDD[(K, V2)], copyConstructor: (V1, List[V2]) => V1)
                                (implicit kt : scala.reflect.ClassTag[K], v1t : scala.reflect.ClassTag[V1], v2t : scala.reflect.ClassTag[V2], ord : scala.Ordering[K])
  : RDD[(K, V1)] = {
    val groupedRightItems = rightItems.groupByKey.mapValues(_.toList)

    leftItems.leftOuterJoin(groupedRightItems)
      .mapValues { case (left, right) => (left, right.getOrElse(List()))}
      .mapValues { case (left, right) => copyConstructor(left, right)}
  }
}
