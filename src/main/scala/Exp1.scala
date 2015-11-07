import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Exp1 {
  case class Person(id : Int, firstname : String, lastname : String, knowledgeItems : List[KnowledgeItem])

  trait DTOFactory[T] {
    protected def construct(line: Array[String]) : T
    def fromCSV(line : String) = construct(line.split(","))
  }

  object PersonFactory extends DTOFactory[Person] {
    def construct(line : Array[String]) = Person(line(0) toInt, line(1), line(2), List())
  }

  case class KnowledgeItem(personId : Int, name : String, level : String)

  object KnowledgeItemFactory extends DTOFactory[KnowledgeItem] {
    def construct(line : Array[String]) = KnowledgeItem(line(0) toInt, line(1), line(2))
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local");
    val sc = new SparkContext(conf);

    val people = sc.textFile("src/main/resources/persons.csv").map(PersonFactory.fromCSV).map(p => (p.id, p));
    people.collect.foreach(println)

    val knowledgeItems = sc.textFile("src/main/resources/knowledge.csv").map(KnowledgeItemFactory.fromCSV).map(k => (k.personId, k));
    knowledgeItems.collect.foreach(println)

    val peopleWithKnowledge = leftOuterJoinList[Int,Person,KnowledgeItem](people, knowledgeItems, (p,k) => p.copy(knowledgeItems = k))
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
