package kreyling.experiments

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

object Exp1 {
  case class Person[KI<:Option[List[KnowledgeItem]]](id : Int, firstname : String, lastname : String, knowledgeItems : KI)

  trait DTOFactory[T] {
    protected def construct(line: Array[String]) : T
    def fromCSV(line : String) = construct(line.split(","))
  }

  object PersonFactory extends DTOFactory[Person[None.type]] {
    def construct(line : Array[String]) = Person(line(0) toInt, line(1), line(2), None)
  }

  case class KnowledgeItem(personId : Int, name : String, level : String)

  object KnowledgeItemFactory extends DTOFactory[KnowledgeItem] {
    def construct(line : Array[String]) = KnowledgeItem(line(0) toInt, line(1), line(2))
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local");
    val sc = new SparkContext(conf);

    val people = sc.textFile("src/main/resources/kreyling/experiments/persons.csv").map(PersonFactory.fromCSV).map(p => (p.id, p));
    people.collect.foreach(println)

    val knowledgeItems = sc.textFile("src/main/resources/kreyling/experiments/knowledge.csv").map(KnowledgeItemFactory.fromCSV).map(k => (k.personId, k));
    knowledgeItems.collect.foreach(println)

    val peopleWithKnowledge = leftOuterJoinList[Int,Person[None.type],Person[Some[List[KnowledgeItem]]],KnowledgeItem](people, knowledgeItems, (p,k) => p.copy(knowledgeItems = Some(k)))
    println(peopleWithKnowledge.toDebugString)

    peopleWithKnowledge.collect.foreach(println);
  }

  def leftOuterJoinList [K:ClassTag,V1:ClassTag,V1X:ClassTag,V2:ClassTag]
  (leftItems: RDD[(K, V1)], rightItems: RDD[(K, V2)], copyConstructor: (V1, List[V2]) => V1X)
  : RDD[(K, V1X)] = {
    val groupedRightItems = rightItems.groupByKey.mapValues(_.toList)

    leftItems.leftOuterJoin(groupedRightItems)
      .mapValues { case (left, right) => (left, right.getOrElse(List()))}
      .mapValues { case (left, right) => copyConstructor(left, right)}
  }
}
