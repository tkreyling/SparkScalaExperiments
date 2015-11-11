object OptionExp {
  case class Person[KI <: Option[List[String]]](name : String, knowledgeItems : KI)

  def main(args: Array[String]): Unit = {
    val personWithoutKnowledgeItems: Person[None.type] = Person("Thomas", None)

    val personWithKnowledgeItems: Person[Some[List[String]]] = personWithoutKnowledgeItems.copy(knowledgeItems = Some(List("Java")))

    println(personWithoutKnowledgeItems)
    println(personWithKnowledgeItems)
  }
}
