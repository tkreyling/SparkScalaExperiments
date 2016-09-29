package kreyling.experiments

class Wurst {
  def sayWurst = {

    val optOfList = {
      Some(List(1, 2, 3))
    };

    val flatten  = optOfList.getOrElse(List())

    println(flatten)
    "Wurst!"
  }
}
