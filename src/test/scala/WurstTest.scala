import org.scalatest.FunSuite

class WurstTest extends FunSuite {

  test("sayWurst says Wurst!") {
    val wurst = new Wurst
    assert(wurst.sayWurst == "Wurst!")
  }
}
