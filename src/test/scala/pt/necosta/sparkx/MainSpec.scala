package pt.necosta.sparkx

import org.scalatest.{FlatSpec, Matchers}

class MainSpec extends FlatSpec with Matchers {

  "Add method" should "correctly sum 2 integers" in {
    Main.add(4, 5) should be(9)
    Main.add(-1, -5) should be(-6)
  }
}
