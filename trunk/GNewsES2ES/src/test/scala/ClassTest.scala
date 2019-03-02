
/**
  * Created by Shy on 2017/10/24
  */

object ClassTest {
  def main(args: Array[String]) {
    val testA: SuperClass = new SuperClass
    val testB: SuperClass = new SubClass
    val testC: SubClass = new SubClass

    println(testA.isInstanceOf[SuperClass])
    println(testB.isInstanceOf[SuperClass])
    println(testC.isInstanceOf[SubClass])

    println(testA.asInstanceOf[SuperClass])
    println(testB.asInstanceOf[SuperClass])
    println(testB.asInstanceOf[SubClass])

    if (testB.getClass() == classOf[SuperClass]) {
      println("true")
    }
    else {
      println("false")
    }
  }
}

class SuperClass {}

class SubClass extends SuperClass {}
