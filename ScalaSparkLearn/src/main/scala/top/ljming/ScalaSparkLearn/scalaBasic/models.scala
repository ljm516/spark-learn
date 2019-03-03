package top.ljming.ScalaSparkLearn.scalaBasic

class Calculator(brand: String) { // 构造函数
  //  val brand: String = "HP"
  val color: String = if (brand == "TI") {
    "blue"
  } else if (brand == "HP") {
    "black"
  } else {
    "white"
  }

  def add(m: Int, n: Int): Int = m + n
}

class ScientificCalculator(brand: String) extends Calculator(brand) { // 继承
  def log(m: Double, base: Double) = math.log(m) / math.log(base)
}

class EvenMoreScientificCalculator(brand: String) extends ScientificCalculator(brand) {
  def log(m: Int): Double = log(m, math.exp(1)) // 重载方法
}

abstract class Shape { // 抽象类
  def getArea(): Double
}

class Circle(r: Int) extends Shape { // 继承抽象类，实现抽象方法
  override def getArea(): Double = {
    r * r * 3.14
  }
}

/**
  * trait(特质)
  * 一些字段和行为的集合， 可以扩展或 mixin 类中
  */
trait Car {
  val brand: String
}

trait Shiny {
  val shineRefraction: Int
}

class BMW extends Car with Shiny { // 通过 with 关键字可以继承多个 trait
  override val brand: String = "BMW"
  override val shineRefraction: Int = 12
}

class Foo {
}

class Bar() {
  def apply() = 0
}

