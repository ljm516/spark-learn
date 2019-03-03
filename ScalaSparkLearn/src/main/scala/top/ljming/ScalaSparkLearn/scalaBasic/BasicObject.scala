package top.ljming.ScalaSparkLearn.scalaBasic

object Basic {

  def main(args: Array[String]): Unit = {

    val a = 1 + 1 // 定义常量
    println("常量 a: " + a)

    var s = "ljm" // 定义变量
    println("origin s: " + s)
    s = "ljm learn scala"
    println("after update -> s: " + s)

    var result = addOne(5)
    println("addOne() -> result: " + result)

    result = adder(2, 3)
    println("adder() -> result: " + result)

    val add2 = adder(2, _: Int)
    result = add2(3)
    println("add2: " + add2) // add2: top.ljming.ScalaSparkLearn.scalaBasic.BasicObject$$$Lambda$5/1321640594@1b40d5f0
    println("部分应用 -> adder(2, _:Int), result: " + result)

    val add3 = adder(_: Int, 3)
    result = add3(2)
    println("add3: " + add3)
    println("部分应用 -> adder(_Int, 3), result: " + result)

    val timeTwo = multiply(2) _
    println("科里函数 -> timeTow: " + timeTwo) // 科里函数 -> timeTow: top.ljming.ScalaSparkLearn.scalaBasic.BasicObject$$$Lambda$7/1207140081@5a01ccaa
    val six = timeTwo(3)
    println("timeTow -> " + six)

    val parameters = capitalizeAll("hello", "world")
    println("可变长度参数: " + parameters)

    parameters.foreach(p => print(p + "\t"))

  }

  // 带参函数
  def addOne(m: Int): Int = m + 1

  // 部分应用
  def adder(m: Int, n: Int) = m + n

  // 科里函数
  def multiply(m: Int)(n: Int): Int = m * n

  // 可变长度参数
  def capitalizeAll(args: String*) = {
    args.map {
      arg => arg.capitalize
    }
  }
}

object CalculatorObject {

  def main(args: Array[String]): Unit = {
    val calc = new Calculator("HP")
    val result = calc.add(2, 5)
    println("calculator color: " + calc.color + ", calc result: " + result)

    val scientificCalc = new ScientificCalculator("HP-scientific")
    val scientificResult = scientificCalc.log(4, 2)
    println("scientificCal color: " + scientificCalc.color + ", calc result: " + scientificResult)

    val evenMoreScientificCalculator = new EvenMoreScientificCalculator("TI")
    val evenScientificResult = evenMoreScientificCalculator.log(10)
    println("evenMoreScientificCalculator color: " + evenMoreScientificCalculator.color + ", calc result: " + evenScientificResult)

    val c = new Circle(3)
    val area = c.getArea()
    println("circle area: " + area)
  }

}

object FooMaker {
  def apply() = new Foo // apply 方法，当类或对象有一个主要用途的时候，apply 方法提供了一个很好的语法糖

  def main(args: Array[String]): Unit = {
    val newFoo = FooMaker()
    println(newFoo) // top.ljming.ScalaSparkLearn.scalaBasic.Foo@256216b3

    val bar = new Bar
    println(bar()) // 0
  }
}

object Timer {
  var count = 0

  def currentCount(): Long = {
    count += 1
    count
  }

  def main(args: Array[String]): Unit = {
    val count = Timer.currentCount()
    println(count)

  }
}

object Plus { // 函数即对象
  def main(args: Array[String]): Unit = {
    val plusOne = PlusOne(2)
    println("plus one result: " + plusOne)
  }
}

object PlusOne extends ((Int) => Int) {
  override def apply(m: Int): Int = m + 1
}

// 模式匹配
object Matcher {


  def main(args: Array[String]): Unit = {
    matchNum()
    println(matchInstance("0"))

  }

  // 类型匹配
  def matchInstance(o: Any): Any = {
    o match {
      case i: Int if i < 0 => i - 1
      case i: Int => i + 1
      case d: Double if d < 0.0 => d - 0.1
      case d: Double => d + 0.1
      case text: String => text + "s"
    }
  }

  // 数字匹配
  def matchNum() {
    val times = 4
    times match {
      case 1 => println("one")
      case 2 => println("two")
      case 3 => println("three")
      case _ => println("other number")
    }

    val another = 2
    another match { // 使用守卫进行匹配
      case i if i == 1 => println("one")
      case i if i == 2 => println("two")
      case i if i == 3 => println("three")
      case _ => println("other number")
    }
  }
}