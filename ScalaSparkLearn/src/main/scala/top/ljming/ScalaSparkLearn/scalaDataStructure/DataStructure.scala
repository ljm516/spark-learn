package top.ljming.ScalaSparkLearn.scalaDataStructure

object DataStructure {

  def main(args: Array[String]): Unit = {
    val numbers = List(1, 2, 2, 3, 4)  // 定义集合，可有重复元素
    println("number list: " + numbers)

    val numberSet = Set(1, 2, 2, 3, 4)
    println("number set: " + numberSet) // 定义集合，无重复元素

    val hostPost = ("localhost", 80)  // 元组
    println(hostPost._1 + ": " + hostPost._2)

    val twoItem = 1 -> 2
    println("twoItem: " + twoItem)

    val map = Map(1 -> "one", 2 -> "two") // map
    println("map: " + map)
    map.foreach(f => println("get by key: " + map.get(f._1)))  //get by key: Some(one)

    println("none: " + map.get(3))  //none: None

  }
}


// 函数组合子
object FunctionalCombinations {

  def main(args: Array[String]): Unit = {
    val numbers = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    // map
    val timesTwoNumbers = numbers.map((i: Int) => i * 2)
    println("times two numbers: " + timesTwoNumbers)
    val timesTwoMethodNumbers = numbers.map(i => timesTwo(i))
    println("times two for timesTwo method: " + timesTwoMethodNumbers)

    // foreach
    numbers.foreach(i => println(i))

    // filter
    val evenNumbers = numbers.filter(i => i % 2 == 0)
    println("evenNumbers: " + evenNumbers)
    val evenNumbersForIsEven = numbers.filter(i => isEven(i))
    println("evenNumbersForIsEven: " + evenNumbersForIsEven)

    // zip： 将两个列表的内容聚合到一个对偶列表中
    val zipResult = List(1, 2, 3, 4).zip(List("one", "two", "three", "four"))
    println("zipResult: " + zipResult)

    // partition: 使用给定的谓语函数分隔列表
    val partResult = numbers.partition(_ % 2 == 0)
    println("partResult: " + partResult)

    // find: 返回集合中第一个匹配谓语函数的元素
    val findResult = numbers.find(i => i > 5)
    println("findResult: " + findResult)

    // drop: 删除前 i 个元素
    val dropResult = numbers.drop(5)
    println("dropResult: " + dropResult)

    // dropWhile: 删除元素知道找到第一个匹配谓语函数的元素
    val dropWhileResult = numbers.dropWhile(_ % 2 == 0)
    println("dropWhileResult: " + dropWhileResult)

    // foldLeft: 0 为初始值（numbers 是一个 List[Int] 类型），m 作为一个累加器
    val foldLeftResult = numbers.foldLeft(0)((m: Int, n: Int) => m + n)
    println("foldLeftResult: " + foldLeftResult)

    // foldRight: 和 foldLeft 效果一样，只是开始的方向颠倒
    val foldRightResult = numbers.foldRight(0)((m: Int, n:Int) => m + n)
    println("foldRightResult: " + foldRightResult)

    // flatten: 将嵌套结构扁平化成一个层次的集合
    val flattenResult = List(List(1, 2), List(3, 4, 5)).flatten
    println("flattenResult: " + flattenResult)

    // flatMap: 是一个常用的组合子，结合映射[mapping] 和扁平化[flattening], flatMap 需要处理嵌套列表的函数，然后将结果串起来
    val flatMapResult = List(List(1, 2), List(3, 4)).flatMap(f => f.map(_ * 2))
    println("flatMapResult: " + flatMapResult)

    // flatMap 和下面的代码是等价的 -> 先映射，再扁平化
    val flatMapResult_2 = List(List(1, 2), List(3, 4)).map(f => f.map(_ * 2)).flatten
    println("flatMapResult_2: " + flatMapResult_2)
  }

  def timesTwo(i: Int): Int = i * 2

  def isEven(i: Int): Boolean = i % 2 == 0
}