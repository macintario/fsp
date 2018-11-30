package org.example

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions._

object ej1 {
  case class Point(x: Double, y: Double)
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(Point(1, 2), Point(3, 4), Point(5, 6))
    val nds = ds.filterWith { //se crea nuevo DataSet para ver el resultado de las transformaciones
      case Point(x, _) => x > 1
    }.reduceWith {
      case (Point(x1, y1), (Point(x2, y2))) => Point(x1 + y1, x2 + y2)
    }.mapWith {
      case Point(x, y) => (x, y)
    }.flatMapWith {
      case (x, y) => Seq("x" -> x, "y" -> y)
    }.groupingBy {
      case (id, value) => id
    }
    println(ds.collect())  //se agregó para ver DS
    println(nds.first(1).collect()) //se agregó para ver el resultado de las transformaciones
  }
}