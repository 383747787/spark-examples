package spark.example.streaming

/**
 * Created by juanpi on 2015/7/17.
 */
trait SemiGroup[T]{
  def op(a: T, b: T): T
}

trait Monoid[T] extends SemiGroup[T]{
  def zero: T

  implicit object IntAdditionMonoid extends Monoid[Int]{
    def op(a: Int, b: Int): Int = a + b
    def zero: Int = 0
  }
}


class MapSemiGroup[K,V]()(implicit sg1: SemiGroup[V]) extends SemiGroup[Map[K,V]]{
  //We are aggregating where the initial map is one of the maps and we loop through key values of other one and combine.
  //This way any keys that don't appear in the looping map are there already,all keys that appear in both are overwritten
  def op(iteratingMap: Map[K,V], startingMap: Map[K,V]): Map[K,V] = iteratingMap.aggregate(startingMap)({
  (currentMap: Map[K,V], kv: (K,V)) => {
  val newValue: V = startingMap.get(kv._1).map(v => sg1.op(v, kv._2)).getOrElse(kv._2)
  currentMap + (kv._1 -> newValue)
}
},
  //This is the combine part (if done in parallel, could have two different maps that need to be combined) this assumes that all keys are already combined....
{
  (mapOne: Map[K,V], mapTwo: Map[K,V]) => mapOne ++ mapTwo
}
  )
}

