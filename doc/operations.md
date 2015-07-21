//used in spark-shell

val data = Array(1,2,3,4,5)
val distData = sc.parallelize(data)

//map
val mapData = distData.map(_+1)
mapData.collect()
//res2: Array[Int] = Array(2, 3, 4, 5, 6)

//filter
val filterData = distData.filter(_%2 == 1)
filterData.collect()
//res6: Array[Int] = Array(1, 3, 5)

//flatMap
val flatMapData = distData.flatMap(i => Seq(i,i))
flatMapData.collect()
res10: Array[Int] = Array(1, 1, 2, 2, 3, 3, 4, 4, 5, 5)

//mapPartitions
val pData = sc.parallelize(1 to 9, 3)
def myfunc[T](iter: Iterator[T]):Iterator[(T,T)] = {
    var res = List[(T,T)]()
    var pre = iter.next 
    while (iter.hasNext){
        val cur = iter.next
        res .::= (pre, cur) 
        pre = cur;
    }
    res.iterator
}
val mapPartData = pData.mapPartitions(myfunc)
mapPartData.collect()
//res5: Array[(Int, Int)] = Array((2,3), (1,2), (5,6), (4,5), (8,9), (7,8))

//mapPartitionsWithIndex
val pData = sc.parallelize(1 to 9, 3)
def myfuncIndex[T](idx:Int,iter:Iterator[T]):Iterator[(Int,T,T)] = {
    var res = List[(Int,T,T)]()
    var pre = iter.next 
    while(iter.hasNext){
        val cur = iter.next
        res .::= (idx,pre, cur) 
        pre = cur;
    }
    res.iterator
} 
 pData.mapPartitionsWithIndex(myfuncIndex).collect()
//res12: Array[(Int, Int, Int)] = Array((0,2,3), (0,1,2), (1,5,6), (1,4,5), (2,8,9), (2,7,8))

//sample 
pData.sample(true,2,2).collect()
//res28: Array[Int] = Array(3, 4, 5, 5, 5, 6, 7, 7, 9, 9, 9, 9)

//union
val pData1=sc.parallelize(5 to 15,2)
pData.union(pData1).collect()
//res30: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)

//intersection
pData.intersection(pData1).collect
res31: Array[Int] = Array(6, 9, 7, 8, 5)

//distinct
val pData = sc.parallelize(Seq(1,1,2,4,4,5,5))
pData.distinct().collect
//res34: Array[Int] = Array(4, 2, 1, 5)

//groupByKey
val pData = sc.parallelize(List((1,2),(3,4),(3,6)))
pData.groupByKey().collect
//res38: Array[(Int, Iterable[Int])] = Array((1,CompactBuffer(2)), (3,CompactBuffer(4, 6)))

//reduceByKey
val pData = sc.parallelize(List((1,2),(3,4),(3,6)))
pData.reduceByKey((x,y) => x+y).collect

//aggregateByKey
val pData = sc.parallelize(List((1,2),(3,4),(3,6)))
pData.aggregateByKey(1)(_+_,_+_).collect
//res40: Array[(Int, Int)] = Array((1,3), (3,11))
def seq(a:Int,b:Int):Int = {
    println("seq:" + a + "\t" + b)
    math.max(a,b)
}
def comb(a:Int,b:Int):Int = {
    println("comb:" + a + "\t" + b)
    a + b
}
pData.aggregateByKey(1)(seq,comb).collect

//sortByKey
val pData = sc.parallelize(List((1,2),(3,4),(3,6),(2,5)))
pData.sortByKey().collect
//res42: Array[(Int, Int)] = Array((1,2), (2,5), (3,4), (3,6))
pData.sortByKey(false).collect
res43: Array[(Int, Int)] = Array((3,4), (3,6), (2,5), (1,2))

//join
val p1Data = sc.parallelize(List((1,2),(3,4),(3,6),(2,5),(4,2)))
val p2Data = sc.parallelize(List((1,20),(3,40),(2,50)))
p1Data.join(p2Data).collect
//res45: Array[(Int, (Int, Int))] = Array((2,(5,50)), (1,(2,20)), (3,(4,40)), (3,(6,40)))
//leftOuterJoin, rightOuterJoin, and fullOuterJoin.
p1Data.leftOuterJoin(p2Data).collect
//res47: Array[(Int, (Int, Option[Int]))] = Array((4,(2,None)), (2,(5,Some(50))), (1,(2,Some(20))), (3,(4,Some(40))), (3,(6,Some(40))))

//cogroup
p1Data.cogroup(p2Data).collect
//res48: Array[(Int, (Iterable[Int], Iterable[Int]))] = Array((4,(CompactBuffer(2),CompactBuffer())), (2,(CompactBuffer(5),CompactBuffer(50))), (1,(CompactBuffer(2),CompactBuffer(20))), (3,(CompactBuffer(4, 6),CompactBuffer(40))))

//cartesian
val p1Data = sc.parallelize(Array(1,2,3))
val p2Data = sc.parallelize(Array(4,5,6))
p1Data.cartesian(p2Data).collect
//res49: Array[(Int, Int)] = Array((1,4), (1,5), (1,6), (2,4), (3,4), (2,5), (2,6), (3,5), (3,6))

//pipe
val pData = sc.parallelize(Array("one","two two","three three three"))
pData.pipe("wc -w").collect
//res59: Array[String] = Array(1, 5)

//coalesce Useful for running operations more efficiently after filtering down a large dataset.
val pData = sc.parallelize(1 to 100,10)
pData.partitions.size
//res65: Int = 10
pData.coalesce(3).partitions.size
//res66: Int = 3

//repartition


//Actions
val pData = sc.parallelize(1 to 10)
pData.reduce(_+_)
//res67: Int = 55

pData.collect
//res68: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

pData.count
//res69: Long = 10

pData.first
res70: Int = 1

pData.take(5)
//res71: Array[Int] = Array(1, 2, 3, 4, 5)

val pData = sc.parallelize(List((1,2),(2,3),(2,4)))
pData.countByKey()
//res73: scala.collection.Map[Int,Long] = Map(2 -> 2, 1 -> 1)

//foreach
val accum = sc.accumulator(0,"My Accumulator")
val pData = sc.parallelize(1 to 10)
pData.foreach(x => accum += x)
accum.value
//scala> accum.value
//res75: Int = 55

