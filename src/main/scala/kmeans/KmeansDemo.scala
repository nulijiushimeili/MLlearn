package kmeans

/**
  * 使用Kmeans++计算出数据的中心点
  */

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object KmeansDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("K-Means++")
    val sc = new SparkContext(conf)

    val data = sc.textFile("D:\\mycode1\\program\\spark\\MLlearn\\data\\kmeans_demo.txt")
    val parsedData = data.map(s => Vectors.dense(s.split(" ").map(_.trim.toDouble))).cache()

    //设置簇的个数为3
    val numClusters = 3
    //设置迭代的次数
    val numIterations = 20
    //运行10次选出最优解
    val runs = 10
    //设置k选取方式为K-means++
    val initMode = "k-mean||"
    val clusters = new KMeans()
      .setInitializationMode(initMode)
      .setK(numClusters)
      .setMaxIterations(numIterations)
      .run(parsedData)

    //打印出测试集属于哪个簇
    println(parsedData.map(v => v.toString +
      " in " + clusters.predict(v))
      .collect()
      .mkString("\n"))

    //损失函数
    val WSSSE = clusters.computeCost(parsedData)
    println("平方误差的总和:" + WSSSE)

    //
    val a21 = clusters.predict(Vectors.dense(1.2,1.3))
    val a22 = clusters.predict(Vectors.dense(4.1,4.2))

    //打印出中心点
    println("Clustercenters:")
    for(center <- clusters.clusterCenters){
      println(" "+ center)
    }

    println("Prediction of (1.2,1.3) --> " + a21)
    println("Prediction of (4.1,4.2) --> " + a22)

  }
}
