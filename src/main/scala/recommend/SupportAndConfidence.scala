package recommend

import org.apache.spark.sql.SparkSession

/**
  * spark计算支持度和置信度
  */

/**
  * TODO 这个案例中的数据是全部进行计算的,可以将单个value值过滤一遍,
  * 比如有些公司只有很少的人喜欢,那么过滤掉这部分数据,会大大减小计算量
  */
object SupportAndConfidence {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("TestSuportAndConfidence")
      //      .config("spark.sql.warehouse.dir","")
      .getOrCreate()
    import spark.implicits._

    //测试数据
    //左边是a,b,c三个用户
    //右边代表用户喜欢的公司
    val testData = Array(
      ("a", "google"),
      ("a", "apple"),
      ("a", "ali"),
      ("b", "google"),
      ("b", "apple"),
      ("c", "google")
    )

    //加载数据
    val data = spark.sparkContext.parallelize(testData)

    //统计每个公司都有多少人喜欢
    val companyCountRDD = data.map(a => (a._2, 1)).reduceByKey(_ + _)

    companyCountRDD.collect.foreach(println)

    //    (ali,1)
    //    (google,3)
    //    (apple,2)

    //使用cartesian这个方法,得到每两个公司所有在一起组合
    val cartesianRDD = companyCountRDD.cartesian(companyCountRDD)
    cartesianRDD.collect.foreach(println)

    //    ((ali,1),(ali,1))
    //    ((ali,1),(google,3))
    //    ((ali,1),(apple,2))
    //    ((google,3),(ali,1))
    //    ((google,3),(google,3))
    //    ((google,3),(apple,2))
    //    ((apple,2),(ali,1))
    //    ((apple,2),(google,3))
    //    ((apple,2),(apple,2))

    //使用filter()方法,只留下喜欢a>喜欢b的组合
    val cartesianFilterRDD = cartesianRDD.filter(c => c._1._2 > c._2._2)

    cartesianFilterRDD.collect.foreach(println)
    //    ((google,3),(ali,1))
    //    ((google,3),(apple,2))
    //    ((apple,2),(ali,1))

    //将结果整理一下
    val cartesianRDD2 = cartesianFilterRDD
      .map(x => ((x._1._1, x._2._1), (x._1._2, x._2._2)))
    cartesianRDD2.collect.foreach(println)
    //    ((google,ali),(3,1))
    //    ((google,apple),(3,2))
    //    ((apple,ali),(2,1))

    //通过groupByKey()这个方法得到单个用户喜欢的公司的组合
    val userCompanyRDD = data.groupByKey().cache()
    val companiesGroup = userCompanyRDD.map(_._2)
    companiesGroup.collect.foreach(println)
    //    CompactBuffer(google, apple, ali)
    //    CompactBuffer(google, apple)
    //    CompactBuffer(google)

    //将公司的两两组合在一起
    val meanWhileRDD = companiesGroup
      .flatMap(item => item.flatMap(a => item.map(b => (a, b))).filter(x => x._1 > x._2))
    meanWhileRDD.collect.foreach(println)
    //    (google,apple)
    //    (google,ali)
    //    (apple,ali)
    //    (google,apple)

    //将每个公司的组合进行统计
    val meanWhileRDD2 = meanWhileRDD.map((_, 1)).reduceByKey(_ + _)

    //获取用户的总数
    val userNum = userCompanyRDD.count()

    val calRDD = cartesianRDD2.join(meanWhileRDD2)

    calRDD.collect.foreach(println)
    //A公司,B公司,((喜欢A公司的人数,喜欢B公司的人数),同时喜欢AB两个公司的人数)
    //    ((google,ali),((3,1),1))
    //    ((apple,ali),((2,1),1))
    //    ((google,apple),((3,2),2))

    //计算结果
    val resRDD = calRDD.map { t =>
      //a公司的名称
      val aCompany = t._1._1
      //b公司的名称
      val bCompany = t._1._2
      //喜欢a公司的人数
      val aCount = t._2._1._1
      //喜欢b公司的人数
      val bCount = t._2._1._2
      //同时喜欢a,b两个公司的人数
      val aAndbCount = t._2._2 * 1.0
      //a公司,b公司,支持度,a->b的置信度,b->a的置信度
      (aCompany, bCompany, aAndbCount / userNum, aAndbCount / aCount, aAndbCount / bCount)
    }
    resRDD.collect.foreach(println)
//    (google,ali,0.3333333333333333,0.3333333333333333,1.0)
//    (apple,ali,0.3333333333333333,0.5,1.0)
//    (google,apple,0.6666666666666666,0.6666666666666666,1.0)

    //设置支持度的阈值时1%,置信度的阈值是50%
    val support = 0.01
    val confidence = 0.5

    //过滤掉支持度和置信度低的数据
    val res = resRDD.filter(x => x._3 > support &&
      x._4 > confidence && x._5 > confidence)
    res.collect().foreach(println)
//    (google,apple,0.6666666666666666,0.6666666666666666,1.0)

  }
}
