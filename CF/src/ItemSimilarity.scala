import org.apache.spark.sql.functions.sum

import scala.math._
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 相似度计算和推荐计算
  */
object ItemSimilarity extends Serializable {


  import org.apache.spark.sql.functions._


  case class ItemPref(val userid:String,val itemid:String,val pref:Double)

  case class ItemSimi(val itemidI:String,val itemidJ:String,val similar:Double)

  case class UserRecomm(val userid:String,val itemid:String,val pref:Double)

  case class ItemAssociation(val itemidI:String,val itemidJ:String,val support:String, val confidence:Double, val lift:Double, val similar:Double)

  /**
    * 关联规则计算
    * @param user_ds
    * @return RDD[ItemAssociation]
    */
  def AssociationRules(user_ds: Dataset[ItemPref]): Dataset[ItemAssociation] = {
    import user_ds.sparkSession.implicits._
    // 1.(用户:物品) => (用户:(物品集合))
    val user_ds1 = user_ds.groupBy("userid").agg(collect_set("itemid")).withColumnRenamed("collect_set(itemid)", "itemis_set")

    // 2.物品:物品 上三角数据
    val user_ds2 = user_ds1.flatMap { row =>
      val itemlist = row.getAs[mutable.WrappedArray[String]](1).toArray.sorted
      val result = new ArrayBuffer[(String, String, Double)]()
      for (i <- 0 to itemlist.length - 2) {
        for (j <- i + 1 to itemlist.length - 1) {
          result += ((itemlist(i), itemlist(j), 1.0))
        }
      }
      result
    }.withColumnRenamed("_1", "itemidI").withColumnRenamed("_2", "itemidJ").withColumnRenamed("_3", "score")

    // 3.计算物品与物品, 上三角,同现频次
    val user_ds3 = user_ds2.groupBy("itemidI", "itemidJ").agg(sum("score").as("sumIJ"))

    // 4.计算物品总共出现的频次
    val user_ds0 = user_ds.withColumn("score", lit(1)).groupBy("itemid").agg(sum("score").as("score"))
    val user_all = user_ds1.count()


    // todo 计算支持度
    val user_ds4 = user_ds3.select("itemidI", "itemidJ", "sumIJ").union(user_ds3.select($"itemidJ".as("itemidI"), $"itemidI".as("itemidJ"),
      $"sumIJ")).withColumn("support", $"sumIJ" / user_all.toDouble)

    // todo 计算置信度
    val user_ds5 = user_ds4.join(user_ds0.withColumnRenamed("itemid","itemidI").withColumnRenamed("score","sumI"),"itemidI")
      .withColumn("confidence",$"sumIJ"/$"sumI")

    // todo 计算提升度
    val user_ds6 = user_ds5.join(user_ds0.withColumnRenamed("itemid","itemidJ").withColumnRenamed("score","sumJ"),"itemidJ")
        .withColumn("lift",$"confidence" / ($"sumJ"/user_all.toDouble))

    // todo 计算同现相似度 欧几里得相似度
    val user_ds8 = user_ds6.withColumn("similar", col("sumIJ") / sqrt(col("sumI") * col("sumJ")))

    // 结果返回
    val out = user_ds8.select("itemidI","itemidJ","support","confidence","lift","similar").map{row =>
      val itemidI = row.getString(0)
      val itemidJ = row.getString(1)
      val support = row.getString(2)
      val confidence = row.getDouble(3)
      val lift = row.getDouble(4)
      var similar = row.getDouble(5)
      ItemAssociation(itemidI,itemidJ,support,confidence,lift,similar)
    }
    out

  }


  /**
    * 计算 Cosine 相似度
    * @param user_ds
    * @return
    */
  def CosineSimilarity(user_ds: Dataset[ItemPref]): Dataset[ItemSimi] = {
    import user_ds.sparkSession.implicits._

    // 1.数据准备
    val user_ds1 = user_ds.withColumn("iv", concat_ws(":",$"itemid",$"pref")).groupBy("userid").agg(collect_set("iv"))
      .withColumnRenamed("collect_set(iv)","item_set").select("userid","item_set")

    // 2.物品 物品上三角数据
    val user_ds2 = user_ds1.flatMap{ row =>
      val itemlist = row.getAs[mutable.WrappedArray[String]](1).toArray.sorted
      val result = new ArrayBuffer[(String, String, Double, Double)]()
      for(i <- 0 to itemlist.length - 2){
        for (j  <- i+1 to itemlist.length -1){
          result += ((itemlist(i).split(":")(0), itemlist(j).split(":")(0), itemlist(i).split(":")(1).toDouble, itemlist(j).split(":")(1).toDouble))
        }
      }
      result

    }.withColumnRenamed("_1", "itemidI").withColumnRenamed("_2", "itemidJ").withColumnRenamed("_3", "scoreI").withColumnRenamed("_4", "socreJ")

    // 按照公式计算相似度
    val user_ds3 = user_ds2.withColumn("cnt",lit(1)).groupBy("itemidI", "itemidJ").agg(
      sum(($"scoreI" * $"socreJ")).as("sum_xy"),
      sum(($"scoreI" * $"socreI")).as("sum_x"),
      sum(($"scoreJ" * $"socreJ")).as("sum_y")
    ).withColumn("result",$"sum_xy"/(sqrt($"sum_x")) * sqrt($"sum_y"))


    // 上下三角合并
    val user_ds8 = user_ds3.select("itemidI","itemidJ","result").union(user_ds3.select($"itemidJ".as("itemidI"),$"itemidI".as("itemidJ"),$"result"))


    val out = user_ds8.select("itemidI","itemidJ","result").map{ row =>
      val itemidI = row.getString(0)
      val itemidJ = row.getString(1)
      val similar = row.getDouble(2)

      ItemSimi(itemidI,itemidJ,similar)
    }
    out

  }

  /**
    * 欧几里得相似度计算
    * @param user_ds
    * @return
    */
  def EuclideanDistanceSimilarity(user_ds:  Dataset[ItemPref]): Dataset[ItemSimi] = {
    import user_ds.sparkSession.implicits._
    // 1.准备数据
    val user_ds1 = user_ds.withColumn("iv", concat_ws(":",$"itemid",$"pref")).groupBy("userid").agg(collect_set("iv"))
        .withColumnRenamed("collect_set(iv)","itemid_set")
        .select("userid","itemid_set")

    // 2.物品,物品 上三角数据
    val user_ds2 = user_ds1.flatMap{ row =>
      val itemlist = row.getAs[mutable.WrappedArray[String]](1).toArray.sorted
      val result = new ArrayBuffer[(String, String, Double, Double)]()
      for(i <- 0 to itemlist.length - 2){
        for (j  <- i+1 to itemlist.length -1){
          result += ((itemlist(i).split(":")(0), itemlist(j).split(":")(0), itemlist(i).split(":")(1).toDouble, itemlist(j).split(":")(1).toDouble))
        }
      }
      result

    }.withColumnRenamed("_1", "itemidI").withColumnRenamed("_2", "itemidJ").withColumnRenamed("_3", "scoreI").withColumnRenamed("_4", "socreJ")

    // 3.按照公式极端相似度
    val user_ds3 = user_ds2.withColumn("cnt",lit(1))
      .groupBy("itemidI", "itemidJ")
      .agg(sqrt(sum(($"sorceI" - $"socreJ") * ($"sorceI" - $"socreJ"))).as("dist"), sum($"cnt").as("cntsum"))
      .withColumn("result",$"cntsum" / lit(1.0) + $"dist")

    // 4.上下三角合并
    val user_ds8 = user_ds3.select("itemidI","itemidJ","result").union(user_ds3.select($"itemidJ".as("itemidI"), $"itemidI".as("itemidJ"),$"result"))

    val out = user_ds8.select("itemidI","itemidJ","result").map{ row =>
      val itemidI = row.getString(0)
      val itemidJ = row.getString(1)
      val similar = row.getDouble(2)
      ItemSimi(itemidI, itemidJ, similar)
    }
    out
  }

  /**
    * 同现相似度计算
    * @param user_ds
    * @return
    */
  def CoccurrenceSimilarity(user_ds: Dataset[ItemPref]): Dataset[ItemSimi] = {
    import user_ds.sparkSession.implicits._


    // 1.(用户:物品) => (用户:物品集合)
    val user_ds1 = user_ds.groupBy("userid").agg(collect_set("itemid")).withColumnRenamed("collect_set(itemid)","itemid_set")

    val user_ds2 = user_ds1.flatMap{ row =>
      val itemlist = row.getAs[mutable.WrappedArray[String]](1).toArray.sorted
      val result = new ArrayBuffer[(String, String, Double)]()
      for(i <- 0 to itemlist.length - 2){
        for (j  <- i+1 to itemlist.length -1){
          result += ((itemlist(i), itemlist(j),1.0))
        }
      }
      result

    }.withColumnRenamed("_1", "itemidI").withColumnRenamed("_2", "itemidJ").withColumnRenamed("_3", "score")

    // 3.计算物品与物品 上三角, 同此频次
    val user_ds3 = user_ds2.groupBy("itemidI", "itemidJ").agg(sum("score").as("sumIJ"))

    // 4.计算物品出现的频次
    val user_ds0 = user_ds.withColumn("score",lit(1)).groupBy("itemid").agg(sum("score").as("score"))

    // 5.计算同现相似度
    val user_ds4 = user_ds3.join(user_ds0.withColumnRenamed("itemid","itemidJ").withColumnRenamed("score", "sumJ").select("itemidJ","sumJ"),"itemidJ")

    val user_ds5 = user_ds4.join(user_ds0.withColumnRenamed("itemid","itemidI").withColumnRenamed("score","sumI").select("itemidI","sumI"),"itemidI")

    // 根据公式计算
    val user_ds6 = user_ds5.withColumn("result", col("sumIJ") / sqrt(col("sumI") * col("sumJ")))

    // 6.上三角合并
    println(s"user_ds6.count(): ${user_ds6.count()}")
    val user_ds8 = user_ds6.select("itemidI", "itemidJ", "result").union(user_ds6.select($"itemidJ".as("itemidI"),$"itemidI".as("itemidJ"), $"result"))

    println(s"user_ds8.count(): ${user_ds8.count()}")

    // 7.结果返回
    val out = user_ds8.select("itemidI", "itemidJ", "result").map{ row =>
      val itemidI  = row.getString(0)
      val itemidJ =  row.getString(1)
      val similar = row.getDouble(2)
      ItemSimi(itemidI, itemidJ, similar)

    }
    out
  }

  /**
    * 计算推荐结果
    * @param items_similar
    * @param user_prefer
    * @return
    */
  def Recommend(items_similar: Dataset[ItemSimi], user_prefer: Dataset[ItemPref]): Dataset[UserRecomm] = {
    import user_prefer.sparkSession.implicits._

    // 1. 数据准备
    val item_similar_ds1 = items_similar
    val user_prefer_ds1 = user_prefer

    // 2. 根据用户的item召回相似物品
    val user_prefer_ds2 = item_similar_ds1.join(user_prefer_ds1, $"itemidI" === $"itemid","inner")
    // user_prefer_ds2.show
    // 3.计算召回用户物品的得分
    val user_prefer_ds3 = user_prefer_ds2.withColumn("score",col("pref") * col("similar")).select("userid","itemidJ","score")
    // 4.得分汇总
    val user_prefer_ds4 = user_prefer_ds3.groupBy("userid","itemidJ").agg(sum("score").as("score")).withColumnRenamed("itemidJ","itemid")
    // 5. 用户得分排序,去除用户已评分物品
    val user_prefer_ds5 = user_prefer_ds4.join(user_prefer_ds1,Seq("userid","itemid"),"left").where("pref is null")
    // 结果返回
    val out1 = user_prefer_ds5.select("userid", "itemid", "score").map{ row =>
      val userid = row.getString(0)
      val itemid = row.getString(1)
      val pref = row.getDouble(2)
      UserRecomm(userid, itemid, pref)
    }
    out1
  }






}
