import ItemSimilarity.{ItemPref, ItemSimi, UserRecomm}
import org.apache.spark.sql.{Row, SparkSession}

import scala.math._



object I2iTest {



  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("test01")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setCheckpointDir("hdfs://192.168.100:9000/data/spark_checkpoint")
    import spark.implicits._

    /**
      * 数据准备
      */
    // 1.1 读取item配置表
    val item_conf_path = "hdfs://192.168.100:9000/data/Recommended_Algorithm_Action/I2I/movies.csv"
    val item_conf_df = spark.read.options(Map(("delimiter",","),("header", "true"))).csv(item_conf_path)
    val item_id2title_map = item_conf_df.select("movieId","title").collect().map(row => (row(0).toString(), row(1).toString())).toMap
    val item_id2genres_map = item_conf_df.select("movieId", "genres").collect().map(row =>(row(0).toString(),row(1).toString())).toMap


    // 读取用户的行为
    val user_rating_path = "hdfs://192.168.100:9000/data/Recommended_Algorithm_Action/I2I/ratings.csv"
    val user_rating_df =spark.read.options(Map(("delimiter",","),("header","true"))).csv(user_rating_path)

    user_rating_df.dtypes

    val  user_ds = user_rating_df.map {
      case Row(userId: String, movieId: String, rating:  String, timestamp: String)  =>
        ItemPref(userId, movieId, rating.toDouble)
    }

    user_ds.show(10)
    user_ds.cache()
    user_ds.count()


    /**
      * 2.相似度计算
      */
    val item_id2title_map_BC = spark.sparkContext.broadcast(item_id2title_map)
    val item_id2genres_map_BC = spark.sparkContext.broadcast(item_id2genres_map)


    // 2.1同现相似度
    val items_similar_coocurrence = ItemSimilarity.CoccurrenceSimilarity(user_ds).map{
      case ItemSimi(itemidI: String, itemidJ: String, similar: Double) =>
        val i_title = item_id2title_map_BC.value.getOrElse(itemidI,"")
        val j_title = item_id2title_map_BC.value.getOrElse(itemidJ,"")
        val i_genres = item_id2genres_map_BC.value.getOrElse(itemidI,"")
        val j_genres = item_id2genres_map_BC.value.getOrElse(itemidJ,"")
        (itemidI, itemidJ, similar, i_title, j_title, i_genres, j_genres)
    }.withColumnRenamed("_1","itemidI").
      withColumnRenamed("_2","itemidJ").
      withColumnRenamed("_3","similar").
      withColumnRenamed("_4", "i_title").
      withColumnRenamed("_5","j_title").
      withColumnRenamed("_6","i_genres").
      withColumnRenamed("_7","j_genres")


    items_similar_coocurrence.columns

    // 结果打印
    items_similar_coocurrence.cache()
    items_similar_coocurrence.count()

    items_similar_coocurrence.
      orderBy($"itemidI".asc, $"similar".desc).
      select("itemidI","itemidJ","i_title","j_title","i_genres","j_genres","similar").
      show(20)


    // 2.2 Cosine相似度
    val items_similar_cosine = ItemSimilarity.CosineSimilarity(user_ds).map{
      case ItemSimi(itemidI: String, itemidJ: String, similar: Double) =>
        val i_title = item_id2title_map_BC.value.getOrElse(itemidI,"")
        val j_title = item_id2title_map_BC.value.getOrElse(itemidJ,"")
        val i_genres = item_id2genres_map_BC.value.getOrElse(itemidI,"")
        val j_genres = item_id2genres_map_BC.value.getOrElse(itemidJ,"")
        (itemidI, itemidJ, similar, i_title, j_title, i_genres, j_genres)
    }.withColumnRenamed("_1","itemidI").
      withColumnRenamed("_2","itemidJ").
      withColumnRenamed("_3","similar").
      withColumnRenamed("_4", "i_title").
      withColumnRenamed("_5","j_title").
      withColumnRenamed("_6","i_genres").
      withColumnRenamed("_7","j_genres")


    items_similar_cosine.columns

    // 结果打印
    items_similar_cosine.cache()
    items_similar_cosine.count()

    items_similar_cosine.
      orderBy($"itemidI".asc, $"similar".desc).
      select("itemidI","itemidJ","i_title","j_title","i_genres","j_genres","similar").
      show(20)


    // 2.3 欧几里得距离相似度
    val items_similar_euclidean = ItemSimilarity.EuclideanDistanceSimilarity(user_ds).map{
      case ItemSimi(itemidI: String, itemidJ: String, similar: Double) =>
        val i_title = item_id2title_map_BC.value.getOrElse(itemidI,"")
        val j_title = item_id2title_map_BC.value.getOrElse(itemidJ,"")
        val i_genres = item_id2genres_map_BC.value.getOrElse(itemidI,"")
        val j_genres = item_id2genres_map_BC.value.getOrElse(itemidJ,"")
        (itemidI, itemidJ, similar, i_title, j_title, i_genres, j_genres)
    }.withColumnRenamed("_1","itemidI").
      withColumnRenamed("_2","itemidJ").
      withColumnRenamed("_3","similar").
      withColumnRenamed("_4", "i_title").
      withColumnRenamed("_5","j_title").
      withColumnRenamed("_6","i_genres").
      withColumnRenamed("_7","j_genres")


    items_similar_euclidean.columns

    // 结果打印
    items_similar_euclidean.cache()
    items_similar_euclidean.count()

    items_similar_euclidean.
      orderBy($"itemidI".asc, $"similar".desc).
      select("itemidI","itemidJ","i_title","j_title","i_genres","j_genres","similar").
      show(20)

    /**
      * 推荐计算
      */
    // 推荐结果计算
    // 3.1 同现相似度推荐


    val coocurrence = items_similar_coocurrence.select("itemidI","itemidJ","similar").map{
      case Row(itemidI:String,itemidJ:String,similar:Double) =>
        ItemSimi(itemidI, itemidJ, similar)
    }

    val user_predictr_coocurrence = ItemSimilarity.Recommend(coocurrence,user_ds).map{
      case UserRecomm(userid: String, itemid: String, pref: Double) =>
        val title = item_id2title_map_BC.value.getOrElse(itemid,"")
        val genres = item_id2genres_map_BC.value.getOrElse(itemid,"")

        (userid, itemid, title, genres,pref)
    }.withColumnRenamed("_1","userid").
      withColumnRenamed("_2","itemid").
      withColumnRenamed("_3","title").
      withColumnRenamed("_4", "genres").
      withColumnRenamed("_5","pref")

    user_predictr_coocurrence.columns

    // 结果打印
    user_predictr_coocurrence.cache()
    user_predictr_coocurrence.count()

    user_predictr_coocurrence.
      orderBy($"userid".asc, $"pref".desc).
      show(20)

    // 3.2cosine相似度推荐

    val cosine = items_similar_cosine.select("itemidI","itemidJ","similar").map{
      case Row(itemidI:String,itemidJ:String,similar:Double) =>
        ItemSimi(itemidI, itemidJ, similar)
    }

    val user_predictr_cosine = ItemSimilarity.Recommend(cosine,user_ds).map{
      case UserRecomm(userid: String, itemid: String, pref: Double) =>
        val title = item_id2title_map_BC.value.getOrElse(itemid,"")
        val genres = item_id2genres_map_BC.value.getOrElse(itemid,"")

        (userid, itemid, title, genres,pref)
    }.withColumnRenamed("_1","userid").
      withColumnRenamed("_2","itemid").
      withColumnRenamed("_3","title").
      withColumnRenamed("_4", "genres").
      withColumnRenamed("_5","pref")

    user_predictr_cosine.columns

    // 结果打印
    user_predictr_cosine.cache()
    user_predictr_cosine.count()

    user_predictr_cosine.
      orderBy($"userid".asc, $"pref".desc).
      show(20)

    // 3.3欧几里得距离相似度推荐

    val euclidean = items_similar_euclidean.select("itemidI","itemidJ","similar").map{
      case Row(itemidI:String,itemidJ:String,similar:Double) =>
        ItemSimi(itemidI, itemidJ, similar)
    }

    val user_predictr_euclidean = ItemSimilarity.Recommend(euclidean,user_ds).map{
      case UserRecomm(userid: String, itemid: String, pref: Double) =>
        val title = item_id2title_map_BC.value.getOrElse(itemid,"")
        val genres = item_id2genres_map_BC.value.getOrElse(itemid,"")

        (userid, itemid, title, genres,pref)
    }.withColumnRenamed("_1","userid").
      withColumnRenamed("_2","itemid").
      withColumnRenamed("_3","title").
      withColumnRenamed("_4", "genres").
      withColumnRenamed("_5","pref")

    user_predictr_euclidean.columns

    // 结果打印
    user_predictr_euclidean.cache()
    user_predictr_euclidean.count()

    user_predictr_euclidean.
      orderBy($"userid".asc, $"pref".desc).
      show(20)


    // 推荐结果保存
    val table_date = 20181025
    val recommend_table = "table_i2i_recommend_result"
    user_predictr_coocurrence.createOrReplaceTempView("df_to_hive_table")

    val insertsql1 = s"insert overwrite table ${recommend_table} partition(ds=${table_date}) select userid,itemid,pref from df_to_hive_table"
    println(insertsql1)
    spark.sql(insertsql1)

  }

}
