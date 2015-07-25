import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}
import scala.io.Source

/**
 * @author peter saltin 
 */
object MovieLensAnalysis {

  def main(args: Array[String]) {
    val sparkHome = "/root/spark"
    val master = Source.fromFile("/root/spark-ec2/cluster-url").mkString.trim
    val masterHostname = Source.fromFile("/root/spark-ec2/masters").mkString.trim    
    val conf = new SparkConf()
      .setAppName("Movielens Analysis")
      .set("spark.executor.memory", "6g")
      .setMaster(master)
      .setSparkHome(sparkHome)
    val sc = new SparkContext(conf)
    val hadoopConf=sc.hadoopConfiguration
    hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoopConf.set("fs.s3n.awsAccessKeyId",sys.env("AWS_ACCESS_KEY_ID"))
    hadoopConf.set("fs.s3n.awsSecretAccessKey",sys.env("AWS_SECRET_ACCESS_KEY"))

    val pathFile = "s3n://saltin1/input/ampcamp/movielens/large/ratings.dat"
    val dataFile = sc.textFile(pathFile, 2).cache()
    //println(dataFile.count() + " <-- number of ratings")
    //println("----------------------")
    val movieLensHomeDir = "s3n://saltin1/input/ampcamp/movielens/large"
    val ratings = sc.textFile(movieLensHomeDir + "/ratings.dat").cache.map { line =>
    val fields = line.split("::")
    // format: (timestamp % 10, Rating(userId, movieId, rating))
      (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }
    
     //create rdd of movies for easy join with ratings
    val moviesRDD = sc.textFile(movieLensHomeDir + "/movies.dat").cache.map { line =>
    val fields = line.split("::")
    // format: (movieId, movieName)
        (fields(0).toInt, fields(1))
      }
      
     //take 10
    /*
     ratings.
       map( x => (x._2.product, 1)).
       reduceByKey(_+_).
       leftOuterJoin(moviesRDD).
       sortByKey(true,2).
       take(10).
       foreach(println)*/
   /*
     ratings.
       map( x => ((x._2.product),(x._2.rating, 1)  ) ).
       reduceByKey( (x,y) =>  ( x._1 + y._1  , x._2 + y._2 )      
       ).leftOuterJoin(moviesRDD).take(10).foreach(println)
       
       ratings.
       map( x => ((x._2.product),(x._2.rating, 1)  ) ).
       reduceByKey( (x,y) =>  ( x._1 + y._1  , x._2 + y._2 )      
       ).map(x => (x._1, x._2._1/x._2._2, x._2._2 )).take(10).foreach(println)   */  
       /* get top 100 among movies with more than 500 votes*/
       ratings.
         map( x => ((x._2.product),(x._2.rating, 1)  ) ).
         reduceByKey( (x,y) =>  ( x._1 + y._1  , x._2 + y._2 ) ).
         map(x => (x._1, (x._2._1/x._2._2, x._2._2) )).
         leftOuterJoin(moviesRDD).
         map(x => (x._2._1._1, (x._1, x._2._1._1, x._2._1._2, x._2._2) ) ).
         sortByKey(false,2).
         filter(x => x._2._3>500). /* collect ? */
         map(x => x._2).
         take(100).foreach(println)          
  


  }
}


