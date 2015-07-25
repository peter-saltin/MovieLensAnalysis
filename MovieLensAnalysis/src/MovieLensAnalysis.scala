import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

/**
 * @author peter saltin 
 */
object MovieLensAnalysis {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Movielens Analysis")
    val sc = new SparkContext(conf)
    val hadoopConf=sc.hadoopConfiguration
      .set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      .set("fs.s3n.awsAccessKeyId",sys.env("AWS_ACCESS_KEY_ID"))
      .set("fs.s3n.awsSecretAccessKey",sys.env("AWS_SECRET_ACCESS_KEY"))
    


    val pathFile = "s3n://saltin1/input/ampcamp/movielens/large/ratings.dat"
    val dataFile = sc.textFile(pathFile, 2).cache()
    println(dataFile.count() + " <-- number of ratings")
    println("----------------------")
  }
}


