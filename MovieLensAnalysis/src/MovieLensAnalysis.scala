import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}
import scala.io.Source
import java.util.Random

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
    
    val movies = sc.textFile(movieLensHomeDir + "/movies.dat").map { line =>
      val fields = line.split("::")
      // format: (movieId, movieName)
      (fields(0).toInt, fields(1))
    }.collect.toMap
      
    //topX(ratings,movies,1000,10)
    //select 10 movies from top numMovies*10
    val selectedMovies = topX(ratings,movies,1000,20,0.5).map(x => (x._2._1, x._2._4)).toSeq

    val myRatings = elicitateRatings(selectedMovies)
    val myRatingsRDD = sc.parallelize(myRatings)
    val numPartitions = 20
    val training = ratings.filter(x => x._1 < 6)
                          .values
                          .union(myRatingsRDD)
                          .repartition(numPartitions)
                          .persist
    val validation = ratings.filter(x => x._1 >= 6 && x._1 < 8)
                            .values
                            .repartition(numPartitions)
                            .persist
    val test = ratings.filter(x => x._1 >= 8).values.persist

    val numTraining = training.count
    val numValidation = validation.count
    val numTest = test.count

    println("Training: " + numTraining + ", validation: " + numValidation + ", test: " + numTest)
    
    val ranks = List(8, 12)
    val lambdas = List(0.1, 10.0)
    val numIters = List(10, 20)
    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1
    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
      val model = ALS.train(training, rank, numIter, lambda)
      val validationRmse = computeRmse(model, validation, numValidation)
      println("RMSE (validation) = " + validationRmse + " for the model trained with rank = "
        + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".")
      if (validationRmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIter
      }
    }

    val testRmse = computeRmse(bestModel.get, test, numTest)

    println("The best model was trained with rank = " + bestRank + " and lambda = " + bestLambda
      + ", and numIter = " + bestNumIter + ", and its RMSE on the test set is " + testRmse + ".")
      
      
      
    val myRatedMovieIds = myRatings.map(_.product).toSet
    val candidates = sc.parallelize(movies.keys.filter(!myRatedMovieIds.contains(_)).toSeq)
    val recommendations = bestModel.get
                                   .predict(candidates.map((0, _)))
                                   .collect
                                   .sortBy(-_.rating)
                                   .take(50)

    var i = 1
    println("Movies recommended for you:")
    recommendations.foreach { r =>
      println("%2d".format(i) + ": " + movies(r.product))
      i += 1
    }
  }
  /* 
   * topX(ratings,movies,1000,10).foreach(println)
   * topX(ratings,movies,1000,10,0.5).foreach(println)
   */
  def topX(
      dataRatings: RDD[(Long, Rating)], 
      movies: Map[Int,String],
      minViews: Int,
      numMovies: Int,
      getProportion: Double = 1
    ) : Array[(Double, (Int, Double, Int, String))] 
    = {
    
    val reducedRatings = dataRatings.
      map(x => ((x._2.product), (x._2.rating, 1) )    ).
      reduceByKey((x,y) => (x._1+y._1, x._2 + y._2)).
      map(x => (x._2._1/x._2._2, (x._1, x._2._1/x._2._2, x._2._2, movies(x._1) )  )).
      sortByKey(false,8)     
    val random = new Random()
    val filtered/*: RDD[(Double, (Int, Double, Int, String))]*/ = 
      if (0 < getProportion && getProportion < 1) 
          reducedRatings.take(numMovies*10). /* take 10 times the size than requested */
          filter(x => x._2._3 >= 500 && random.nextDouble() <= getProportion ). /* get random movies, biased towards better rated ones due to take(x) */
          take(numMovies)
      else 
        reducedRatings.filter(x => x._2._3 >= minViews).take(numMovies) 

    filtered.foreach( x => println(s"movie: ${x._2._4} \t\t rating: ${x._2._2} \t votes: ${x._2._3}" ))
    
    return filtered
  }
     
  /** Compute RMSE (Root Mean Squared Error). */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long) = {
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
                                           .join(data.map(x => ((x.user, x.product), x.rating)))
                                           .values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
  }
  
  /** Elicitate ratings from command-line. */
  def elicitateRatings(movies: Seq[(Int, String)]) = {
    val prompt = "Please rate the following movie (1-5 (best), or 0 if not seen):"
    println(prompt)
    val ratings = movies.flatMap { x =>
      var rating: Option[Rating] = None
      var valid = false
      while (!valid) {
        print(x._2 + ": ")
        try {
          val r = Console.readInt
          if (r < 0 || r > 5) {
            println(prompt)
          } else {
            valid = true
            if (r > 0) {
              rating = Some(Rating(0, x._1, r))
            }
          }
        } catch {
          case e: Exception => println(prompt)
        }
      }
      rating match {
        case Some(r) => Iterator(r)
        case None => Iterator.empty
      }
    }
    if(ratings.isEmpty) {
      error("No rating provided!")
    } else {
      ratings
    }
  }
}


