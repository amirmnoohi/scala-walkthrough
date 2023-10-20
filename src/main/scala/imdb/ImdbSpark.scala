package imdb

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ImdbSpark {
  private val conf: SparkConf = new SparkConf()
    .setAppName("ImdbAnalysis") // Set your application's name
    .setMaster("local[*]") // Use all cores of the local machine

  private val sc: SparkContext = new SparkContext(conf)

  private val titleBasicsRDD: RDD[TitleBasics] = sc.textFile(ImdbData.titleBasicsPath)
    .map(line => ImdbData.parseTitleBasics(line))

  private val titleRatingsRDD: RDD[TitleRatings] = sc.textFile(ImdbData.titleRatingsPath)
    .map(line => ImdbData.parseTitleRatings(line))

  private val titleCrewRDD: RDD[TitleCrew] = sc.textFile(ImdbData.titleCrewPath)
    .map(line => ImdbData.parseTitleCrew(line))

  private val nameBasicsRDD: RDD[NameBasics] = sc.textFile(ImdbData.nameBasicsPath)
    .map(line => ImdbData.parseNameBasics(line))

  def main(args: Array[String]) {
    printClassInfo()

    val durations = timed("Task 1", task1(titleBasicsRDD).collect().toList)
    println(durations)

    val titles = timed("Task 2", task2(titleBasicsRDD, titleRatingsRDD).collect().toList)
    println(titles)

    val topRated = timed("Task 3", task3(titleBasicsRDD, titleRatingsRDD).collect().toList)
    println(topRated)

    val crews = timed("Task 4", task4(titleBasicsRDD, nameBasicsRDD, titleCrewRDD).collect().toList)
    println(crews)

    sc.stop()
  }

  def task1(rdd: RDD[TitleBasics]): RDD[(String, Float, Int, Int)] = {
    // Filter out titles with no runtime or no genres
    val validTitles = rdd.filter(tb => tb.runtimeMinutes.isDefined && tb.genres.isDefined)

    // Create a flat RDD where each title with multiple genres is duplicated for each genre
    val expandedByGenre = validTitles.flatMap(tb => tb.genres.get.map(g => (g, tb.runtimeMinutes.get)))

    // Group by genre and compute statistics
    val statsByGenre = expandedByGenre
      .groupByKey() // Group by genre
      .mapValues(titles => {
        val runtimes = titles.toList
        val avg = runtimes.sum.toFloat / runtimes.size
        val min = runtimes.min
        val max = runtimes.max
        (avg, min, max)
      })
      .map { case (genre, (avg, min, max)) => (genre, avg, min, max) }
      .sortBy(_._1) // Sort by genre for consistency

    statsByGenre
  }

  def task2(l1: RDD[TitleBasics], l2: RDD[TitleRatings]): RDD[String] = {
    // Create an RDD of tconst to TitleRatings after filtering
    val ratingsRDD = l2
      .filter(tr => tr.averageRating >= 7.5 && tr.numVotes >= 500000)
      .map(tr => (tr.tconst, tr))


    // Convert the l1 RDD to a pair RDD with tconst as the key and then join with ratingsRDD
    l1.filter(tb =>
        tb.titleType.contains("movie") && // Filter movies based on type
          tb.startYear.isDefined && // Filter movies based on startYear
          tb.startYear.get >= 1990 && // Filter movies based on startYear
          tb.startYear.get <= 2018) // Filter movies based on startYear
      .keyBy(_.tconst) // Convert to pair RDD with tconst as the key or use map(tb => (tb.tconst, tb))
      .join(ratingsRDD) // Join with ratingsRDD
      .map { case (_, (tb, _)) => tb.primaryTitle.getOrElse("") } // Extract primaryTitle
  }

  def task3(l1: RDD[TitleBasics], l2: RDD[TitleRatings]): RDD[((Int, String), String)] = {

    // Step 1: Join TitleBasics and TitleRatings RDDs on tconst
    val joinedData = l1.keyBy(_.tconst).join(l2.keyBy(_.tconst))

    // Step 2: Filter and Step 3: Extract Decade
    val moviesWithDecadeAndGenre = joinedData
      .filter { case (_, (tb, _)) => // why this structure?
        tb.titleType.contains("movie") && // Filter movies based on type
          tb.startYear.isDefined && // Filter movies based on startYear
          tb.startYear.get >= 1900 && // Filter movies based on startYear
          tb.startYear.get <= 1999 // Filter movies based on startYear
      }
      .flatMap { case (_, (tb, tr)) =>
        val decade = (tb.startYear.get - 1900) / 10
        tb.genres.getOrElse(List()).map(genre => ((decade, genre), (tb.primaryTitle.getOrElse(""), tr.averageRating)))
      }

    // Step 4, 5, 6: Group, Find Top-Rated and Sort
    val topRatedMovies = moviesWithDecadeAndGenre
      .groupByKey()
      .mapValues(movies =>
        movies.toList.minBy(movie => (-movie._2, movie._1))._1 // first sort by rating(descending), then by title
      )
      .sortByKey()

    topRatedMovies
  }

  def task4(l1: RDD[TitleBasics], l3: RDD[NameBasics], l4: RDD[TitleCrew]): RDD[(String, Int)] = {

    // Step 1: Filter movies released between 2010 and 2021
    val movies2010_2021 = l1
      .filter(tb =>
        tb.titleType.contains("movie") &&
          tb.startYear.isDefined &&
          tb.startYear.get >= 2010 &&
          tb.startYear.get <= 2021
      )
      .map(_.tconst)
      .collect()
      .toSet

    // Step 2: Filter TitleCrew records based on movies2010_2021 and extract crew members
    val relevantCrew = l4
      .filter(tc => movies2010_2021.contains(tc.tconst))
      .flatMap(tc =>
        (tc.directors.getOrElse(List()) ++ tc.writers.getOrElse(List())).distinct.map(crewId => (crewId, tc.tconst))
      )

    // Step 3: Join NameBasics with relevantCrew based on nconst (crew ID)
    val crewWithMovies = l3
      .keyBy(_.nconst)
      .join(relevantCrew)
      .map { case (_, (nb, movie)) =>
        (nb.primaryName.getOrElse(""), movie)
      }

    // Step 4: Group by crew name and count movies
    val crewMovieCounts = crewWithMovies
      .groupByKey()
      .mapValues(_.toSet.size)

    // Step 5: Filter crew with at least 2 movies
    val result = crewMovieCounts.filter(_._2 >= 2).sortByKey()

    result
  }


  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    println(s"Processing $label took ${stop - start} ms.")
    result
  }

  private def printClassInfo(): Unit = {
    val allRDDs = List(
      ("TitleBasics", titleBasicsRDD),
      ("TitleRatings", titleRatingsRDD),
      ("TitleCrew", titleCrewRDD),
      ("NameBasics", nameBasicsRDD)
    )

    allRDDs.foreach {
      case (name, rdd) =>
        println(s"Case class: $name")
        println(s"Length: ${rdd.count()}")
        println(s"Head: ${rdd.first()}")
        println("-------------------------------")
    }
  }
}
