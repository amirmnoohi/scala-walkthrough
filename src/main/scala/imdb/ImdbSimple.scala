package imdb

import scala.io.Source

object ImdbSimple {

  val titleBasicsList: List[TitleBasics] = {
    Source.fromFile(ImdbData.titleBasicsPath)
      .getLines()
      .map(ImdbData.parseTitleBasics)
      .toList
  }

  val titleRatingsList: List[TitleRatings] = {
    Source.fromFile(ImdbData.titleRatingsPath)
      .getLines()
      .map(ImdbData.parseTitleRatings)
      .toList
  }

  // Using `ImdbData.titleCrewPath` and `ImdbData.parseTitleCrew` to populate the list
  val titleCrewList: List[TitleCrew] = {
    Source.fromFile(ImdbData.titleCrewPath)
      .getLines()
      .map(ImdbData.parseTitleCrew)
      .toList
  }

  val nameBasicsList: List[NameBasics] = {
    Source.fromFile(ImdbData.nameBasicsPath)
      .getLines()
      .map(ImdbData.parseNameBasics)
      .toList
  }
  val timing = new StringBuffer

  def main(args: Array[String]) {
    printClassInfo()
    val durations = timed("Task 1", task1(titleBasicsList))
    println(durations)

    val titles = timed("Task 2", task2(titleBasicsList, titleRatingsList))
    println(titles)

    val topRated = timed("Task 3", task3(titleBasicsList, titleRatingsList))
    println(topRated)

    val crews = timed("Task 4", task4(titleBasicsList, nameBasicsList, titleCrewList))
    println(crews)
  }

  def printClassInfo(): Unit = {
    // List of all case classes
    val allLists = List(
      ("TitleBasics", titleBasicsList),
      ("TitleRatings", titleRatingsList),
      ("TitleCrew", titleCrewList),
      ("NameBasics", nameBasicsList)
    )

    allLists.foreach {
      case (name, list) =>
        println(s"Case class: $name")
        println(s"Length: ${list.length}")
        println(s"Head: ${list.headOption.getOrElse("Empty List")}")
        println("-------------------------------")
    }
  }

  def task1(list: List[TitleBasics]): List[(String, Float, Int, Int)] = {
    // Filter out titles with no runtime or no genres
    val validTitles = list.filter(tb => tb.runtimeMinutes.isDefined && tb.genres.isDefined)

    // Create a flat list where each title with multiple genres is duplicated for each genre
    val expandedByGenre = validTitles.flatMap(tb => tb.genres.get.map(g => (g, tb.runtimeMinutes.get)))

    // Group by genre and compute statistics
    val statsByGenre = expandedByGenre
      .groupBy(_._1) // Group by genre
      .mapValues(titles => {
        val runtimes = titles.map(_._2)
        val avg = runtimes.sum.toFloat / runtimes.size
        val min = runtimes.min
        val max = runtimes.max
        (avg, min, max)
      })
      .toList
      .map { case (genre, (avg, min, max)) => (genre, avg, min, max) }
      .sortBy(_._1) // Sort by genre for consistency

    statsByGenre
  }

  def task2(l1: List[TitleBasics], l2: List[TitleRatings]): List[String] = {
    // Create a map of tconst to TitleRatings for efficient look-up
    val ratingsMap = l2
      .filter(tr => tr.averageRating >= 7.5 && tr.numVotes >= 500000)
      .map(tr => tr.tconst -> tr)
      .toMap

    // Filter movies based on criteria and lookup ratings efficiently using the map
    l1.filter(tb =>
      tb.titleType.contains("movie") &&
        tb.startYear.isDefined &&
        tb.startYear.get >= 1990 &&
        tb.startYear.get <= 2018 &&
        ratingsMap.contains(tb.tconst)
    ).map(_.primaryTitle.getOrElse(""))
  }

  def task3(l1: List[TitleBasics], l2: List[TitleRatings]): List[((Int, String), String)] = {

    //    // Step 1: Join TitleBasics and TitleRatings RDDs on tconst
    //    val joinedData = l1.flatMap(tb => l2.find(tr => tr.tconst == tb.tconst).map(tr => (tb, tr)))

    // Create a map for faster lookups
    val ratingsMap = l2.map(tr => tr.tconst -> tr).toMap

    // Step 1: Join TitleBasics and TitleRatings using the map
    val joinedData = l1.flatMap(tb => ratingsMap.get(tb.tconst).map(tr => (tb, tr)))

    // Step 2, 3: Filter and Extract Decade
    val moviesWithDecadeAndGenre = joinedData
      .filter { case (tb, _) =>
        tb.titleType.contains("movie") &&
          tb.startYear.isDefined &&
          tb.startYear.get >= 1900 &&
          tb.startYear.get <= 1999
      }
      .flatMap { case (tb, tr) =>
        val decade = (tb.startYear.get - 1900) / 10
        tb.genres.getOrElse(List()).map(genre => ((decade, genre), (tb.primaryTitle.getOrElse(""), tr.averageRating)))
      }

    // Step 4, 5, 6: Group, Find Top-Rated, and Sort
    val topRatedMovies = moviesWithDecadeAndGenre
      .groupBy(_._1)
      .mapValues(movies =>
        movies.minBy(movie => (-movie._2._2, movie._2._1))._2._1
      )
      .toList
      .sortBy(_._1)

    topRatedMovies
  }

  def task4(l1: List[TitleBasics], l3: List[NameBasics], l4: List[TitleCrew]): List[(String, Int)] = {

    // Step 1: Filter movies released between 2010 and 2021
    val movies2010_2021 = l1
      .filter(tb =>
        tb.titleType.contains("movie") &&
          tb.startYear.isDefined &&
          tb.startYear.get >= 2010 &&
          tb.startYear.get <= 2021
      )
      .map(_.tconst)
      .toSet

    // Step 2: Filter TitleCrew records based on movies2010_2021 and extract crew members
    val relevantCrew = l4
      .filter(tc => movies2010_2021.contains(tc.tconst))
      .flatMap(tc =>
        (tc.directors.getOrElse(List()) ++ tc.writers.getOrElse(List())).distinct.map(crewId => (crewId, tc.tconst))
      )

    // Step 3: Join NameBasics with relevantCrew based on nconst (crew ID)
    val crewWithMovies = l3
      .map(nb => (nb.nconst, nb.primaryName.getOrElse("")))
      .flatMap { case (nconst, name) =>
        relevantCrew.filter(_._1 == nconst).map { case (_, movie) => (name, movie) }
      }

    // Step 4: Group by crew name and count movies
    val crewMovieCounts = crewWithMovies
      .groupBy(_._1)
      .mapValues(movies => movies.map(_._2).toSet.size)
      .toList

    // Step 5: Filter crew with at least 2 movies
    val result = crewMovieCounts.filter(_._2 >= 2).sortBy(_._1)

    result
  }

  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    println(s"Processing $label took ${stop - start} ms.")
    result
  }
}
