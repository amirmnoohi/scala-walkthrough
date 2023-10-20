package imdb

import java.io.File

case class TitleBasics(tconst: String, titleType: Option[String], primaryTitle: Option[String],
                       originalTitle: Option[String], isAdult: Int, startYear: Option[Int], endYear: Option[Int],
                       runtimeMinutes: Option[Int], genres: Option[List[String]])

case class TitleRatings(tconst: String, averageRating: Float, numVotes: Int)

case class TitleCrew(tconst: String, directors: Option[List[String]], writers: Option[List[String]])

case class NameBasics(nconst: String, primaryName: Option[String], birthYear: Option[Int], deathYear: Option[Int],
                      primaryProfession: Option[List[String]], knownForTitles: Option[List[String]])

object ImdbData {
  private val SKIP_VAL = "\\N"

  private[imdb] def filePath(name: String) = {
    val resource = this.getClass.getClassLoader.getResource(s"$name.tsv")
    if (resource == null) sys.error("Please download the dataset as explained in the assignment instructions.")
    new File(resource.toURI).getPath
  }

  private[imdb] def titleBasicsPath = filePath("title.basics")
  private[imdb] def titleRatingsPath = filePath("title.ratings")
  private[imdb] def titleCrewPath = filePath("title.crew")
  private[imdb] def nameBasicsPath = filePath("name.basics")

  private[imdb] def parseAttribute(word: String): Option[String] = 
    if(word == SKIP_VAL) None else Some(word)

  private[imdb] def parseTitleBasics(line: String): TitleBasics = {
    val attrs = line.split("\t").map(parseAttribute)
    if(attrs.length != 9)
      sys.error("Error in the format of `title.basics.tsv`.")
    imdb.TitleBasics(attrs(0).get, attrs(1), attrs(2),
               attrs(3), attrs(4).get.toInt, attrs(5).map(_.toInt), attrs(6).map(_.toInt),
               attrs(7).map(_.toInt), attrs(8).map(_.split(",").toList))
  }

  private[imdb] def parseTitleRatings(line: String): TitleRatings = {
    val attrs = line.split("\t").map(parseAttribute)
    if(attrs.length != 3)
      sys.error("Error in the format of `title.ratings.tsv`.")
    imdb.TitleRatings(attrs(0).get, attrs(1).get.toFloat, attrs(2).get.toInt)
  }

  private[imdb] def parseTitleCrew(line: String): TitleCrew = {
    val attrs = line.split("\t").map(parseAttribute)
    if(attrs.length != 3)
      sys.error("Error in the format of `title.crew.tsv`.")
    imdb.TitleCrew(attrs(0).get, attrs(1).map(_.split(",").toList), attrs(2).map(_.split(",").toList))
  }

  private[imdb] def parseNameBasics(line: String): NameBasics = {
    val attrs = line.split("\t").map(parseAttribute)
    if(attrs.length != 6)
      sys.error("Error in the format of `name.basics.tsv`.")
    imdb.NameBasics(attrs(0).get, attrs(1), attrs(2).map(_.toInt), attrs(3).map(_.toInt),
               attrs(4).map(_.split(",").toList), attrs(5).map(_.split(",").toList))
  }
}
