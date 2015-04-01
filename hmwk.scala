
import scala.util.control.Exception._
import scala.language.implicitConversions
import scala.language.higherKinds
import scala.math.Ordering

import java.io.{ PrintWriter, Serializable }

trait Ops[A] {
  def self: A
  def |>[B](f: (A) => B): B = f(self)
}

object ToOps {
  implicit def anyToOps[A](a: A) = new Ops[A] {
    def self: A = a
  }
}

import ToOps._

object InputHelpers {
  def readLine(reader: java.io.BufferedReader)() = {
    val line = reader.readLine
    if (line == null) None else Some(line)
  }
  def lineIter(reader: java.io.BufferedReader): Iterator[Option[String]] = 
    Iterator.continually(readLine(reader))
  def lineIter(file: String): Option[Iterator[Option[String]]] = allCatch.opt({
    val freader = new java.io.FileReader(file)
    val breader = new java.io.BufferedReader(freader)
    lineIter(breader)
  })
}

class ColumnedMap[A, B](private val amap: Map[A, B]) {
  def collapseColumns(columns: List[A])
                     (keyCollapser: (A, A) => A)
                     (valueCollapser: (B, B) => B): (A, B) =
                       columns.map({ (x) =>
                         (x, amap(x)) 
                       }).reduceLeft({ (a: Tuple2[A, B], b: Tuple2[A, B]) =>
                         (keyCollapser(a._1, b._1), valueCollapser(a._2, b._2))
                       })

  def foldColumns(columns: List[A])
                 (keyCollapser: (A, A) => A)
                 (valueCollapser: (B, B) => B): Map[A, B] =
                   Map[A, B](
                     collapseColumns(columns)
                                    (keyCollapser)
                                    (valueCollapser) 
                     :: amap.keys.toList.filter({ (x) =>
                       !(columns contains x)
                     }).map({ (x) => 
                       (x, amap(x))
                     }) :_*
                   )
}

object ColumnedMap {
  implicit def mapToColumnedMap[A, B](m: Map[A, B]) = new ColumnedMap(m)
}

import ColumnedMap._

object CSVParser {
  def apply[A](file: String, hdr: Boolean = true)(t: (Option[String], Int) => A) =
    CSVParser.parse[A](file, hdr)(t)
  def intP(file: String, hdr: Boolean = true) = 
    CSVParser[Int](file, hdr)(
      (x, i) => x.flatMap(
        y => allCatch.opt({
          y.toInt
        })
      ).getOrElse(Int.MaxValue)
    )
  def strP(file: String, hdr: Boolean = true) = CSVParser[String](file, hdr)(
    (x, i) => x.getOrElse("")
  )
  def parse[A](file: String, hasHeader: Boolean = true)(t: (Option[String], Int) => A) = {
    InputHelpers.lineIter(file).map({ (lineI: Iterator[Option[String]]) =>
      var data = scala.collection.mutable.Map[String, List[A]]()
      if (hasHeader) {
        val keys: List[String] = (lineI take 1).next().map(_.split(',').toList).
          map(_ map (_.trim) ).getOrElse(Nil)
        val vals = lineI.map(
          _.map(
            _.split(',').toList.map(
              (x) => Some(x.trim)
            ).padTo(keys.length, None)
          )
        ).takeWhile(
          (x: Option[List[Option[String]]]) => x.isDefined
        ).filter(
          (x: Option[List[Option[String]]]) => x.map(
            y => y.map(
              z => z.map(
                w => w.length
              ).getOrElse(0)
            ).exists(_ != 0)
          ).getOrElse(false)
        ).toList.map(_.getOrElse(List[Option[String]]()).zipWithIndex.map({
            (x: Option[String], i: Int) => 
              t(x, i)
          }.tupled)
        )
        keys.zipWithIndex foreach { (key: String, i: Int) =>
          data(key) = vals.foldRight(List[A]())(
            (line: List[A], l: List[A]) => line(i) :: l
          )
        }.tupled
      } else {
        val avals = lineI.map(
          _.map(
            _.split(',').toList.map(
              (x) => if (true) Some(x.trim) else None
            )
          )
        ).takeWhile(
          (x: Option[List[Option[String]]]) => x.isDefined
        ).filter(
          (x: Option[List[Option[String]]]) => x.map(
            y => y.map(
              z => z.map(
                w => w.length
              ).getOrElse(0)
            ).exists(_ != 0)
          ).getOrElse(false)
        ).toList.map(_.getOrElse(List[Option[String]]()).zipWithIndex.map({
            (x: Option[String], i: Int) =>
              t(x, i)
          }.tupled)
        )
        val maxlen = avals.map(_.length).max
        val tnone = t(None, _: Int)
        val vals = avals.zipWithIndex map { (x: List[A], i: Int) =>
          x.padTo(maxlen, tnone(i))
        }.tupled
        val keys = (0 until maxlen).map(_.toString)
        keys.zipWithIndex foreach { (key: String, i: Int) =>
          data(key) = vals.foldRight(List[A]())(
            (line: List[A], l: List[A]) => line(i) :: l
          )
        }.tupled
      }
      data
    })
  }
}

class CSVWriter[A, B](private val amap: Map[A, List[B]]) {
  private var file: Option[String] = None
  private var owriter: Option[PrintWriter] = None
  private var afterNewLine: Boolean = true
  private var keyOrdering: Option[Ordering[A]] = None
  def writeToCSV(f: String) = {
    file = Some(f)
    this
  }
  def withWriters(ohwriter: Option[(A) => String])(writer: (Option[B] => String)) = {
    val k = if (keyOrdering.isDefined) {
      keyOrdering.map({ (ord) =>
        amap.keys.toList.sorted(ord)
      }).getOrElse(amap.keys.toList)
    } else amap.keys.toList
    val maxlen = k.map((kk) => amap(kk).length).max
    allCatch.opt {
      ohwriter.map({ (hwriter: (A) => String) =>
        write(List(k.map((x) => hwriter(x))))
      })
      write((0 until maxlen).toList.map({ (i) =>
        (0 until k.length).toList.map({ (j) =>
          val lval = amap(k(j))
          if (i >= lval.length)
            writer(None)
          else
            writer(Some(amap(k(j))(i)))
        })
      })).finish
      true
    } getOrElse(false)
  }
  def withKeyOrdering(ord: Ordering[A]) = {
    keyOrdering = Some(ord)
    this
  }
  def write(l: List[List[String]]): CSVWriter[A, B] = {
    l foreach { (ll: List[String]) =>
      ll foreach { (s: String) =>
        writeCell(s)
      }
      writeNewLine
    }
    this
  }
  def stream: Option[PrintWriter] = {
    if (!file.isDefined)
      return None
    if (!owriter.isDefined)
      owriter = Some(new PrintWriter(file.getOrElse("banana.csv")))
    owriter
  }
  def writeCell(s: String) = {
    if (!afterNewLine) {
      stream.map(_.print(", "))
    }
    afterNewLine = false
    stream.map(_.print(s))
    this
  }
  def writeNewLine = {
    afterNewLine = true;
    stream.map(_.print("\n"))
    this
  }
  def finish(): CSVWriter[A, B] = {
    stream.map(_.close())
    this
  }
}

object CSVWriter {
  implicit def mapToCSVWriter[A <% Serializable, B <% Serializable](m: Map[A, List[B]]): CSVWriter[A, B] = new CSVWriter(m)
}

import CSVWriter._

class Categories(private val jar: scala.collection.mutable.Map[String, List[Int]], val cls: String) {
  def apply(cat: String) = 
    if ((jar contains cat) && jar(cat).length >= 2) Some((jar(cat)(0), jar(cat)(1)))
    else None // yes, isDefinedAt exists but jar isDefinedAt cat isnt as cool as jar contains cat
  def names = jar.keys.toList
  def weightOf(cat: String) = this(cat).map(_._1)
  def timesWePet(cat: String) = this(cat).map(_._2)
  def jarOfCats = 
    jar.map({(key: String, value: List[Int]) => 
      (key, this(key))
    }.tupled)
}

object Categories {
  def apply(cls: String = "comp170") =
    CSVParser.intP("categories_" + cls + ".txt").map(j => new Categories(j, cls))
}

class Students(private val pot: scala.collection.mutable.Map[String, List[String]], private val ingredients: Categories) {
  private val ratings = pot("0").map(
    (key: String) => (key, GradeBook(key, ingredients))
  ).toMap
  def madeBy(id: String) = allCatch.opt { pot("1")(pot("0").indexOf(id)) } getOrElse ""
  def madeFor(id: String) = allCatch.opt { pot("2")(pot("0").indexOf(id)) } getOrElse ""
  def cooks = pot("0")
  def ratingsFrom(id: String) = if (ratings contains id) ratings(id) else None
}

object Students {
  def apply(cats: Categories) = CSVParser.strP("students_" + cats.cls + ".txt", false).flatMap(
    p => if (p.size >= 3) Some(new Students(p, cats)) else None
  )
}

sealed abstract class GradeBookData
case class GradeCategory(category: String) extends GradeBookData
case class GradeIndex(index: Int) extends GradeBookData
case class Grade(grade: Double) extends GradeBookData {
  def toGradeString =
    (grade match {
      case x if (x > 100) => "A+++"
      case x if (x >= 90) => "A"
      case x if (x >= 80) => "B"
      case x if (x >= 70) => "C"
      case x if (x >= 60) => "D"
      case x if (x >= 0) => "F"
      case _ => "F---"
    }) + {
      if (grade < 100 && grade >= 60) {
        grade.toInt.abs % 10 match {
          case x if (x <= 2) => "-"
          case x if (x >= 8) => "+"
          case _ => ""
        }
      } else ""
    }
}
case class GradeBookBadData(data: String) extends GradeBookData

object DoubleStuffed {
  implicit def doubleToGrade(d: Double) = Grade(d)
}

import DoubleStuffed._

class GradeBook(private val unSortedBook: scala.collection.mutable.Map[String, List[GradeBookData]], private val cats: Categories) {
  private var book = scala.collection.mutable.Map[String, List[Double]]()
  var subjects: List[String] = unSortedBook("0").map({ 
    case GradeCategory(c) => c
    case _ => "Banana"
  }).distinct
  def gradesFor(cat: String) = {
    if (!book.contains(cat))
      book(cat) = (unSortedBook("0").zipWithIndex filter (_._1 == GradeCategory(cat)) map PartialFunction[(GradeBookData, Int), (GradeBookData, GradeBookData)] {
        case (GradeCategory(key), i: Int) =>
          (unSortedBook("1")(i), unSortedBook("2")(i))
        case (_, i: Int) =>
          (GradeIndex(Int.MaxValue), Grade(0))
      } map PartialFunction[(GradeBookData, GradeBookData), (Int, Double)] {
        case (GradeIndex(i), Grade(g)) => (i, g)
        case (_, _) => (Int.MaxValue, 0.0)
      }).sorted.map({
        case (i: Int, j: Double) => j
      })
    book(cat)
  }
  def averageFor(cat: String) =
    gradesFor(cat).reduceLeft(_ + _) / cats.timesWePet(cat).
                                            getOrElse(gradesFor(cat).length).
                                            toDouble
  def average = subjects.map({ (subject: String) =>
      averageFor(subject) * cats.weightOf(subject).getOrElse(0) / 100
    }).reduceLeft(_ + _) / cats.jarOfCats.foldLeft(0: Double)({ (a: Double, d: Tuple2[String, Option[Tuple2[Int, Int]]]) =>
      a + ( d._2.getOrElse((0, 0))._1.toDouble / 100.0 )
    })
}

object GradeBook {
  def apply(id: String, cats: Categories) = CSVParser(id + cats.cls + ".data", false)(Function.untupled(PartialFunction[(Option[String], Int), GradeBookData] {
        case (Some(value), 0) => GradeCategory(value)
        case (None, 0) => GradeCategory("Banana")
        case (Some(value), 1) => GradeIndex(allCatch.opt { value.toInt } getOrElse Int.MaxValue)
        case (None, 1) => GradeIndex(Int.MaxValue)
        case (Some(value), 2) => Grade(allCatch.opt { value.toDouble } getOrElse 0)
        case (None, 2) => Grade(0)
        case (Some(value), _) => GradeBookBadData(value)
        case (None, _) => GradeBookBadData("~~None")
      })) map { g => new GradeBook(g, cats) }
}

object Main extends App {
  val whoacats: Option[Categories] = if (args.length > 0) Categories(args(0))
                                     else Categories({
                                       print("Course Abbreviation: ")
                                     } |> { (a: Unit) =>
                                       InputHelpers.readLine(new java.io.BufferedReader(new java.io.InputStreamReader(java.lang.System.in))).getOrElse("comp170")
                                     })
  whoacats.map({(cats: Categories) =>
    cats.names foreach { (cat: String) =>
      println(s"${cat}\n\tWeight: ${cats weightOf cat getOrElse Int.MaxValue}\n\t# Grades: ${cats timesWePet cat getOrElse Int.MaxValue}")
    }
    val yumstus: Option[Students] = Students(cats)
    yumstus.map({(stus: Students) =>
      stus.cooks foreach { (cook: String) =>
        println(s"${cook}\n\tLast: ${stus madeBy cook}\n\tFirst: ${stus madeFor cook}")
        println(stus ratingsFrom cook map { (ratings: GradeBook) =>
          "Grades:\n" + ratings.subjects.foldLeft("")({ (str: String, subject) =>
            val average = ratings.averageFor(subject)
            f"$str\t$subject: ${ratings.averageFor(subject)}%.1f" +
            ratings.gradesFor(subject).foldLeft("")({ (a: String, grade: Double) =>
              a + f"\n\t\t$grade%.1f"
            }) + "\n"
          }) + f"\tOverall: ${ratings.average}%.1f ${ratings.average.toGradeString}"
        } getOrElse s"No grades for $cook")
      }

      Map("last" -> stus.cooks.map({ (cook: String) => stus madeBy cook }),
        "first" -> stus.cooks.map({ (cook: String) => stus madeFor cook }),
        "avg" -> stus.cooks.map({ (cook: String) => stus ratingsFrom cook map { (ratings: GradeBook) =>
          f"${ratings.average}%.1f"
        } getOrElse ""}),
        "letter" -> stus.cooks.map({ (cook: String) => stus ratingsFrom cook map { (ratings: GradeBook) =>
          ratings.average.toGradeString
        } getOrElse ""})
      ).foldColumns(List("first", "avg", "letter"))(_ + " " + _)({(a: List[String], b: List[String]) =>
        val maxlen = math.max(a.length, b.length)
        (0 until maxlen).toList.map({ (x) =>
          val aa = if (x < a.length) a(x) else ""
          val bb = if (x < b.length) b(x) else ""
          aa + " " + bb
        })
      }).writeToCSV(cats.cls + "_summary.txt").withKeyOrdering(new Ordering[String] {
        def compare(x: String, y: String): Int = (x, y) match {
          case ("last", "last") => 0
          case ("last", _) => -1
          case (_, "last") => 1
          case (_, _) => 0
        }
      }).withWriters(None)((x) => x.getOrElse(""))

    })
  })
}

object Tests {
  def testCSVWriter = {
    Map("hello" -> List("world", "girl", "meow"), "and" -> List("some", "more!")).writeToCSV("cat.csv").
      withWriters(Some((x) => x))((x) => x.getOrElse(""))
  }
}

