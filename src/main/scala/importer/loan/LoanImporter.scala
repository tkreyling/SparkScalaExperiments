package importer.loan

import java.io.StringWriter
import java.time.LocalDate
import java.util
import javax.xml.bind.{JAXB, JAXBContext, Marshaller}
import javax.xml.bind.annotation.adapters.{XmlAdapter, XmlJavaTypeAdapter}
import javax.xml.bind.annotation._

import org.apache.spark.{SparkConf, SparkContext}

import scala.annotation.meta.field
import scala.collection.JavaConverters._
import scala.tools.nsc.interpreter.JList

object LoanImporter {

  type xmlElement     = XmlElement @field
  type xmlElementWrapper     = XmlElementWrapper @field
  type xmlTypeAdapter = XmlJavaTypeAdapter @field

  class StringOptionAdapter extends OptionAdapter[String](null, "")
  class OptionAdapter[A](nones: A*) extends XmlAdapter[A, Option[A]] {
    def marshal(v: Option[A]): A = v.getOrElse(nones(0))
    def unmarshal(v: A) = if (nones contains v) None else Some(v)
  }

  class LocalDateAdapter extends XmlAdapter[String, LocalDate] {
    override def marshal(v: LocalDate): String = v.toString
    override def unmarshal(v: String): LocalDate = LocalDate.parse(v)
  }

  class BigDecimalAdapter extends XmlAdapter[String, BigDecimal] {
    override def marshal(v: BigDecimal): String = v.toString()
    override def unmarshal(v: String): BigDecimal = BigDecimal(v)
  }


  @XmlRootElement(name = "loan")
  @XmlAccessorType(XmlAccessType.FIELD)
  case class Loan(@xmlElement(required=true) id: String,
                  @xmlTypeAdapter(classOf[LocalDateAdapter])startDate: LocalDate,
                  @xmlTypeAdapter(classOf[LocalDateAdapter])endDate: LocalDate,
                  @xmlElementWrapper(name = "drawings") @xmlElement(name = "drawing") drawings: JList[Drawing] = new util.ArrayList(),
                  @xmlTypeAdapter(classOf[StringOptionAdapter]) counterparty: Option[String] = None,
                  @xmlElement(required=true)commitment: Money = ZeroEuro,
                  @xmlElement(required=true)undrawn: Money = ZeroEuro){
    private def this() = this("", null, null)
  }

  case class Drawing(@xmlElement(required=true)id: String,
                     @xmlTypeAdapter(classOf[LocalDateAdapter])startDate: LocalDate,
                     @xmlTypeAdapter(classOf[LocalDateAdapter])endDate: LocalDate,
                     @xmlElement(required=true)outstanding: Money = ZeroEuro){
    private def this() = this("", null, null)
  }

  case class Money(@xmlTypeAdapter(classOf[BigDecimalAdapter])amount: BigDecimal,
                   @xmlElement(required=true)currency: String){
    private def this() = this(0, "")
  }

  val ZeroEuro = Money(0, "EUR")

  def main(args: Array[String]): Unit ={
    if(System.getProperty("os.name").contains("Windows"))
      System.setProperty("hadoop.home.dir", "C:\\winutil\\")

    val conf = new SparkConf().setAppName("Loan Importer").setMaster("local")
    val sc = new SparkContext(conf)

    val loansAndDrawings = sc.textFile("src/main/resources/importer/loan/BasicData.csv").map(
      line => line.split(",")(1) match {
        case "loan" => parseLoan(line)
        case "drawing" => parseDrawing(line)
        case _ =>
      }
    ).keyBy {
      case l: Loan => l.id
      case d: Drawing => d.id
      case _ => "None"
    }

    val loansAndDrawingsWithCapitals = loansAndDrawings.join(
      sc.textFile("src/main/resources/importer/loan/Capitals.csv")
        .keyBy(line => line.split(",")(0)))
      .mapValues {
        case (l: Loan, s: String) => parseCapitals(l, s)
        case (d: Drawing, s: String) => parseCapitals(d, s)
        case _ =>
      }

    val loans = loansAndDrawingsWithCapitals.filter( pair => pair._2 match {
      case l : Loan => true
      case _ => false
    })

    val drawings = loansAndDrawingsWithCapitals.filter( pair => pair._2 match {
      case d : Drawing => true
      case _ => false
    })

    val counterpartyReferences = sc.textFile("src/main/resources/importer/loan/References.csv")
      .filter(line => line.split(",")(2).equalsIgnoreCase("CP"))
      .keyBy( line => line.split(",")(0))

    val loansWithCounterparties = loans.join(counterpartyReferences).mapValues {
      case (a: Loan, b: String) => a.copy(counterparty = Some(b.split(",")(1)))
    }

    val drawingsWithLoanKeys = sc.textFile("src/main/resources/importer/loan/References.csv")
          .filter(line => line.split(",")(2).equalsIgnoreCase("D"))
          .keyBy( line => line.split(",")(1))
      .join(drawings)
          .values.map(pair => (pair._1.split(",")(0), pair._2))
          .groupByKey()

    val loansWithDrawings = loansWithCounterparties.leftOuterJoin(drawingsWithLoanKeys)
      .mapValues {
        case (a: Loan, b: Option[Iterable[Drawing]]) => a.copy(drawings = b.getOrElse(List()).toList.asJava)
        case _ => throw new RuntimeException
      }

    loansWithDrawings
      .map( v => {
        val out: StringWriter = new StringWriter()
        JAXB.marshal(v._2, out)
        out.toString
      }
    )
      .saveAsTextFile("src/main/resources/importer/loan/out.xml")
  }

  def parseLoan(line: String) : Loan = {
    val parts = line.split(",")
    Loan(parts(0), LocalDate.parse(parts(2)), LocalDate.parse(parts(3)))
  }

  def parseCapitals(l: Loan, s: String) : Loan = {
    val parts = s.split(",")
    val currency = parts(4)

    var result = l

    if(!currency.isEmpty){
      val commitment = parts(1)
      val undrawn = parts(2)

      if(!commitment.isEmpty)
        result = result.copy(commitment = Money(BigDecimal(commitment), currency))
      if(!undrawn.isEmpty)
        result = result.copy(undrawn = Money(BigDecimal(undrawn), currency))

    }
    result
  }

  def parseDrawing(line: String) : Drawing = {
    val parts = line.split(",")
    Drawing(parts(0), LocalDate.parse(parts(2)), LocalDate.parse(parts(3)))
  }

  def parseCapitals(d: Drawing, s: String) : Drawing = {
    val parts = s.split(",")
    val currency = parts(4)

    var result = d

    if(!currency.isEmpty){
      val outstanding = parts(3)

      if(!outstanding.isEmpty)
        result = d.copy(outstanding = Money(BigDecimal(outstanding), currency))
    }
    result
  }

}
