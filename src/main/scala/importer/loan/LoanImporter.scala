package importer.loan

import java.io.StringWriter
import java.time.LocalDate

import javax.xml.bind.{JAXBContext, Marshaller}

import importer.loan.LoanModel.{Drawing, Loan, Money}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

object LoanImporter {

  def main(args: Array[String]): Unit ={
    if(System.getProperty("os.name").contains("Windows"))
      System.setProperty("hadoop.home.dir", "C:\\winutil\\")

    val conf = new SparkConf()
      .setAppName("Loan Importer")
      .set("spark.hadoop.validateOutputSpecs", "false")

    val sc = new SparkContext(conf)

    val RESOURCE_PATH: String = "/Applications/mesos-1.0.1/share/"
    /*
          read and group into (key, value) tuples based on loan or drawing id
         */
    val basicData = sc.textFile(RESOURCE_PATH +
      "BasicData.csv")
      .map(line => line.split(",")(1) match {
        case "loan" => parseLoan(line)
        case "drawing" => parseDrawing(line)
        case _ =>
      })
      .keyBy {
        case l: Loan => l.id
        case d: Drawing => d.id
        case _ => "None"
    }

    val capitals = sc.textFile(RESOURCE_PATH +
      "Capitals.csv")
      .keyBy(line => line.split(",")(0))

    val counterpartyReferences = sc.textFile(RESOURCE_PATH +
      "References.csv")
      .filter(line => line.split(",")(2).equalsIgnoreCase("CP"))
      .keyBy( line => line.split(",")(0))

    val drawingReferences = sc.textFile(RESOURCE_PATH +
      "References.csv")
      .filter(line => line.split(",")(2).equalsIgnoreCase("D"))
      .keyBy( line => line.split(",")(1))

    /*
      join basic data and capitals
     */
    val loansAndDrawings = basicData.join(capitals)
      .mapValues {
        case (l: Loan, s: String) => parseCapitals(l, s)
        case (d: Drawing, s: String) => parseCapitals(d, s)
        case _ =>
      }

    /*
      join loans and counterparties
     */
    var loans = loansAndDrawings.filter( pair => pair._2 match {
      case l : Loan => true
      case _ => false
    })

    loans = loans.leftOuterJoin(counterpartyReferences).mapValues {
      case (a: Loan, b: Some[String]) => a.copy(counterparty = Some(b.get.split(",")(1)))
      case (a: Loan, _) => a
    }

    /*
      join drawings and drawing references and group by loan id to get a list of drawings per loan
     */
    val drawings = loansAndDrawings.filter( pair => pair._2 match {
      case d : Drawing => true
      case _ => false
    })

    val drawingsWithLoanKeys = drawingReferences.join(drawings)
          .values.map(pair => (pair._1.split(",")(0), pair._2))
          .groupByKey()

    /*
      add the drawings to the loans
     */
    loans = loans.leftOuterJoin(drawingsWithLoanKeys)
      .mapValues {
        case (a: Loan, b: Option[Iterable[Drawing]]) => a.copy(drawings = b.getOrElse(List()).toList.asJava)
        case _ => throw new RuntimeException
      }

    loans.repartition(24)
      .map( v => {
        val out: StringWriter = new StringWriter()
//        StaticJaxbContext.marshaller.marshal(v._2, out)
        val context = JAXBContext.newInstance(v._2.getClass)
        val marshaller = context.createMarshaller()
        marshaller.setProperty(Marshaller.JAXB_FRAGMENT, true)
        out.toString
      })
      .saveAsTextFile(RESOURCE_PATH +
        "out.xml")

    println("Done")

//    Thread.sleep(1000000L)
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
