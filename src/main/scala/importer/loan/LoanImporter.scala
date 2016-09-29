package importer.loan

import java.time.LocalDate

import org.apache.spark.{SparkConf, SparkContext}

object LoanImporter {

  case class Loan(id: String,
                  startDate: LocalDate,
                  endDate: LocalDate,
                  drawings: Option[Iterable[Drawing]] = None,
                  counterparty: Option[String] = None,
                  commitment: Money = ZeroEuro,
                  undrawn: Money = ZeroEuro)

  case class Drawing(id: String,
                     startDate: LocalDate,
                     endDate: LocalDate,
                     outstanding: Money = ZeroEuro)

  case class Money(amount: BigDecimal,
                   currency: String)

  val ZeroEuro = Money(0, "EUR")

  def main(args: Array[String]): Unit ={
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
        case (a: Loan, b: Option[Iterable[Drawing]]) => a.copy(drawings = b)
        case _ => throw new RuntimeException
      }

    loansWithDrawings.mapValues(v => v.toString).values.saveAsTextFile("src/main/resources/importer/loan/out.xml")
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
