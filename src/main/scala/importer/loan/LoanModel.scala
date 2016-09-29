package importer.loan

import java.time.LocalDate
import java.util
import javax.xml.bind.annotation._
import javax.xml.bind.annotation.adapters.{XmlAdapter, XmlJavaTypeAdapter}

import scala.annotation.meta.field
import scala.tools.nsc.interpreter._

object LoanModel {

  type xmlElement     = XmlElement @field
  type xmlElementWrapper     = XmlElementWrapper @field
  type xmlTypeAdapter = XmlJavaTypeAdapter @field

  class StringOptionAdapter extends OptionAdapter[String]("", "")
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

}
