package importer.loan

import java.time.LocalDate
import javax.xml.bind.{JAXBContext, Marshaller}

import importer.loan.LoanModel.Loan

object StaticJaxbContext {
  private val loan: Loan = LoanModel.Loan("", LocalDate.MAX, LocalDate.MIN)
  private val context = JAXBContext.newInstance( loan.getClass)
  val marshaller = context.createMarshaller()
  marshaller.setProperty(Marshaller.JAXB_FRAGMENT, true)
}
