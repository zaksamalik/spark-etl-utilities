import org.scalatest.FunSuite

import com.civicboost.spark.etl.utilities.GeneralFunctions

/* ~~~~~~~~~~ General Functions ~~~~~~~~~~*/
class CleanStringTest extends FunSuite {
  test("GeneralFunctions.cleanString") {
    assert(GeneralFunctions.cleanString("\u0000") == "")
    assert(GeneralFunctions.cleanString(null) == null)
    assert(GeneralFunctions.cleanString("") == "")
    assert(GeneralFunctions.cleanString("abc ") == "abc")
    assert(GeneralFunctions.cleanString("abc 123\u0000") == "abc 123")
  }
}

class EmptyStringToNullTest extends FunSuite {
  test("GeneralFunctions.emptyStringToNull") {
    assert(GeneralFunctions.emptyStringToNull(" ") == null)
    assert(GeneralFunctions.emptyStringToNull("") == null)
    assert(GeneralFunctions.emptyStringToNull(null) == null)
    assert(GeneralFunctions.emptyStringToNull("abc ") == "abc ")
    assert(GeneralFunctions.emptyStringToNull("abc 123\u0000") == "abc 123\u0000")
  }
}

class MapBooleansTest extends FunSuite {
  test("GeneralFunctions.mapBooleansYNU") {
    // N
    assert(GeneralFunctions.mapBooleansYNU(false) == "N")
    assert(GeneralFunctions.mapBooleansYNU(0) == "N")
    assert(GeneralFunctions.mapBooleansYNU("0") == "N")
    assert(GeneralFunctions.mapBooleansYNU("f") == "N")
    assert(GeneralFunctions.mapBooleansYNU("F") == "N")
    assert(GeneralFunctions.mapBooleansYNU("false") == "N")
    assert(GeneralFunctions.mapBooleansYNU("False") == "N")
    assert(GeneralFunctions.mapBooleansYNU("FALSE") == "N")
    assert(GeneralFunctions.mapBooleansYNU("n") == "N")
    assert(GeneralFunctions.mapBooleansYNU("N") == "N")
    assert(GeneralFunctions.mapBooleansYNU("no") == "N")
    assert(GeneralFunctions.mapBooleansYNU("No") == "N")
    assert(GeneralFunctions.mapBooleansYNU("NO") == "N")
    // Y
    assert(GeneralFunctions.mapBooleansYNU(true) == "Y")
    assert(GeneralFunctions.mapBooleansYNU(1) == "Y")
    assert(GeneralFunctions.mapBooleansYNU("1") == "Y")
    assert(GeneralFunctions.mapBooleansYNU("t") == "Y")
    assert(GeneralFunctions.mapBooleansYNU("T") == "Y")
    assert(GeneralFunctions.mapBooleansYNU("true") == "Y")
    assert(GeneralFunctions.mapBooleansYNU("True") == "Y")
    assert(GeneralFunctions.mapBooleansYNU("TRUE") == "Y")
    assert(GeneralFunctions.mapBooleansYNU("y") == "Y")
    assert(GeneralFunctions.mapBooleansYNU("Y") == "Y")
    assert(GeneralFunctions.mapBooleansYNU("yes") == "Y")
    assert(GeneralFunctions.mapBooleansYNU("Yes") == "Y")
    assert(GeneralFunctions.mapBooleansYNU("YES") == "Y")
    // Unknown
    assert(GeneralFunctions.mapBooleansYNU("") == "Unknown")
    assert(GeneralFunctions.mapBooleansYNU(" ") == "Unknown")
    assert(GeneralFunctions.mapBooleansYNU(3) == "Unknown")
    assert(GeneralFunctions.mapBooleansYNU(3.0) == "Unknown")
    assert(GeneralFunctions.mapBooleansYNU(null) == "Unknown")
    assert(GeneralFunctions.mapBooleansYNU("foo") == "Unknown")
    assert(GeneralFunctions.mapBooleansYNU("BAR") == "Unknown")
  }
}

class StringIsNumberTest extends FunSuite {
  test("GeneralFunctions.stringIsNumber") {
    assert(GeneralFunctions.stringIsNumber("100"))
    assert(GeneralFunctions.stringIsNumber("-100"))
    assert(GeneralFunctions.stringIsNumber("(100)"))
    assert(GeneralFunctions.stringIsNumber("$100"))
    assert(GeneralFunctions.stringIsNumber("-$100"))
    assert(GeneralFunctions.stringIsNumber("($100)"))
    assert(GeneralFunctions.stringIsNumber("100%"))
    assert(GeneralFunctions.stringIsNumber("-100%"))
    assert(GeneralFunctions.stringIsNumber("(100%)"))
    //
    assert(GeneralFunctions.stringIsNumber("100.00"))
    assert(GeneralFunctions.stringIsNumber("-100.00"))
    assert(GeneralFunctions.stringIsNumber("(100.00)"))
    assert(GeneralFunctions.stringIsNumber("$100.00"))
    assert(GeneralFunctions.stringIsNumber("-$100.00"))
    assert(GeneralFunctions.stringIsNumber("($100.00)"))
    assert(GeneralFunctions.stringIsNumber("100.00%"))
    assert(GeneralFunctions.stringIsNumber("-100.00%"))
    assert(GeneralFunctions.stringIsNumber("(100.00%)"))
    //
    assert(GeneralFunctions.stringIsNumber("100 Apples"))
    assert(GeneralFunctions.stringIsNumber("$3.14/lb."))
    assert(GeneralFunctions.stringIsNumber("4 294 967 295,000"))
    assert(GeneralFunctions.stringIsNumber("4 294 967.295,000"))
    assert(GeneralFunctions.stringIsNumber("4.294.967.295,000"))
    assert(GeneralFunctions.stringIsNumber("4,294,967,295.00"))
    //
    assert(!GeneralFunctions.stringIsNumber(null))
    assert(!GeneralFunctions.stringIsNumber(""))
    assert(!GeneralFunctions.stringIsNumber(" "))
    assert(!GeneralFunctions.stringIsNumber("foo"))
  }
}

class StringToDoubleTest extends FunSuite {
  test("GeneralFunctions.stringToDouble") {
    assert(GeneralFunctions.stringToDouble("100").contains(100.00))
    assert(GeneralFunctions.stringToDouble("-100").contains(-100.00))
    assert(GeneralFunctions.stringToDouble("(100)").contains(-100.00))
    assert(GeneralFunctions.stringToDouble("$100").contains(100.00))
    assert(GeneralFunctions.stringToDouble("-$100").contains(-100.00))
    assert(GeneralFunctions.stringToDouble("($100)").contains(-100.00))
    assert(GeneralFunctions.stringToDouble("100%").contains(100.00))
    assert(GeneralFunctions.stringToDouble("-100%").contains(-100.00))
    assert(GeneralFunctions.stringToDouble("(100%)").contains(-100.00))
    //
    assert(GeneralFunctions.stringToDouble("100.00").contains(100.00))
    assert(GeneralFunctions.stringToDouble("-100.00").contains(-100.00))
    assert(GeneralFunctions.stringToDouble("(100.00)").contains(-100.00))
    assert(GeneralFunctions.stringToDouble("$100.00").contains(100.00))
    assert(GeneralFunctions.stringToDouble("-$100.00").contains(-100.00))
    assert(GeneralFunctions.stringToDouble("($100.00)").contains(-100.00))
    assert(GeneralFunctions.stringToDouble("100.00%").contains(100.00))
    assert(GeneralFunctions.stringToDouble("-100.00%").contains(-100.00))
    assert(GeneralFunctions.stringToDouble("(100.00%)").contains(-100.00))
    //
    assert(GeneralFunctions.stringToDouble("100 Apples").contains(100.00))
    assert(GeneralFunctions.stringToDouble("$3.14/lbs.").contains(3.14))
    assert(GeneralFunctions.stringToDouble("4 294 967 295,000", comma_for_decimal = true).contains(4294967295.00))
    assert(GeneralFunctions.stringToDouble("4 294 967.295,000", comma_for_decimal = true).contains(4294967295.00))
    assert(GeneralFunctions.stringToDouble("4.294.967.295,000", comma_for_decimal = true).contains(4294967295.00))
    assert(GeneralFunctions.stringToDouble("4,294,967,295.00").contains(4294967295.00))
    //
    assert(GeneralFunctions.stringToDouble(null) == null)
    assert(GeneralFunctions.stringToDouble("") == null)
    assert(GeneralFunctions.stringToDouble(" ") == null)
    assert(GeneralFunctions.stringToDouble("foo") == null)
  }
}