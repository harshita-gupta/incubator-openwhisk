//Grammar
//e ::= c | [e1, e2, ..., en] | {str1: e1, ..., strn:en} | .complexPat | . | (e1 ^ complexPat) | 
//      (e) | e1 * e2 | e1 == e2 | e1 != e2 | e1 >= e2 | e1 <= e2 | e1 < e2 | e1 > e2 | 
//      e1 && e2 | e1 || e2 | if e1 then e2 else e3
//complexPat ::= simplePat | simplePat . complexPat
//simplePat ::= [n] | id | [str]

package org.apache.openwhisk.core.controller

import spray.json._

case class DSLParsingError(msg: String, private val cause: Throwable = None.orNull) extends Exception ("Parsing Failed: " + msg, cause)

class DSLInterpreterException (private val msg: String, private val cause: Throwable = None.orNull) extends Exception (msg, cause)
class DSLInterpreterFieldNotFoundException (private val msg: String) extends DSLInterpreterException (msg)
class DSLInterpreterKeyNotFoundException (private val msg: String) extends DSLInterpreterException (msg)
class DSLInterpreterIndexOutOfBoundsException (private val msg: String) extends DSLInterpreterException (msg)

sealed class DagularDSL () {
  def apply (code : String, jsValue : JsValue) : JsValue = {
    astNode match {
      case exp : Expression => interpretExpression (exp, jsValue)
      //case pat : Pattern => interpretPattern (pat, jsObject)
      case _ => throw new IllegalArgumentException ("Not handling this ASTNode in interpreter $astNode")
    }
  }
  
  def jsValueToNumber (jsValue : JsValue) : BigDecimal = {
    try {
      return jsValue.asInstanceOf[JsNumber].value
    } catch {
      case e: Exception => throw new IllegalArgumentException (s"Cannot convert $jsValue to Number")
    }
  }
  
  def jsValueToBoolean (jsValue : JsValue) : Boolean = {
    try {
      return jsValue.asInstanceOf[JsBoolean].value
    } catch {
      case e: Exception => throw new IllegalArgumentException (s"Cannot convert $jsValue to Boolean")
    }
  }
  
  def jsObjectsUnion (jsObject1 : JsObject, jsObject2 : JsObject) : Map[String, JsValue]= {
    val JsObject (ret1Map) = jsObject1
    val JsObject (ret2Map) = jsObject2
    
    var retMap : Map[String, JsValue] = ret1Map
    
    for ((k,v) <- ret2Map) {
      if (v.isInstanceOf[JsObject] && (retMap contains k)) {
        retMap =  retMap + (k -> JsObject (jsObjectsUnion (v.asInstanceOf[JsObject], ret1Map(k).asInstanceOf[JsObject])))
      } else {
        retMap = retMap + (k -> v)
      }
    }
    
    retMap
  }
  
  def interpretExpression (expr : Expression, jsObject: JsValue) : JsValue = {
    expr match {
      case NumberConstant (num) => JsNumber (num)
      case StringConstant (str) => JsString (str)
      case BoolConstant (bool) => JsBoolean (bool)
      case NullConstant () => JsNull
      case ArrayExpression (exprs) => JsArray ((exprs.map (expr => interpretExpression(expr, jsObject))).toVector)
      case JsonObjectExpression (keyVals) => {
        val map : Map [String, JsValue] = keyVals.map {case KeyValuePair (key, expr) => (key, interpretExpression (expr, jsObject))}.toMap
        JsObject (map)
      }
      case PatternExistExpression (e, pat) => {
        val jsVal = interpretExpression (e, jsObject)
        try {
          val res = interpretPattern (pat, jsVal)
          JsTrue
        } catch {
          case e : DSLInterpreterFieldNotFoundException => JsFalse
          case e : DSLInterpreterKeyNotFoundException => JsFalse
          case e : DSLInterpreterIndexOutOfBoundsException => JsFalse
          case _ : Throwable => JsFalse 
        }
      }
      case BinaryOperatorExpression (e1, op, e2) => {
        val ret1 = interpretExpression (e1, jsObject)
        val ret2 = interpretExpression (e2, jsObject)
        op.getOperator match {
          case Operator.IsEqual => if (ret1 == ret2) JsTrue else JsFalse 
          case Operator.IsNotEqual => if (ret1 != ret2) JsTrue else JsFalse
          case Operator.Union => {
            var ret1JsObject : JsObject = null
            var ret2JsObject : JsObject = null
            
            try {
              ret1JsObject = ret1.asJsObject
            } catch {
              case e: Exception => throw new IllegalArgumentException (s"First argument to Union operator $ret1 is not JsObject")
            }
            
            try {
              ret2JsObject = ret2.asJsObject
            } catch {
              case e: Exception => throw new IllegalArgumentException (s"Second argument to Union operator $ret2 is not JsObject")
            }
            
            val JsObject (ret1Map) = ret1JsObject
            val JsObject (ret2Map) = ret2JsObject
            JsObject(jsObjectsUnion (ret1JsObject, ret2JsObject))
          }
          
          case Operator.IsGreaterThan => {
            val op1 = jsValueToNumber (ret1)
            val op2 = jsValueToNumber (ret2)
            
            JsBoolean (op1 > op2)
          }
          
          case Operator.IsGreaterEqual => {
            val op1 = jsValueToNumber (ret1)
            val op2 = jsValueToNumber (ret2)
            
            JsBoolean (op1 >= op2)
          }
          
          case Operator.IsLessThan => {
            val op1 = jsValueToNumber (ret1)
            val op2 = jsValueToNumber (ret2)
            
            JsBoolean (op1 <= op2)
          }
          
          case Operator.IsLessEqual => {
            val op1 = jsValueToNumber (ret1)
            val op2 = jsValueToNumber (ret2)
            
            JsBoolean (op1 < op2)
          }
          
          case Operator.LogicalAnd => {
            val op1 = jsValueToBoolean (ret1)
            if (op1 == false) {
              JsFalse
            } else {
              val op2 = jsValueToBoolean (ret2)
            
              JsBoolean (op1 && op2)
            }
          }
          
          case Operator.LogicalOr => {
            val op1 = jsValueToBoolean (ret1)
            if (op1 == true) {
              JsTrue
            } else {
              val op2 = jsValueToBoolean (ret2)
              
              JsBoolean (op1 || op2)
            }
          }
        }
      }
      case IfThenElseExpression(condExp, exp1, exp2) => {
        if (interpretExpression (condExp, jsObject) == JsTrue) {
          interpretExpression(exp1, jsObject)
        }
        else {
          interpretExpression(exp2, jsObject)
        }
      }
      case PatternExpression (pattern) => interpretPattern (pattern, jsObject)
      case EmptyPatternExpression () => jsObject
      case _ => throw new IllegalArgumentException ("Not handling this expression") 
    }
  }
  
  private def jsValueToJsObject (jsValue: JsValue) : JsObject = {
    try {
      jsValue.asInstanceOf[JsObject]
    }
    catch {
      case e : Exception => throw new DSLInterpreterException (s"Cannot convert $jsValue to JsObject")
    }
  }
  
  def interpretPattern (pat: Pattern, jsValue: JsValue) : JsValue = {
    pat match {
      //~ case Identifier (id) => {
        //~ val JsObject (mapJsObject : Map[String, JsValue]) = jsValueToJsObject (jsValue)
        //~ if (!(mapJsObject contains id)) {
          //~ throw new DSLInterpreterException (s"$id not found in JsObject $jsValue")
        //~ }
        //~ mapJsObject (id)
      //~ }
      case ArrayIndexPattern (index) => {
        try {
          val jsArray = jsValue.asInstanceOf [JsArray]
          val JsArray (elements) = jsArray
          elements (index)
        }
        catch {
          case ex : java.lang.ClassCastException => throw new DSLInterpreterException (s"$jsValue is not Array. Error interpreting ArrayIndexPattern($index)")
          case ex : java.lang.ArrayIndexOutOfBoundsException => throw new DSLInterpreterIndexOutOfBoundsException (s"$index out of array of length")
        }
      }
      
      case FieldPattern (field) => {
        val JsObject (mapJsObject : Map[String, JsValue]) = jsValueToJsObject (jsValue)
        try {
          mapJsObject (field)
        }
        catch {
          case e: Exception => throw new DSLInterpreterKeyNotFoundException (s"Field $field not present in JsObject $mapJsObject")
        }
      }
      
      case StringPattern (str) => {
        val JsObject (mapJsObject : Map[String, JsValue]) = jsValueToJsObject (jsValue)
        try {
          mapJsObject (str)
        }
        catch {
          case e: Exception => throw new DSLInterpreterFieldNotFoundException (s"StringPattern $str not present in JsObject $mapJsObject")
        }
      }
      case ContinuousPatterns (pattern1, pattern2) => interpretPattern (pattern2, interpretPattern (pattern1, jsValue))
      case DotPattern (pattern) => interpretPattern (pattern, jsValue)
      case _ => throw new IllegalArgumentException ("Not handling this pattern") 
    }
  }
}
