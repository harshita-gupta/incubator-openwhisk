//Grammar
//e ::= c | [e1, e2, ..., en] | {str1: e1, ..., strn:en} | .complexPat | . | (e1 ^ complexPat) | 
//      (e) | e1 * e2 | e1 == e2 | e1 != e2 | e1 >= e2 | e1 <= e2 | e1 < e2 | e1 > e2 | 
//      e1 && e2 | e1 || e2 | if e1 then e2 else e3
//complexPat ::= simplePat | simplePat . complexPat
//simplePat ::= [n] | id | [str]

package whisk.core.controller

import scala.util.parsing.combinator._
import scala.util.parsing.input.Positional
import scala.util.parsing.input.{Position, Reader}
import spray.json._

sealed trait Token extends Positional
class ConstantToken extends Token
class ConditionalOperatorToken extends Token
class LogicalOperatorToken extends Token
case class NumberToken (num :String) extends ConstantToken
case class BooleanToken (bool: String) extends ConstantToken
case class StringToken (str : String) extends ConstantToken
case object NullToken  extends ConstantToken

case object BigBraceStart extends Token // [
case object BigBraceEnd extends Token // ]
case object CurlyBraceStart extends Token // {
case object CurlyBraceEnd extends Token // }
case object BracketStart extends Token // (
case object BracketEnd extends Token // )
case object UnionOperatorToken extends Token //*
case object PatternExistOperatorToken extends Token //^
case object PatternDot extends Token //.
case object KeyValueSeparator extends Token //:
case object CommaSeparator extends Token
case object If extends Token
case object Then extends Token
case object Else extends Token
case object IsEqualToken extends ConditionalOperatorToken
case object IsNotEqualToken extends ConditionalOperatorToken
case object IsGreaterThanToken extends ConditionalOperatorToken
case object IsGreaterEqualToken extends ConditionalOperatorToken
case object IsLessThanToken extends ConditionalOperatorToken
case object IsLessEqualToken extends ConditionalOperatorToken
case object LogicalAndToken extends LogicalOperatorToken
case object LogicalOrToken extends LogicalOperatorToken

case class IdentifierToken (id: String) extends Token

trait ParsingError
case class TokenizerError(msg: String, private val cause: Throwable = None.orNull) extends Exception ("Tokenization Failed: " + msg, cause)

object Tokenizer extends RegexParsers {
  
  def bigBraceStart = "[" ^^ {_ => BigBraceStart}
  def bigBraceEnd = "]" ^^ {_ => BigBraceEnd}
  def curlyBraceStart = "{" ^^ {_ => CurlyBraceStart}
  def curlyBraceEnd = "}" ^^ {_ => CurlyBraceEnd}
  def bracketStart = "(" ^^ {_ => BracketStart}
  def bracketEnd = ")" ^^ {_ => BracketEnd}
  def unionOperator = "*" ^^ {_ => UnionOperatorToken}
  def patternExistOperator = "^" ^^ {_ => PatternExistOperatorToken}
  def patternDot = "." ^^ {_ => PatternDot}
  def keyValueSeparator = ":" ^^ {_ => KeyValueSeparator}
  def commaSeparator = "," ^^ {_ => CommaSeparator}
  def ifKeyword = "if" ^^ {_ => If}
  def elseKeyword = "else" ^^ {_ => Else}
  def thenKeyword = "then" ^^ {_ => Then}
  def isEqualToken = "==" ^^ {_ => IsEqualToken}
  def isNotEqualToken = "!=" ^^ {_ => IsNotEqualToken}
  def isGreaterThanToken = ">" ^^ {_ => IsGreaterThanToken}
  def isGreaterEqualToken = ">=" ^^ {_ => IsGreaterEqualToken}
  def isLessThanToken = "<" ^^ {_ => IsLessThanToken}
  def isLessEqualToken = "<=" ^^ {_ => IsLessEqualToken}
  def logicalAndToken = "&&" ^^ {_ => LogicalAndToken}
  def logicalOrToken = "||" ^^ {_ => LogicalOrToken}
  
  def identifier: Parser[IdentifierToken] = {
    //TODO: null is recognized as identifier
    "[a-zA-Z_][a-zA-Z0-9_]*".r ^^ { str => IdentifierToken(str) }
  }


  def stringToken : Parser[StringToken] = {
    """"[^"]*"""".r ^^ { str =>
      StringToken (str.substring (1, str.length - 1))
    }
  }
  
  def numberToken : Parser[NumberToken] = {
    """[-+]?\d*\.\d+|\d+""".r ^^ { num => 
      NumberToken (num)
    }
  }
  
  def booleanToken : Parser[BooleanToken] = {
    """(false|true)""".r ^^ { bool =>
      BooleanToken (bool)
    }
  }
  
  def nullToken = "null" ^^ (_ => NullToken)
  
  def tokens:Parser[List[Token]] = {
    phrase (rep1 (bigBraceStart   | bigBraceEnd          |
                  curlyBraceStart | curlyBraceEnd        |
                  bracketStart    | bracketEnd           |
                  unionOperator   | patternExistOperator |
                  patternDot      | keyValueSeparator    |
                  stringToken     | numberToken          | 
                  booleanToken    | nullToken            |
                  commaSeparator  | ifKeyword            | 
                  elseKeyword     | thenKeyword          | 
                  isEqualToken    | isNotEqualToken      |
                  isGreaterThanToken | isGreaterEqualToken |
                  isLessThanToken | isLessEqualToken       |
                  logicalAndToken | logicalOrToken         |
                  booleanToken    | nullToken              |
                  identifier))
  }
  
  def apply(code: String): Either [TokenizerError, List[Token]] = {
    System.out.println ("Doing Tokenization of code " + code)
    parse (tokens, code) match {
      case NoSuccess(msg, next) => Left(TokenizerError (msg))
      case Success(result, next) => Right (result)
    }
  }
}

case class TokenPosition (val col:Int) extends Position {
  override def column = col
  override def line = 0
  override def lineContents = ""
}

class TokenReader (tokens: List[Token]) extends Reader[Token] {
  override def first: Token = tokens.head
  override def atEnd: Boolean = {if (tokens.isEmpty == true) true else false}
  override def pos: Position = new TokenPosition(3-tokens.length)
  override def rest: Reader[Token] = new TokenReader(tokens.tail)
  override def drop (n: Int) = new TokenReader(tokens.drop(n))
}

object Operator extends Enumeration {
  type Operator = Value
  val IsEqual, IsNotEqual, IsGreaterEqual, IsGreaterThan, IsLessThan, IsLessEqual,
  Union, LogicalAnd, LogicalOr, PatternExist = Value
}

trait ASTNode
trait Expression extends ASTNode
trait Pattern extends ASTNode
trait Constant extends Expression
class Operator (op: Operator.Value) extends ASTNode {
  def getOperator = op
}
class BinaryOperator (op: Operator.Value) extends Operator (op)

case class NumberConstant (num : Float) extends Constant
case class BoolConstant (bool : Boolean) extends Constant
case class StringConstant (str : String) extends Constant
case class NullConstant () extends Constant

case class KeyValuePair (key: String, value: Expression) extends ASTNode
case class Identifier (id: String) extends Pattern

case class BinaryOperatorExpression (exp1 : Expression, op: Operator, exp2 : Expression) extends Expression
case class ArrayExpression (expressions : List [Expression]) extends Expression
case class JsonObjectExpression (map : List[KeyValuePair]) extends Expression
case class PatternExpression (pattern: Pattern) extends Expression
case class EmptyPatternExpression() extends Expression
case class IfThenElseExpression(cond : Expression, thenExp : Expression, elseExp : Expression) extends Expression

case class ArrayIndexPattern (index : Int) extends Pattern
case class FieldPattern (field : String) extends Pattern
case class StringPattern (str : String) extends Pattern
case class ContinuousPatterns (pattern1 : Pattern, pattern2: Pattern) extends Pattern
case class DotPattern (pattern: Pattern) extends Pattern

case class BigBraceStartASTNode () extends ASTNode // [
case class BigBraceEndASTNode () extends ASTNode // ]
case class CurlyBraceStartASTNode () extends ASTNode // {
case class CurlyBraceEndASTNode () extends ASTNode // }
case class BracketStartASTNode () extends ASTNode // (
case class BracketEndASTNode () extends ASTNode // )
case class PatternDotASTNode () extends ASTNode //.
case class KeyValueSeparatorASTNode () extends ASTNode //:
case class CommaSeparatorASTNode () extends ASTNode //:
case class IfASTNode() extends ASTNode
case class ThenASTNode() extends ASTNode
case class ElseASTNode() extends ASTNode
case class IsEqualOperator() extends BinaryOperator (Operator.IsEqual)
case class IsNotEqualOperator() extends BinaryOperator (Operator.IsNotEqual)
case class IsGreaterEqualOperator() extends BinaryOperator (Operator.IsGreaterEqual)
case class IsGreaterThanOperator() extends BinaryOperator (Operator.IsGreaterThan)
case class IsLessThanOperator () extends BinaryOperator (Operator.IsLessThan)
case class IsLessEqualOperator () extends BinaryOperator (Operator.IsLessEqual)
case class LogicalOrOperator () extends BinaryOperator (Operator.LogicalOr)
case class LogicalAndOperator () extends BinaryOperator (Operator.LogicalAnd)
case class UnionOperator () extends BinaryOperator (Operator.Union)
case class PatternExistOperator () extends ASTNode 
case class PatternExistExpression (exp: Expression, pat: Pattern) extends Expression

case class DSLParsingError(msg: String, private val cause: Throwable = None.orNull) extends Exception ("Parsing Failed: " + msg, cause)

object DSLParser extends PackratParsers {
  override type Elem = Token
  
  lazy val stringConstant : PackratParser[StringConstant] = {
    accept ("string constant", {case StringToken(n) => StringConstant (n)})
  }
  
  lazy val identifier : PackratParser[Identifier] = {
    accept ("identifier", {case IdentifierToken(id) => Identifier (id)})
  }
  
  lazy val numberConstant : PackratParser[NumberConstant] = {
    accept ("number", {case n @ NumberToken (num) => NumberConstant (num.toFloat)})
  }
  
  lazy val boolConstant : PackratParser[BoolConstant] = {
    accept ("boolean", {case b @ BooleanToken (bool) => if (bool == "true") BoolConstant(true) else BoolConstant(false)})
  }
  
  lazy val nullConstant : PackratParser[NullConstant] = {
    accept ("null", {case NullToken => NullConstant ()})
  }
  
  lazy val commaSeparatorASTNode : PackratParser[CommaSeparatorASTNode] = {
    accept (",", {case s @ CommaSeparator => CommaSeparatorASTNode ()})
  }
  
  lazy val bigBraceStartASTNode : PackratParser[BigBraceStartASTNode] = {
    accept ("[", {case BigBraceStart => BigBraceStartASTNode ()})
  }
  
  lazy val bigBraceEndASTNode : PackratParser[BigBraceEndASTNode] = {
    accept ("]", {case BigBraceEnd => BigBraceEndASTNode ()})
  }
  
  lazy val curlyBraceStartASTNode : PackratParser[CurlyBraceStartASTNode] = {
    accept ("{", {case CurlyBraceStart => CurlyBraceStartASTNode ()})
  }
  
  lazy val curlyBraceEndASTNode : PackratParser[CurlyBraceEndASTNode] = {
    accept ("}", {case CurlyBraceEnd => CurlyBraceEndASTNode ()})
  }
  
  lazy val bracketStartASTNode : PackratParser[BracketStartASTNode] = {
    accept ("}", {case BracketStart => BracketStartASTNode ()})
  }
  
  lazy val bracketEndASTNode : PackratParser[BracketEndASTNode] = {
    accept ("}", {case BracketEnd => BracketEndASTNode ()})
  }
  
  lazy val patternDotASTNode : PackratParser[PatternDotASTNode] = {
    accept (".", {case PatternDot => PatternDotASTNode ()})
  }
  
  lazy val keyValueSeparatorASTNode : PackratParser[KeyValueSeparatorASTNode] = {
    accept (":", {case KeyValueSeparator => KeyValueSeparatorASTNode ()})
  }
  
  lazy val ifASTNode : PackratParser[IfASTNode] = {
    accept ("if", {case If => IfASTNode()})
  }
  
  lazy val elseASTNode : PackratParser[ElseASTNode] = {
    accept ("else", {case Else => ElseASTNode()})
  }
  
  lazy val thenASTNode : PackratParser[ThenASTNode] = {
    accept ("then", {case Then => ThenASTNode()})
  }
  
  lazy val isEqualOperator : PackratParser[IsEqualOperator] = {
    accept ("==", {case IsEqualToken => IsEqualOperator()})
  }
  
  lazy val isNotEqualOperator : PackratParser[IsNotEqualOperator] = {
    accept ("==", {case IsNotEqualToken => IsNotEqualOperator()})
  }
  
  lazy val isGreaterEqualOperator : PackratParser[IsGreaterEqualOperator] = {
    accept (">=", {case IsGreaterEqualToken => IsGreaterEqualOperator()})
  }
  
  lazy val isGreaterThanOperator : PackratParser[IsGreaterThanOperator] = {
    accept (">", {case IsGreaterThanToken => IsGreaterThanOperator()})
  }
  
  lazy val isLessThanOperator : PackratParser[IsLessThanOperator] = {
    accept ("<=", {case IsLessThanToken => IsLessThanOperator()})
  }
  
  lazy val isLessEqualOperator : PackratParser[IsLessEqualOperator] = {
    accept ("<", {case IsLessEqualToken => IsLessEqualOperator()})
  }

  lazy val logicalOrOperator : PackratParser[LogicalOrOperator] = {
    accept ("||", {case LogicalOrToken => LogicalOrOperator()})
  }
  
  lazy val logicalAndOperator : PackratParser[LogicalAndOperator] = {
    accept ("&&", {case LogicalAndToken => LogicalAndOperator()})
  }
  
  lazy val binaryOperator : PackratParser[BinaryOperator] = {
    isEqualOperator | isNotEqualOperator | unionOperator | 
    isGreaterEqualOperator | isGreaterThanOperator | isLessThanOperator | 
    isLessEqualOperator | logicalOrOperator | logicalAndOperator
  }

  lazy val patternExistOperator : PackratParser[PatternExistOperator] = {
    accept ("^", {case PatternExistOperatorToken => PatternExistOperator ()})
  }
  
  lazy val unionOperator : PackratParser[UnionOperator] = {
    accept ("*", {case UnionOperatorToken => UnionOperator ()})
  }
  
  lazy val patternExistExpression : PackratParser[PatternExistExpression] = {
    (expression ~ patternExistOperator ~ complexPattern) ^^ {
      case exp ~ _ ~ pat => PatternExistExpression(exp, pat)
    }
  }
  
  lazy val bracketExpression : PackratParser[Expression] = {
    (bracketStartASTNode ~ expression ~ bracketEndASTNode) ^^ {
      case _ ~ exp ~ _ => exp
    }
  }
  
  lazy val binaryOperatorExpression : PackratParser[BinaryOperatorExpression] = {
    (expression ~ binaryOperator ~ expression) ^^ {
      case exp1 ~ op ~ exp2 => BinaryOperatorExpression (exp1, op, exp2)
    }
  }
  
  lazy val fieldPattern : PackratParser[FieldPattern] = {
    (identifier) ^^ {case (i @ Identifier(id)) => FieldPattern (id)}
  }
  
  lazy val stringPattern : PackratParser[StringPattern] = {
    (bigBraceStartASTNode ~ stringConstant ~ bigBraceEndASTNode) ^^ {
      case sbrace ~ StringConstant(str) ~ ebrace => StringPattern (str)
    }
  }
  
  lazy val arrayIndexPattern : PackratParser[ArrayIndexPattern] = {
    (bigBraceStartASTNode ~ numberConstant ~ bigBraceEndASTNode) ^^ {
      case sbrace ~ NumberConstant(n) ~ ebrace => ArrayIndexPattern (n.toInt)
    }
  }

  lazy val keyValuePair : PackratParser[KeyValuePair] = {
    (stringConstant ~ keyValueSeparatorASTNode ~ expression) ^^ {case StringConstant(str) ~ sep ~ exp => KeyValuePair (str, exp)}
  }
  
  lazy val arrayExpression : PackratParser[ArrayExpression] = {
    (bigBraceStartASTNode ~ rep1sep(expression, commaSeparatorASTNode) ~ bigBraceEndASTNode) ^^ {case sbrace ~ exps ~ ebrace => ArrayExpression (exps)}
  }
  
  lazy val jsonObjectExpression : PackratParser[JsonObjectExpression] = {
    (curlyBraceStartASTNode ~ rep1sep(keyValuePair, commaSeparatorASTNode) ~ curlyBraceEndASTNode) ^^ {case sbrace ~ kvpairs ~ ebrace => JsonObjectExpression (kvpairs)}
  }
  
  lazy val patternExpression : PackratParser[PatternExpression] = {
    (patternDotASTNode ~ complexPattern) ^^ {case dot ~ pat => PatternExpression (pat)}
  }
  
  lazy val emptyPatternExpression : PackratParser[EmptyPatternExpression] = {
    (patternDotASTNode) ^^ {case dot => EmptyPatternExpression ()}
  }
  
  //~ def dotPattern : Parser [DotPattern] = {
    //~ (patternDotASTNode ~ pattern) ^^ {case dot ~ pat => DotPattern (pat)}
  //~ }
  
  lazy val continuousPatterns : PackratParser[ContinuousPatterns] = {
    (simplePattern ~ patternDotASTNode ~ complexPattern) ^^ { case pat1 ~ dot ~ pat2 => ContinuousPatterns (pat1, pat2)}
  }
  
  lazy val ifThenElseExpression : PackratParser[IfThenElseExpression] = {
    (ifASTNode ~ expression ~ thenASTNode ~ expression ~ elseASTNode ~ expression) ^^ {
      case _ ~ condExp ~ _ ~ exp1 ~ _ ~ exp2 => IfThenElseExpression(condExp, exp1, exp2)
    }
  }
  
  lazy val expression : PackratParser[Expression] = {
    patternExistExpression   | bracketExpression        | ifThenElseExpression     | 
    binaryOperatorExpression | patternExpression        |
    stringConstant           | boolConstant             | 
    nullConstant             | numberConstant           | 
    arrayExpression          | jsonObjectExpression     | 
    emptyPatternExpression 
  }
  
  lazy val simplePattern : PackratParser[Pattern] = {
    arrayIndexPattern | stringPattern | fieldPattern
  }
  
  lazy val complexPattern : PackratParser[Pattern] = {
    continuousPatterns | simplePattern
    
  }
  
  lazy val result : PackratParser[Expression] = {
    phrase (expression)
  }
  
  def apply(tokens: List[Token]): Either [DSLParsingError, Expression] = {
    val reader = new TokenReader (tokens)
    result (new PackratReader(reader)) match {
      case NoSuccess(msg, next) => Left(DSLParsingError (msg + " " + next))
      case Success(result, next) => Right (result)
    }
  }
}

class DSLInterpreterException (private val msg: String, private val cause: Throwable = None.orNull) extends Exception (msg, cause)
class DSLInterpreterFieldNotFoundException (private val msg: String) extends DSLInterpreterException (msg)
class DSLInterpreterKeyNotFoundException (private val msg: String) extends DSLInterpreterException (msg)
class DSLInterpreterIndexOutOfBoundsException (private val msg: String) extends DSLInterpreterException (msg)
          
sealed class ProjectionDSL () {
  def apply (code : String, jsValue : JsValue) : JsValue = {
    val lexerResult = Tokenizer (code)
    val tokens = lexerResult match {
      case Right(t) => t
      case Left(m) => throw m
    }
    
    System.out.println (s"Tokens are $tokens")
    val astNode = DSLParser (tokens) match {
      case Right(t) => t
      case Left(m) => throw m
    } 
    
    System.out.println (s"ASTNode after parsing is $astNode")
    
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