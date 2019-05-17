//Grammar
//e ::= c | [e1, e2, ..., en] | {str1: e1, ..., strn:en} | .complexPat | . | (e1 ^ complexPat) | 
//      (e) | e1 * e2 | e1 == e2 | e1 != e2 | e1 >= e2 | e1 <= e2 | e1 < e2 | e1 > e2 | 
//      e1 && e2 | e1 || e2 | if e1 then e2 else e3
//complexPat ::= simplePat | simplePat . complexPat
//simplePat ::= [n] | id | [str]

package org.apache.openwhisk.core.controller

import spray.json._

sealed class DagularDSL () {
  def apply (prog : Map [String, JsValue], jsValue : JsValue) : JsValue = {
    System.out.println(s"we made it\n")
    JsObject (("out"-> JsString("hello, world!")))
  }
}
