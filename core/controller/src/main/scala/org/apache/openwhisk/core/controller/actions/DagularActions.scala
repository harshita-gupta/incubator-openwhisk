/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.core.controller.actions

import java.time.{Clock, Instant}

import akka.actor.ActorSystem
import spray.json._
import org.apache.openwhisk.common.{Logging, TransactionId, UserEvents}
import org.apache.openwhisk.core.connector.{EventMessage, MessagingProvider}
import org.apache.openwhisk.core.controller.WhiskServices
import org.apache.openwhisk.core.database.{ActivationStore, UserContext}
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.types._
import org.apache.openwhisk.spi.SpiLoader

import scala.collection._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

protected[actions] trait DagularActions {
  /** The core collections require backend services to be injected in this trait. */
  services: WhiskServices =>

  /** An actor system for timed based futures. */
  protected implicit val actorSystem: ActorSystem

  /** An execution context for futures. */
  protected implicit val executionContext: ExecutionContext

  protected implicit val logging: Logging

  /** Database service to CRUD actions. */
  protected val entityStore: EntityStore

  /** Database service to get activations. */
  protected val activationStore: ActivationStore

  /** Instance of the controller. This is needed to write user-metrics. */
  protected val activeAckTopicIndex: ControllerInstanceId

  /** Message producer. This is needed to write user-metrics. */
  private val messagingProvider = SpiLoader.get[MessagingProvider]
  private val producer = messagingProvider.getProducer(services.whiskConfig)

  /** A method that knows how to invoke a single primitive action. */
  protected[actions] def invokeAction(
    user: Identity,
    action: WhiskActionMetaData,
    payload: Option[JsObject],
    waitForResponse: Option[FiniteDuration],
    cause: Option[ActivationId])(implicit transid: TransactionId): Future[Either[ActivationId, WhiskActivation]]

  protected[actions] def invokeDagular(
    user: Identity,
    action: WhiskActionMetaData,
    payload: Option[JsObject],
    waitForResponse: Option[FiniteDuration],
    cause: Option[ActivationId])(implicit transid: TransactionId): Future[Either[ActivationId, WhiskActivation]] = {
      
      val context = UserContext(user)
      
      val start = Instant.now(Clock.systemUTC())
      
      System.out.println (s"invoke dagular")
      val DagularExecMetaData(code) = action.exec
      System.out.println (s"invokeDagular: program is $code")
      
      // run dagular program
      val input = payload.getOrElse(JsObject.empty)
      val result = new DagularDSL(user, cause)(transid)(code, input)
      System.out.println (s"invokeDagular: result: $result for code $code and payload $input")
      
      val end = Instant.now(Clock.systemUTC())
  
      // create the whisk activation for the final result
      result map { result =>
        val activation = WhiskActivation(
          namespace = user.namespace.name.toPath,
          name = action.name,
          user.subject,
          activationId = activationIdFactory.make(),
          start = start,
          end = end,
          cause = cause,
          response = ActivationResponse.success(Option(result)),
          version = action.version,
          publish = false,
          duration = Some(end.getEpochSecond() - start.getEpochSecond()))
    
        if (UserEvents.enabled) {
          EventMessage.from(activation, s"recording activation '${activation.activationId}'", user.namespace.uuid) match {
            case Success(msg) => UserEvents.send(producer, msg)
            case Failure(t)   => logging.warn(this, s"activation event was not sent: $t")
          }
        }
        activationStore.storeAfterCheck(activation, context)(transid, notifier = None)
    
        Right(activation)
      }
  }
  
  // AST nodes after slight decode
  private trait DagularAST
  private case class DagularNode(data : String, children : Vector[DagularAST]) extends DagularAST // we're short on time...
  private case class DagularLeaf(v : JsValue) extends DagularAST
  
  private abstract class DagularValue {
    // this future will only finish once all the components of this DagularValue finish
    def toJsValue () : Future[JsValue]
  }
  private case class DagularAtom(v : JsValue) extends DagularValue {
    override def toJsValue () = Future { v }
  }
  private case class DagularArray(v : Vector[Future[DagularValue]]) extends DagularValue {
    def toJsValue () = Future.sequence(v.map(_ flatMap {_.toJsValue()})) map {JsArray(_)}
  }
  private case class DagularObject(v : Map[String, Future[DagularValue]]) extends DagularValue {
    def toJsValue () = {
      val (ids, vals) = v.unzip
      Future.sequence(vals.map(_ flatMap {_.toJsValue()})) map {v => JsObject(ids.zip(v).toMap)}
    }
  }
  
  private class DagularDSL(user: Identity, cause: Option[ActivationId])(implicit transid: TransactionId) {
    
    def apply(code : String, dagInput : JsObject) : Future[JsValue] = {
      System.out.println(s"OMG the dagular interpreter just got called")
      
      // prepare dagular program and initial environment
      val dagProg = parseDagular(code)
      val dagEnv = Map[String, Future[DagularValue]] ("input" -> Future { DagularAtom(dagInput) })
      
      interpretDagular(dagProg, dagEnv) flatMap {_.toJsValue()}
    }
    
    // turn some dagular code into a more amenable internal representation
    // we expect to receive something parseable as JSON
    private def parseDagular(code : String) : DagularAST = {
      val json = code.parseJson
      jsonToDagularAST(json)
    }
    
    private def jsonToDagularAST(json : JsValue) : DagularAST = {
      val JsObject(map) = json.asJsObject(s"dagular parse found non-object")
      val data = map.get("data") match {
        case Some(JsString(s)) => s
        case Some(s)           => throw new IllegalArgumentException (s"dagular parse found non-string AST node name $s")
        case None              => throw new IllegalArgumentException (s"dagular parse found object with missing data field")
      }
      val children = map.get("children") match {
        case Some(JsArray(s))  => s
        case Some(s)           => throw new IllegalArgumentException (s"dagular parse found non-array in AST node children")
        case None              => throw new IllegalArgumentException (s"dagular parse found object with missing children field")
      }
      
      def needs_children(data : String, count : Int, children : Vector[DagularAST]) : DagularAST = {
        if (children.length != count)
          throw new IllegalArgumentException (s"dagular parse found ${children.length} children in ${data}: expected 3")
        else {
          DagularNode(data, children)
        }
      }
      
      // parse args and check arg counts
      data match {
        case "id" => { // [string]
          needs_children(data, 1, children map DagularLeaf)
        }
        
        case "number" => { // [number]
          needs_children(data, 1, children map DagularLeaf)
        }
        
        case "string" => { // [string]
          needs_children(data, 1, children map DagularLeaf)
        }
        
        case "list" => { // [expr1, expr2, ...]
          DagularNode(data, children map jsonToDagularAST)
        }
        
        case "pair" => { // [string, expr]
          DagularNode(data, children map jsonToDagularAST)
        }
        
        case "dict" => { // [pair1, pair2, ...]
          DagularNode(data, children map jsonToDagularAST)
        }
        
        case "assign" => { // [id, expr]
          needs_children(data, 2, children map jsonToDagularAST)
        }
        
        case "return" => { // [expr]
          needs_children(data, 1, children map jsonToDagularAST)
        }
        
        case "block_expr" => { // [assign1, assign2, ...]
          DagularNode(data, children map jsonToDagularAST)
        }
        
        case "if_expr" => { // [cond_expr, true_expr, false_expr]
          needs_children(data, 3, children map jsonToDagularAST)
        }
        
        case "unop" => { // [op, operand]
          if (children.length != 2)
            throw new IllegalArgumentException (s"dagular parse found ${children.length} children in ${data}: expected 3")
          else {
            val op = DagularLeaf(children(0)) // for some reason lark produces a plain string here
            val operand = jsonToDagularAST(children(1))
            DagularNode(data, Vector(op, operand))
          }
        }
        
        case "binop" => { // [operand1, op, operand2]
          if (children.length != 3)
            throw new IllegalArgumentException (s"dagular parse found ${children.length} children in ${data}: expected 3")
          else {
            val operand1 = jsonToDagularAST(children(0))
            val op = DagularLeaf(children(1)) // for some reason lark produces a plain string here
            val operand2 = jsonToDagularAST(children(2))
            DagularNode(data, Vector(operand1, op, operand2))
          }
        }
        
        case "index" => { // [variable name, index]
          if (children.length != 2)
            throw new IllegalArgumentException (s"dagular parse found ${children.length} children in ${data}: expected 2")
          else {
            val var_name = DagularLeaf(children(0))
            val index = jsonToDagularAST(children(1))
            DagularNode(data, Vector(var_name, index))
          }
        }
        
        case "invocation" => { // [function name, argument]
          // it would be convenient to resolve functions at this location
          if (children.length != 3)
            throw new IllegalArgumentException (s"dagular parse found ${children.length} children in ${data}: expected 3")
          else {
            val func_name = DagularLeaf(children(0))
            val argument = jsonToDagularAST(children(1))
            DagularNode(data, Vector(func_name, argument))
          }
        }
        
        case "comprehension" => { // [body_expr, id, list_expr]
          needs_children(data, 3, children map jsonToDagularAST)
        }
        
        case s => {
          throw new IllegalArgumentException (s"dagular parse found unrecognized node name $s in AST node")
        }
      }
    }
    
    private def mapAtom2(v1 : Future[DagularValue], v2 : Future[DagularValue], f : (DagularValue, DagularValue) => DagularValue) : Future[DagularValue] = {
      for {
        left <- v1
        right <- v2
      } yield {
        f (left, right)
      }
    }
    
    // interpret some dagular code
    private def interpretDagular(
        prog : DagularAST,
        env : Map[String, Future[DagularValue]]) : Future[DagularValue] = {
      
      prog match {
        case DagularLeaf(v) => {
          Future { DagularAtom(v) }
        }
        case DagularNode(data, children) => {
          data match {
            case "id" => { // [string]
              val DagularLeaf(JsString(s)) = children(0)
              if (s == "true")
                Future { DagularAtom(JsBoolean(true)) }
              else if (s == "false")
                Future { DagularAtom(JsBoolean(false)) }
              else
                env(s)
            }
            
            case "number" => { // [number]
              val DagularLeaf(JsNumber(n)) = children(0)
              Future { DagularAtom(JsNumber(n)) }
            }
            
            case "string" => { // [string]
              val DagularLeaf(JsString(s)) = children(0)
              Future { DagularAtom(JsString(s)) }
            }
            
            case "list" => { // [expr1, expr2, ...]
              Future { DagularArray(children map { child => interpretDagular(child, env) }) }
            }
            
            case "dict" => { // [pair1, pair2, ...]
              val pairs = children.map({ child =>
                // get pair contents
                val DagularNode(data, pair) = child
                
                // make sure it really is a pair
                if (data != "pair") {
                  throw new IllegalArgumentException (s"dagular interpret found non-pair in dict expr")
                }
                
                // disassemble the pair
                val DagularNode(_, idnode) = pair(0)
                val DagularLeaf(JsString(id)) = idnode(0) // string
                val value = interpretDagular(idnode(1), env) // expr
                
                // get the pair as (string, future)
                (id, value)
              })
              
              Future { DagularObject(pairs.toMap) }
            }
            
            case "unop" => { // [op, operand]
              val DagularLeaf(JsString(op)) = children(0)
              val operand = interpretDagular(children(1), env)
              
              op match {
                case "not" =>
                  operand map {
                    _ match {
                      case DagularAtom(JsBoolean(v)) => DagularAtom(JsBoolean(! v))
                      
                      case _ => throw new IllegalArgumentException(s"dagular interpret got unexpected dagular non-atom in unop $op")
                    }
                  }
                  
                case "-" =>
                  operand map {
                    _ match {
                      case DagularAtom(JsNumber(v)) => DagularAtom(JsNumber(- v))
                      
                      case _ => throw new IllegalArgumentException(s"dagular interpret got unexpected dagular non-atom in unop $op")
                    }
                  }
                
                case s => {
                  throw new IllegalArgumentException (s"dagular interpret found unrecognized unop $s")
                }
              }
            }
            
            case "binop" => { // [operand1, op, operand2]
              val left = interpretDagular(children(0), env)
              val right = interpretDagular(children(2), env)
              val DagularLeaf(JsString(op)) = children(1)
              
              op match {
                case "or" => {
                  mapAtom2(left, right, {
                    case (DagularAtom(JsBoolean(left)), DagularAtom(JsBoolean(right))) =>
                      DagularAtom(JsBoolean(left || right))
                    
                    case _ =>
                      throw new IllegalArgumentException(s"dagular interpret got unexpected dagular non-atom in binop $op")
                  })
                }
                
                case "and" => {
                  mapAtom2(left, right, {
                    case (DagularAtom(JsBoolean(left)), DagularAtom(JsBoolean(right))) =>
                      DagularAtom(JsBoolean(left && right))
                    
                    case _ =>
                      throw new IllegalArgumentException(s"dagular interpret got unexpected dagular non-atom in binop $op")
                  })
                }
                
                case "<" => {
                  mapAtom2(left, right, {
                    case (DagularAtom(JsNumber(left)), DagularAtom(JsNumber(right))) =>
                      DagularAtom(JsBoolean(left < right))
                    
                    case _ =>
                      throw new IllegalArgumentException(s"dagular interpret got unexpected dagular non-atom in binop $op")
                  })
                }
                
                case ">" => {
                  mapAtom2(left, right, {
                    case (DagularAtom(JsNumber(left)), DagularAtom(JsNumber(right))) =>
                      DagularAtom(JsBoolean(left > right))
                    
                    case _ =>
                      throw new IllegalArgumentException(s"dagular interpret got unexpected dagular non-atom in binop $op")
                  })
                }
                
                case ">=" => {
                  mapAtom2(left, right, {
                    case (DagularAtom(JsNumber(left)), DagularAtom(JsNumber(right))) =>
                      DagularAtom(JsBoolean(left >= right))
                    
                    case _ =>
                      throw new IllegalArgumentException(s"dagular interpret got unexpected dagular non-atom in binop $op")
                  })
                }
                
                case "<=" => {
                  mapAtom2(left, right, {
                    case (DagularAtom(JsNumber(left)), DagularAtom(JsNumber(right))) =>
                      DagularAtom(JsBoolean(left <= right))
                    
                    case _ =>
                      throw new IllegalArgumentException(s"dagular interpret got unexpected dagular non-atom in binop $op")
                  })
                }
                
                case "==" => {
                  for {
                    left_val <- left
                    right_val <- right
                  } yield {
                    DagularAtom(JsBoolean(left_val.toJsValue() == right_val.toJsValue()))
                  }
                }
                
                case "!=" => {
                  for {
                    left_val <- left
                    right_val <- right
                  } yield {
                    DagularAtom(JsBoolean(left_val.toJsValue() != right_val.toJsValue()))
                  }
                }
                
                case "+" => {
                  mapAtom2(left, right, {
                    case (DagularAtom(JsNumber(left)), DagularAtom(JsNumber(right))) =>
                      DagularAtom(JsNumber(left + right))
                    
                    case _ =>
                      throw new IllegalArgumentException(s"dagular interpret got unexpected dagular non-atom in binop $op")
                  })
                }
                
                case "-" => {
                  mapAtom2(left, right, {
                    case (DagularAtom(JsNumber(left)), DagularAtom(JsNumber(right))) =>
                      DagularAtom(JsNumber(left - right))
                    
                    case _ =>
                      throw new IllegalArgumentException(s"dagular interpret got unexpected dagular non-atom in binop $op")
                  })
                }
                
                case "*" => {
                  mapAtom2(left, right, {
                    case (DagularAtom(JsNumber(left)), DagularAtom(JsNumber(right))) =>
                      DagularAtom(JsNumber(left * right))
                    
                    case _ =>
                      throw new IllegalArgumentException(s"dagular interpret got unexpected dagular non-atom in binop $op")
                  })
                }
                
                case "/" => {
                  mapAtom2(left, right, {
                    case (DagularAtom(JsNumber(left)), DagularAtom(JsNumber(right))) =>
                      DagularAtom(JsNumber(left / right))
                    
                    case _ =>
                      throw new IllegalArgumentException(s"dagular interpret got unexpected dagular non-atom in binop $op")
                  })
                }
                
                case "%" => {
                  mapAtom2(left, right, {
                    case (DagularAtom(JsNumber(left)), DagularAtom(JsNumber(right))) =>
                      DagularAtom(JsNumber(left % right))
                    
                    case _ =>
                      throw new IllegalArgumentException(s"dagular interpret got unexpected dagular non-atom in binop $op")
                  })
                }
                
                case s => {
                  throw new IllegalArgumentException (s"dagular interpret found unrecognized binop $s")
                }
              }
            }
            
            case "index" => { // [var name, index]
              interpretDagular(children(0), env) flatMap {
                case DagularAtom(JsArray(arr)) =>
                  interpretDagular(children(1), env) map {
                    case DagularAtom(JsNumber(idx)) => DagularAtom(arr(idx.toIntExact))
                    
                    case _ =>
                      throw new IllegalArgumentException (s"dagular interpret attempted to index array with non-number")
                  }
                
                case DagularAtom(JsObject(obj)) =>
                  interpretDagular(children(1), env) map {
                    case DagularAtom(JsString(idx)) => DagularAtom(obj(idx))
                    
                    case _ =>
                      throw new IllegalArgumentException (s"dagular interpret attempted to index object with non-string")
                  }
                  
                case DagularAtom(_) =>
                  throw new IllegalArgumentException (s"dagular interpret found atomic as indexing argument")
                  
                case DagularArray(arr) =>
                  interpretDagular(children(1), env) flatMap {
                    case DagularAtom(JsNumber(idx)) => arr(idx.toIntExact)
                    
                    case _ =>
                      throw new IllegalArgumentException (s"dagular interpret attempted to index array with non-number")
                  }
                  
                case DagularObject(obj) =>
                  interpretDagular(children(1), env) flatMap {
                    case DagularAtom(JsString(idx)) => obj(idx)
                    
                    case _ =>
                      throw new IllegalArgumentException (s"dagular interpret attempted to index object with non-string")
                  }
              }
            }
            
            case "invocation" => { // [function name, argument]
              val DagularLeaf(JsString(func_name)) = children(0)
              val arg = interpretDagular(children(1), env)
              
              arg flatMap {
                case DagularAtom(JsObject(arg)) =>
                  interpretInvocation(func_name, JsObject(arg)) map { DagularAtom(_) }
                  
                case DagularAtom(_) =>
                  throw new IllegalArgumentException (s"dagular interpret found non-object as function argument")
                  
                case DagularArray(_) =>
                  throw new IllegalArgumentException (s"dagular interpret found array as function argument")
                  
                case DagularObject(arg) =>
                  DagularObject(arg).toJsValue() flatMap { arg => interpretInvocation(func_name, arg.asJsObject) map {DagularAtom(_)}}
              }
            }
            
            case "comprehension" => { // [body_expr, id, list_expr]
              val DagularLeaf(JsString(id)) = children(1)
              interpretDagular(children(2), env) map {
                case DagularAtom(JsArray(arr)) =>
                  DagularArray(arr.map({ item => interpretDagular(children(0), env + (id -> Future { DagularAtom(item) })) }))
                  
                case DagularAtom(_) =>
                  throw new IllegalArgumentException (s"dagular interpret found non-array as comprehension argument")
                  
                case DagularArray(arr) =>
                  DagularArray(arr.map({ item => interpretDagular(children(0), env + (id -> item)) }))
                  
                case DagularObject(_) =>
                  throw new IllegalArgumentException (s"dagular interpret found object as comprehension argument")
              }
            }
               
            case "if_expr" => { // [cond_expr, true_expr, false_expr]
              val cond = children(0)
              val true_branch = children(1)
              val false_branch = children(2)
              
              interpretDagular(cond, env) flatMap {
                // wait for the result before executing subDAG
                case DagularAtom(cond) => {
                  val JsBoolean(cond_bool) = cond
                  if (cond_bool)
                    interpretDagular(true_branch, env)
                  else
                    interpretDagular(false_branch, env)
                }
                
                case DagularArray(_) => {
                  throw new IllegalArgumentException(s"dagular interpret got array instead of boolean as if_expr condition")
                }
                case DagularObject(_) => {
                  throw new IllegalArgumentException(s"dagular interpret got object instead of boolean as if_expr condition")
                }
              }
            }
            
            case "block_expr" => { // [assign1, assign2, ..., return]
              // interpret series of assignments
              
              val new_env = children.dropRight(1).foldRight(env)({(node, env) => node match {
                case DagularNode("assign", assign_children) => // [leaf, expr]
                  val DagularLeaf(JsString(id)) = assign_children(0)
                  env + (id -> interpretDagular(assign_children(1), env))
                  
                case DagularNode(s, _) =>
                  throw new IllegalArgumentException (s"dagular interpret found $s inside block")
              }})
              
              children.takeRight(1)(0) match {
                case DagularNode("return", ret_children) => // [expr]
                  interpretDagular(ret_children(0), new_env)
                  
                case DagularNode(s, _) =>
                  throw new IllegalArgumentException (s"dagular interpret found non-return $s at block end")
              }
            }
            
            case "return" | "assign" => {
              throw new IllegalArgumentException (s"dagular interpret found $data in non-block")
            }
            
            case s => {
              throw new IllegalArgumentException (s"dagular interpret found unrecognized node name $s")
            }
          }
        }
      }
    }
    
    // call a serverless function and coerce result as JsObject
    private def interpretInvocation(name : String, payload : JsObject) : Future[JsObject] = {
      logging.info(this, s"dagular is invoking an action called $name")

      // this is what's done to resolve an action name
      val fqn = FullyQualifiedEntityName.serdes.read(JsString(name))
      val fqn_default_namespace = FullyQualifiedEntityName(fqn.path.resolveNamespace(user.namespace), fqn.name)
      val action_future = WhiskActionMetaData.resolveActionAndMergeParameters(entityStore, fqn_default_namespace)
      
      // now that we have obtained the future for the action, we can run it
      action_future flatMap { action =>
        logging.info(this, s"dagular got a future for action called $name")
      
        val timeout = action.limits.timeout.duration + 1.minute
        val future_whisk_activation = invokeAction(user, action, Some(payload), waitForResponse = Some(timeout), cause)
        
        // see what the result is
        future_whisk_activation flatMap {
          case Right(activation) => {
            // success?
            val output = activation.response.result.map(_.asJsObject).getOrElse(JsObject.empty)
            val error = output.fields.get(ActivationResponse.ERROR_FIELD)
            
            if (error.isEmpty) {
              // dump the output value
              val JsObject(map) = output
              Future.successful(map.getOrElse("output", JsObject.empty).asJsObject)
            } else {
              // throw an exception and let it ride
              logging.error(this, s"dagular activation ${activation.activationId} encountered error")
              Future.failed(new IllegalArgumentException(s"dagular activation ${activation.activationId} encountered error"))
            }
          }

          case Left(activationId) => {
            // let an exception ride
            logging.error(this, s"dagular activation $activationId timed out")
            Future.failed(new IllegalArgumentException(s"dagular activation $activationId timed out"))
          }
        }
      }
    }
  }
}