package qeduce
package cql

import transducers.{Reducer, count}
import flowlib._
import Process._

import com.datastax.driver.core
import scala.collection.mutable.{Map => MutMap}

import com.google.common.util.concurrent.ListenableFuture
import java.util.concurrent.Executor

trait CQLActions { this: Qeduce with CQLTypes =>

  class Session( val inner: core.Session) {
    val stmts = MutMap.empty[String, core.PreparedStatement]
    def executeAsync(s: Statement) = futureStep(inner.executeAsync(s))
    def close() = inner.close()
    def prepareAsync(src: String): Process[core.PreparedStatement] = {
      if(stmts contains src) stmts(src)
      else {
        val ps = inner.prepare(src)
        stmts(src) = ps
        ps
      }
      ???
    }
  }

  type Context[+A] = Process[A]

  implicit class CQLContext( sc: StringContext) {
    def cql( ps: QueryValue* ): Query = new Query {
      val parts = sc.parts
      val params = ps
    }
  }

  def action[A](f: Session => Process[A]): Action[A] = new Action[A] {
    def run(implicit s: Session): Process[A] = f(s)
    def flatMap[B](f: A => Action[B]): Action[B] = action { implicit s => run >>= (f(_).run) }
    def map[B](f: A => B): Action[B] = action { implicit s => run.map(f) }
    def zip[B]( other: Action[B]):Action[(A, B)] = flatMap( a => other.map((a, _)))
  }

  def action(q: Query): Action[Int] = action(q, count)

  def action[S](q: Query, f: Reducer[Row, S]): Action[S] = action {
    session =>
      session.prepareAsync(q.parts.mkString("?")) >>= {
        statement =>

          val prepared = statement.bind()
          for((p, i) <- q.params.zipWithIndex)
            p.sqlType.inject(prepared, i, p.value)

          session.executeAsync(prepared) >>= {
            result =>
              var s = f.init
              while(! f.isReduced(s) && ! result.isExhausted)
                s = f(s, result.one())
              stop(f.complete(s))
          }
      }
  }

  def consumeConnection[A](a: Action[A]): Action[A] = action {
    c => try { a.run(c) } finally { c.close }
  }

  object directExecutor extends Executor {
    def execute(r: Runnable) = r.run()
  }

  def futureStep[T]( step: => ListenableFuture[T]): Process[T] = waitFor {
    k =>
      val f = step
      val r = new Runnable {
        def run = k(f.get())
      }
      f.addListener(r, directExecutor)
  }
}
