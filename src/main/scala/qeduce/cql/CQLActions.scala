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
    private val stmts = new Cache(innerPrepare)
    private def innerPrepare(s: String) = futureStep(inner.prepareAsync(s))
    def prepare(s: String): Process[core.PreparedStatement] = stmts.request(s)
    def execute(s: Statement): Process[core.ResultSet] = futureStep(inner.executeAsync(s))
    def close(): Process[Unit] = futureStep(inner.closeAsync()) >> stop(())
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
      session.prepare(q.parts.mkString("?")) >>= {
        statement =>

          val prepared = statement.bind()
          for((p, i) <- q.params.zipWithIndex)
            p.sqlType.inject(prepared, i, p.value)

          session.execute(prepared) >>= {
            result =>
              var s = f.init
              while(! f.isReduced(s) && ! result.isExhausted)
                s = f(s, result.one())
              stop(f.complete(s))
          }
      }
  }

  def consumeConnection[A](aa: Action[A]): Action[A] = action {
    c => aa.run(c) >>= { a => c.close >> stop(a) }
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
