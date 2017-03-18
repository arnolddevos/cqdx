package qeduce
package cql

import transducers.api.{Reducer, count}
import flowlib._
import Process._
import Generators._

import com.datastax.driver.core
import scala.collection.mutable.{Map => MutMap}
import scala.util.control.NonFatal

import com.google.common.util.concurrent.ListenableFuture
import java.util.concurrent.Executor

trait CQLActions { this: Qeduce with CQLTypes =>

  implicit class CQLContext( sc: StringContext) {
    def cql( ps: QueryValue* ): Query = new Query {
      val parts = sc.parts
      val params = ps
    }
  }

  class Session( val inner: core.Session) {
    private val stmts = new Cache(innerPrepare)
    private def innerPrepare(s: String) = futureStep(inner.prepareAsync(s))
    def prepare(s: String): Process[core.PreparedStatement] = stmts.request(s)
    def execute(s: Statement): Process[core.ResultSet] = futureStep(inner.executeAsync(s))
    def close(): Process[Unit] = futureStep(inner.closeAsync()) >> stop(())
  }

  private def bindAndExecute(session: Session, q: Query): Process[core.ResultSet] = {
    session.prepare(q.parts.mkString("?")) >>= {
      statement =>

        val prepared = statement.bind()
        for((p, i) <- q.params.zipWithIndex)
          p.sqlType.inject(prepared, i, p.value)

        session.execute(prepared)
    }
  }

  type Context[+A] = Process[A]

  def action[A](f: Session => Process[A]): Action[A] = new Action[A] {
    def run(implicit s: Session): Process[A] = f(s)
    def flatMap[B](f: A => Action[B]): Action[B] = action { implicit s => run >>= (f(_).run) }
    def map[B](f: A => B): Action[B] = action { implicit s => run.map(f) }
    def zip[B]( other: Action[B]):Action[(A, B)] = flatMap( a => other.map((a, _)))
  }

  def action(q: Query): Action[Int] = action(q, count)

  def action[S](query: Query, f: Reducer[Row, S]): Action[S] = action {
    session =>
      bindAndExecute(session, query) >>= {
        result =>
          import result._
          import f._

          @annotation.tailrec
          def exhaustPage(s: State): State =
            if(isReduced(s) || getAvailableWithoutFetching == 0) s
            else exhaustPage(f(s, one()))

          def loop(s0: f.State): Process[S] = {
            val s = exhaustPage(s0)
            if(isReduced(s) || isExhausted) stop(complete(s))
            else futureStep(fetchMoreResults) >> loop(s)
          }

          loop(init)
      }
  }

  def batched[S](query: Query, f: Reducer[Row, S]): Action[Series[S]] = action {
    session =>
      bindAndExecute(session, query) >>= {
        result =>
          import result._
          import f._

          @annotation.tailrec
          def batch(s: State): S =
            if(isReduced(s) || getAvailableWithoutFetching == 0) complete(s)
            else batch(f(s, one()))

          def batchAndPrefetch: S = {
            val s = batch(init)
            if(getAvailableWithoutFetching == 0 && ! isExhausted)
              fetchMoreResults
            s
          }

          def loop: Producer[S] = {
            if(isExhausted) Producer()
            else if(getAvailableWithoutFetching > 0) Producer(batchAndPrefetch, loop)
            else futureStep(fetchMoreResults) >> loop
          }

          loop
      }
  }

  object directExecutor extends Executor {
    def execute(r: Runnable) = r.run()
  }

  def futureStep[T]( step: => ListenableFuture[T]): Process[T] =
    waitFor[Process[T]] {
      k =>
        val f = step
        val r = new Runnable {
          def run = k {
            try { stop(f.get) }
            catch { case NonFatal(e) => fail("cql error", e) }
          }
        }
        f.addListener(r, directExecutor)
    } >>= identity
}
