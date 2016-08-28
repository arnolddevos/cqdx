package qeduce
package cql

import transducers.{Reducer, count}
import flowlib._
import Process._

trait CQLActions { this: Qeduce with CQLTypes =>

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
      val statement = session.prepare(q.parts.mkString("?")).bind()

      for((p, i) <- q.params.zipWithIndex)
        p.sqlType.inject(statement, i, p.value)

      val result = session.execute(statement)

      var s = f.init
      while(! f.isReduced(s) && ! result.isExhausted)
        s = f(s, result.one())
      stop(f.complete(s))
  }

  def consumeConnection[A](a: Action[A]): Action[A] = action {
    c => try { a.run(c) } finally { c.close }
  }
}
