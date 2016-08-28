package qeduce
package cql

import transducers.{Reducer, count}

trait CQLActions { this: Qeduce with CQLTypes =>

  implicit class CQLContext( sc: StringContext) {
    def cql( ps: QueryValue* ): Query = new Query {
      val parts = sc.parts
      val params = ps
    }
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
      f.complete(s)
  }

  def consumeConnection[A](a: Action[A]): Action[A] = action {
    c => try { a.run(c) } finally { c.close }
  }
}
