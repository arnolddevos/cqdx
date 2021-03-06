package qeduce
package cql

import com.datastax.driver.core
import java.util.Date

trait CQLTypes { this: Qeduce =>

  type Statement = core.BoundStatement
  type Row = core.Row

  abstract class CQLType[A] extends QueryType[A] {
    def tryExtract =
      (rs, n) =>
        try { Some(extract(rs, n)) }
        catch { case _:IllegalArgumentException => None }
  }

  implicit object cqlInt extends CQLType[Int] {
    def extract = _ getInt _
    def inject = _.setInt(_, _)
    def display = _.toString
  }

  implicit object cqlLong extends CQLType[Long] {
    def extract = _ getLong _
    def inject = _.setLong(_, _)
    def display = _.toString
  }

  implicit object cqlTimestamp extends CQLType[Date] {
    def extract = _ getTimestamp _
    def inject = _.setTimestamp(_, _)
    def display = _.toString
  }

  implicit object cqlDouble extends CQLType[Double] {
    def extract = _ getDouble _
    def inject = _.setDouble(_, _)
    def display = _.toString
  }

  implicit object cqlBoolean extends CQLType[Boolean] {
    def extract = _ getBool _
    def inject = _.setBool(_, _)
    def display = _.toString
  }

  implicit object cqlString extends CQLType[String] {
    def extract = _ getString _
    def inject = _.setString(_, _)
    def display = "\"" + _ + "\""
  }

  implicit def cqlNullable[A]( implicit u: QueryType[A]): QueryType[Option[A]] =
    new CQLType[Option[A]] {
      def extract = {
        (rs, name) =>
          if(rs.isNull(name)) None
          else Some(u.extract(rs, name))
      }
      def inject = {
        (st, ix, as) =>
          if(as.isDefined) u.inject(st, ix, as.get)
          else st.setToNull(ix)
      }
      def display =
        as =>
          if(as.isDefined) "Some(" + u.display(as.get) + ")"
          else "None"
    }
}
