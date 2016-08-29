package qeduce
package cql

import flowlib._
import Process._
import Gate._

class Cache[S, T](f: S => Process[T]) {

  val latch = new KeyedLatch[S, T]
  val reservations = new Reservations[S]

  def request(s: S): Process[T] = {
    val cache = latch(s)
    waitFor(reservations.take(s)) >>= {
      reserved =>
        if(reserved) waitFor(cache.take)
        else
          f(s) >>= {
            t =>
              cache.signal(t)
              stop(t)
          }
    }
  }
}

class KeyedLatch[S, T] {
  private val state = Transactor(Map.empty[S, T])

  def apply(s: S): Latch[T] = new Latch[T] {
    def take( k: T => Unit): Unit =
      state.transact {
        case st if st contains s => st
      } {
        st => k(st(s))
      }

    def offer(t: T)(k: => Unit): Unit =
      state.transact {
        case st if st contains s => st
        case st => st.updated(s, t)
      } { _ => k }
  }
}

class Reservations[S] {
  private val state = Transactor(Set.empty[S])

  def take(s: S)( k: Boolean => Unit ) =
    state.transact {
      case ss => ss + s
    } {
      ss => k(ss contains s)
    }
}
