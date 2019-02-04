import cats.effect.{IO, Sync}
import cats.free.Free
import cats.implicits._
import cats.{Monad, ~>}

object CCCInFreeMonad {
  sealed trait StuffAlgebra[A] {
    val liftF: Free[StuffAlgebra, A] = Free.liftF(this)
  }
  case object DoStuff1 extends StuffAlgebra[Unit]
  case object DoStuff2 extends StuffAlgebra[Unit]
  case object DoExceptionalStuff extends StuffAlgebra[Unit]

  // cross-cutting concern for `StuffAlgebra`
  class LoggingStuffInterpreter[F[_]](implInterpreter: (StuffAlgebra ~> F))(
      implicit s: Sync[F],
      m: Monad[F])
      extends (StuffAlgebra ~> F) {
    override def apply[A](fa: StuffAlgebra[A]): F[A] =
      for {
        _ <- s.delay(println("logging enter"))
        result <- implInterpreter(fa)
        _ <- s.delay(println("logging exit"))
      } yield result
  }

  // cross-cutting concern for any algebra
  class TransactionalInterpreter[F[_], G[_]](implInterpreter: (F ~> G))(
      implicit s: Sync[G],
      m: Monad[G])
      extends (F ~> G) {
    override def apply[A](fa: F[A]): G[A] =
      for {
        _ <- s.delay(println("start TX"))
        resultE <- implInterpreter(fa).attempt
        result <- resultE match {
          case Right(result) => s.delay(println("commit TX")).map(_ => result)
          case Left(t) =>
            for {
              _ <- s.delay(println("rollback TX"))
              result <- s.raiseError[A](t)
            } yield result
        }
      } yield result
  }

  // separate concerns
  object RealStuffInterpreter extends (StuffAlgebra ~> IO) {
    override def apply[A](fa: StuffAlgebra[A]): IO[A] = fa match {
      case DoStuff1           => IO(println("doing real stuff 1"))
      case DoStuff2           => IO(println("doing real stuff 2"))
      case DoExceptionalStuff => IO.raiseError(new Exception())
    }
  }

  def main(args: Array[String]): Unit = {
    val interpreter = new LoggingStuffInterpreter(
      new TransactionalInterpreter(RealStuffInterpreter))

    val program = for {
      _ <- DoStuff1.liftF
      _ <- DoStuff2.liftF
      _ <- DoExceptionalStuff.liftF
    } yield ()

    program.foldMap(interpreter).attempt.unsafeRunSync()
  }
}
