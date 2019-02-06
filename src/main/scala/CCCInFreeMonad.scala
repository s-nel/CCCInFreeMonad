import cats.effect.{IO, Sync}
import cats.free.Free
import cats.implicits._
import cats.{Monad, ~>}
import freestyle.free._

object CCCInFreeMonad {
  @free trait Stuff {
    def doStuff1: FS[Unit]
    def doStuff2: FS[Unit]
    def doExceptionalStuff: FS[Unit]
  }

  object RealStuff extends Stuff.Handler[IO] {
    override def doStuff1: IO[Unit] = IO(println("doing real stuff 1"))

    override def doStuff2: IO[Unit] = IO(println("doing real stuff 2"))

    override def doExceptionalStuff: IO[Unit] = IO.raiseError(new Exception())
  }

  // cross-cutting concern for `StuffAlgebra`
  class LoggingStuffInterpreter[F[_]](implInterpreter: (Stuff.Op ~> F))(
      implicit s: Sync[F],
      m: Monad[F])
      extends (Stuff.Op ~> F) {
    override def apply[A](fa: Stuff.Op[A]): F[A] =
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

  def main(args: Array[String]): Unit = {
    val stuff = Stuff[Stuff.Op]

    implicit val interpreter = new LoggingStuffInterpreter(
      new TransactionalInterpreter(RealStuff))

    val program = for {
      _ <- stuff.doStuff1
      _ <- stuff.doStuff2
      _ <- stuff.doExceptionalStuff
    } yield ()

    program.interpret[IO].attempt.unsafeRunSync()
  }
}
