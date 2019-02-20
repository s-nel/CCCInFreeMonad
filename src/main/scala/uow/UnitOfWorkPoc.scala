package uow

import cats.data.StateT
import cats.implicits._
import shapeless._
import shapeless.Nat._
import shapeless.ops.hlist._
import uow.UnitOfWorkPoc.Domain.{MultiEntityExample, SingleEntityExample}
import uow.UnitOfWorkPoc.{DefaultUnitOfWorkExecutor, Instances, ZookeeperBackingStore}

import scala.concurrent.{ExecutionContext, Future}

object UnitOfWorkPoc {
  trait Entity {
    type Id
    val id: Id
  }

  sealed trait BatchOp[BS <: BackingStore]
  final case class CreateOp[BS <: BackingStore, E <: Entity](e: E) extends BatchOp[BS]
  final case class UpdateOp[BS <: BackingStore, E <: Entity](e: E) extends BatchOp[BS]
  final case class DeleteOp[BS <: BackingStore, E <: Entity](id: E#Id) extends BatchOp[BS]

  /**
    * Repository is not friends with UnitOfWork!!
    */
  trait Repository[BS <: BackingStore, E <: Entity] {
    def get(id: E#Id): Future[Option[E]]
    def create(e: E): Future[Unit]
    def update(e: E): Future[Unit]
    def delete(id: E#Id): Future[Unit]
    def commit(ops: Seq[BatchOp[BS]]): Future[Unit]
  }

  /**
    * Unit of work abstraction that works on a single entity type
    */
  trait UnitOfWork[E <: Entity, State] {
    def get(id: E#Id): StateT[Future, State, Option[E]]
    def create(e: E): StateT[Future, State, Unit]
    def update(e: E): StateT[Future, State, Unit]
    def delete(id: E#Id): StateT[Future, State, Unit]
  }

  trait BackingStore
  trait AtomicBackingStore extends BackingStore
  trait ZookeeperBackingStore extends AtomicBackingStore

  /**
    * State of the unit of work
    * @param ops   The operations that will be committed when the unit of work has finished
    * @param cache The read cache that is kept consistent with the operations performed in this unit of work
    */
  final case class UnitOfWorkState[BS <: BackingStore, E <: Entity](ops: Seq[BatchOp[BS]], cache: Map[E#Id, Option[E]])

  /**
    * Object that can execute units ofs works. Forgive my amateur shapeless
    */
  trait UnitOfWorkExecutor[BS <: AtomicBackingStore] {
    // single entity unit of work
    def execute[E1 <: Entity, T](
        f: UnitOfWork[E1, UnitOfWorkState[BS, E1] :: HNil] => StateT[Future, UnitOfWorkState[BS, E1] :: HNil, T])(
        implicit repo1: Repository[BS, E1]): Future[T]

    // two entity unit of work
    def execute[E1 <: Entity, E2 <: Entity, T](
        f: (UnitOfWork[E1, UnitOfWorkState[BS, E1] :: UnitOfWorkState[BS, E2] :: HNil],
            UnitOfWork[E2, UnitOfWorkState[BS, E1] :: UnitOfWorkState[BS, E2] :: HNil]) => StateT[
          Future,
          UnitOfWorkState[BS, E1] :: UnitOfWorkState[BS, E2] :: HNil,
          T])(implicit repo1: Repository[BS, E1],
              repo2: Repository[BS, E2],
              at0: At[UnitOfWorkState[BS, E1] :: UnitOfWorkState[BS, E2] :: HNil, Nat._0] {
                type Out = UnitOfWorkState[BS, E1]
              },
              at1: At[UnitOfWorkState[BS, E1] :: UnitOfWorkState[BS, E2] :: HNil, Nat._1] {
                type Out = UnitOfWorkState[BS, E2]
              },
              toList: ToList[Seq[BatchOp[BS]] :: Seq[BatchOp[BS]] :: HNil, Seq[BatchOp[BS]]]): Future[T]

    // we can define more functions that take more types of entities
  }

  class DefaultUnitOfWorkExecutor[BS <: AtomicBackingStore](implicit executionContext: ExecutionContext)
      extends UnitOfWorkExecutor[BS] {

    class DefaultUnitOfWork[E <: Entity, States <: HList](
        fromStates: States => UnitOfWorkState[BS, E],
        toStates: (States, UnitOfWorkState[BS, E]) => States)(implicit repo: Repository[BS, E])
        extends UnitOfWork[E, States] {
      override def get(id: E#Id): StateT[Future, States, Option[E]] =
        StateT[Future, States, Option[E]] { states =>
          val state = fromStates(states)
          (state.cache.get(id) match {
            case Some(Some(e)) => Future.successful((state, Some(e)))
            case Some(None)    => Future.successful((state, None))
            case None =>
              repo.get(id).map {
                case Some(e) =>
                  val newState: UnitOfWorkState[BS, E] =
                    state.copy[BS, E](cache = state.cache.updated(id, Some(e)))
                  (newState, Some(e))
                case None =>
                  val newState = state.copy[BS, E](cache = state.cache.updated(id, None))
                  (newState, None)
              }
          }).map {
            case (newState, v) => (toStates(states, newState), v)
          }
        }

      override def create(e: E): StateT[Future, States, Unit] =
        StateT[Future, States, Unit] { states =>
          val state = fromStates(states)
          (state.cache.get(e.id) match {
            case Some(Some(e)) =>
              Future.failed(new Exception("Entity already exists"))
            case _ =>
              val newState =
                state.copy(ops = state.ops ++ Seq(CreateOp[BS, E](e)), cache = state.cache.updated(e.id, Some(e)))
              Future.successful((newState, ()))
          }).map {
            case (newState, v) => (toStates(states, newState), v)
          }
        }

      override def update(e: E): StateT[Future, States, Unit] =
        StateT[Future, States, Unit] { states =>
          val state = fromStates(states)
          val newState =
            state.copy(ops = state.ops ++ Seq(UpdateOp[BS, E](e)), cache = state.cache.updated(e.id, Some(e)))
          Future.successful((newState, ())).map {
            case (newState, v) => (toStates(states, newState), v)
          }
        }

      override def delete(id: E#Id): StateT[Future, States, Unit] = ???
    }

    override def execute[E1 <: Entity, T](
        f: UnitOfWork[E1, UnitOfWorkState[BS, E1] :: HNil] => StateT[Future, UnitOfWorkState[BS, E1] :: HNil, T])(
        implicit repo1: Repository[BS, E1]): Future[T] = ???

    override def execute[E1 <: Entity, E2 <: Entity, T](
        f: (UnitOfWork[E1, UnitOfWorkState[BS, E1] :: UnitOfWorkState[BS, E2] :: HNil],
            UnitOfWork[E2, UnitOfWorkState[BS, E1] :: UnitOfWorkState[BS, E2] :: HNil]) => StateT[
          Future,
          UnitOfWorkState[BS, E1] :: UnitOfWorkState[BS, E2] :: HNil,
          T])(implicit repo1: Repository[BS, E1],
              repo2: Repository[BS, E2],
              at0: At[UnitOfWorkState[BS, E1] :: UnitOfWorkState[BS, E2] :: HNil, Nat._0] {
                type Out = UnitOfWorkState[BS, E1]
              },
              at1: At[UnitOfWorkState[BS, E1] :: UnitOfWorkState[BS, E2] :: HNil, Nat._1] {
                type Out = UnitOfWorkState[BS, E2]
              },
              toList: ToList[Seq[BatchOp[BS]] :: Seq[BatchOp[BS]] :: HNil, Seq[BatchOp[BS]]]): Future[T] = {
      type States = UnitOfWorkState[BS, E1] :: UnitOfWorkState[BS, E2] :: HNil
      val unitOfWork1: UnitOfWork[E1, States] = new DefaultUnitOfWork[E1, States](at0.apply, {
        (states: States, newState: UnitOfWorkState[BS, E1]) =>
          states.updatedAt(_0, newState)
      })
      val unitOfWork2: UnitOfWork[E2, States] = new DefaultUnitOfWork[E2, States](at1.apply, {
        (states: States, newState: UnitOfWorkState[BS, E2]) =>
          states.updatedAt(_1, newState)
      })

      object toOps extends Poly1 {
        implicit def unitOfWorkStateCase[E <: Entity]: Case.Aux[UnitOfWorkState[BS, E], Seq[BatchOp[BS]]] = at(_.ops)
      }

      for {
        (finalState, result) <- f(unitOfWork1, unitOfWork2).run(
          UnitOfWorkState[BS, E1](Seq.empty, Map.empty) :: UnitOfWorkState[BS, E2](Seq.empty, Map.empty) :: HNil)
        allOps = finalState.map(toOps).toList[Seq[BatchOp[BS]]].toSeq.flatten
        _ <- repo1.commit(allOps)
      } yield result
    }
  }

  class ZookeeperRepository[E <: Entity](implicit executionContext: ExecutionContext)
      extends Repository[ZookeeperBackingStore, E] {
    override def get(id: E#Id): Future[Option[E]] = Future {
      println(s"get ${id}")
      None
    }

    override def create(e: E): Future[Unit] = ???

    override def update(e: E): Future[Unit] = ???

    override def delete(id: E#Id): Future[Unit] = ???

    override def commit(ops: Seq[BatchOp[ZookeeperBackingStore]]): Future[Unit] =
      Future {
        println(s"commit ops ${ops}")
      }
  }

  //
  // Entities
  //
  final case class Foo(id: Int, v: String) extends Entity {
    type Id = Int
  }
  final case class Bar(id: Int, v: String) extends Entity {
    type Id = Int
  }

  object Instances {
    import scala.concurrent.ExecutionContext.Implicits.global

    implicit val fooRepoInstance: Repository[ZookeeperBackingStore, Foo] =
      new ZookeeperRepository[Foo]

    implicit val barRepoInstance: Repository[ZookeeperBackingStore, Bar] = new ZookeeperRepository[Bar]

    implicit val unitOfWorkExecutorInstance: UnitOfWorkExecutor[ZookeeperBackingStore] =
      new DefaultUnitOfWorkExecutor[ZookeeperBackingStore]
  }

  object Domain {
    import scala.concurrent.ExecutionContext.Implicits.global

    /**
      * Example usage with a single kind of entity
      */
    class SingleEntityExample[BackingStore <: AtomicBackingStore](unitOfWorkExecutor: UnitOfWorkExecutor[BackingStore])(
        implicit fooRepo: Repository[BackingStore, Foo]) {

      def singleTest: Future[Option[Foo]] = unitOfWorkExecutor.execute[Foo, Option[Foo]] { uow =>
        for {
          _ <- uow.create(Foo(1, "foo"))
          foo <- uow.get(1)
        } yield foo
      }
    }

    /**
      * Example usage with multiple kinds of entities
      */
    class MultiEntityExample[BackingStore <: AtomicBackingStore](implicit fooRepo: Repository[BackingStore, Foo],
                                                                 barRepo: Repository[BackingStore, Bar],
                                                                 unitOfWorkExecutor: UnitOfWorkExecutor[BackingStore]) {
      def multiTest: Future[Unit] = unitOfWorkExecutor.execute[Foo, Bar, Unit] { (foos, bars) =>
        for {
          nonExistantFoo <- foos.get(1)
          _ = assert(nonExistantFoo == None) // read from repo
          _ <- foos.create(Foo(1, "foo"))
          _ <- bars.create(Bar(1, "bar")) // create a different entity
          foo1 <- foos.get(1)
          _ = assert(foo1 == Some(Foo(1, "foo"))) // read insert from cache
        } yield ()
      }
    }
  }
}

object App {
  import Instances._
  import scala.concurrent.duration._
  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.Await

  def main(args: Array[String]): Unit = {
    Await.result(
      new MultiEntityExample[ZookeeperBackingStore].multiTest,
      1.second)

  }
}
