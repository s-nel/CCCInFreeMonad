package uow

import cats.data.StateT
import cats.implicits._
import shapeless._
import shapeless.Nat._
import shapeless.ops.hlist._
import uow.UnitOfWorkPoc.Domain.MultiEntityExample
import uow.UnitOfWorkPoc.{
  Bar,
  DefaultUnitOfWorkExecutor,
  Foo,
  Repository,
  UnitOfWorkExecutor,
  ZookeeperBackingStore,
  ZookeeperRepository
}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect.runtime.universe._

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
  trait UnitOfWork[BS <: AtomicBackingStore, State] {
    def get[E <: Entity](id: E#Id)(implicit repository: Repository[BS, E],
                                   typ: TypeTag[E]): StateT[Future, State, Option[E]]
    def create[E <: Entity](e: E)(implicit repository: Repository[BS, E], typ: TypeTag[E]): StateT[Future, State, Unit]
    def update[E <: Entity](e: E)(implicit repository: Repository[BS, E], typ: TypeTag[E]): StateT[Future, State, Unit]
    def delete[E <: Entity](id: E#Id)(implicit repository: Repository[BS, E],
                                      typ: TypeTag[E]): StateT[Future, State, Unit]
  }

  trait BackingStore
  trait AtomicBackingStore extends BackingStore
  trait ZookeeperBackingStore extends AtomicBackingStore

  class EntityStateMapIS[K, V]
  implicit def entityToState[E <: Entity]: EntityStateMapIS[TypeTag[E], Map[E#Id, Option[E]]] =
    new EntityStateMapIS[TypeTag[E], Map[E#Id, Option[E]]]

  /**
    * State of the unit of work
    * @param ops   The operations that will be committed when the unit of work has finished
    * @param cache The read cache that is kept consistent with the operations performed in this unit of work
    */
  final case class UnitOfWorkState[BS <: BackingStore](ops: Seq[BatchOp[BS]], cache: HMap[EntityStateMapIS])

  class DefaultUnitOfWork[BS <: AtomicBackingStore] extends UnitOfWork[BS, UnitOfWorkState[BS]] {
    override def get[E <: Entity](id: E#Id)(implicit repo: Repository[BS, E],
                                            typ: TypeTag[E]): StateT[Future, UnitOfWorkState[BS], Option[E]] =
      StateT[Future, UnitOfWorkState[BS], Option[E]] { state =>
        val cache = state.cache.get(typ).getOrElse(Map.empty[E#Id, Option[E]])
        cache.get(id) match {
          case Some(Some(e)) => Future.successful((state, Some(e)))
          case Some(None)    => Future.successful((state, None))
          case None =>
            repo.get(id).map {
              case Some(e) =>
                val updatedCache = cache.updated(id, Some(e))
                val newState = state.copy(cache = state.cache + (typ, updatedCache))
                (newState, Some(e))
              case None =>
                val updatedCache = cache.updated(id, None)
                val newState = state.copy(cache = state.cache + (typ, updatedCache))
                (newState, None)
            }
        }

      }

    override def create[E <: Entity](e: E)(implicit repository: Repository[BS, E],
                                           typ: TypeTag[E]): StateT[Future, UnitOfWorkState[BS], Unit] =
      StateT[Future, UnitOfWorkState[BS], Unit] { state =>
        val cache = state.cache.get(typ).getOrElse(Map.empty[E#Id, Option[E]])
        cache.get(e.id) match {
          case Some(Some(e)) =>
            Future.failed(new Exception("Entity already exists"))
          case _ =>
            val id: E#Id = e.id
            val updatedCache = cache.updated(id, Some(e))
            val newState =
              state.copy(ops = state.ops ++ Seq(CreateOp[BS, E](e)), cache = state.cache + (typ, updatedCache))
            Future.successful((newState, ()))
        }
      }

    override def update[E <: Entity](e: E)(implicit repository: Repository[BS, E],
                                           typ: TypeTag[E]): StateT[Future, UnitOfWorkState[BS], Unit] =
      StateT[Future, UnitOfWorkState[BS], Unit] { state =>
        val cache = state.cache.get(typ).getOrElse(Map.empty[E#Id, Option[E]])
        val updatedCache = cache.updated(e.id, Some(e))
        val newState =
          state.copy(ops = state.ops ++ Seq(UpdateOp[BS, E](e)), cache = state.cache + (typ, updatedCache))
        Future.successful((newState, ()))
      }

    override def delete[E <: Entity](id: E#Id)(implicit repository: Repository[BS, E],
                                               typ: TypeTag[E]): StateT[Future, UnitOfWorkState[BS], Unit] = ???
  }

  trait UnitOfWorkExecutor[BS <: AtomicBackingStore] {
    def execute[T](f: UnitOfWork[BS, UnitOfWorkState[BS]] => StateT[Future, UnitOfWorkState[BS], T]): Future[T]
  }

  class DefaultUnitOfWorkExecutor[BS <: AtomicBackingStore, E <: Entity](implicit repo: Repository[BS, E])
      extends UnitOfWorkExecutor[BS] {
    override def execute[T](
        f: UnitOfWork[BS, UnitOfWorkState[BS]] => StateT[Future, UnitOfWorkState[BS], T]): Future[T] = {
      for {
        (finalState, result) <- f(new DefaultUnitOfWork[BS])
          .run(UnitOfWorkState(Seq.empty[BatchOp[BS]], HMap.empty[EntityStateMapIS]))
        _ <- repo.commit(finalState.ops)
      } yield result
    }
  }

  class ZookeeperRepository[E <: Entity] extends Repository[ZookeeperBackingStore, E] {
    override def get(id: E#Id): Future[Option[E]] = Future {
      println(s"get ${id}")
      None
    }

    override def create(e: E): Future[Unit] = ???

    override def update(e: E): Future[Unit] = ???

    override def delete(id: E#Id): Future[Unit] = ???

    override def commit(ops: Seq[BatchOp[ZookeeperBackingStore]]): Future[Unit] = Future {
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

  object Domain {

    /**
      * Example usage with multiple kinds of entities
      */
    class MultiEntityExample[BS <: AtomicBackingStore](implicit fooRepo: Repository[BS, Foo],
                                                       barRepo: Repository[BS, Bar],
                                                       executor: UnitOfWorkExecutor[BS]) {
      def multiTest: Future[Option[Foo]] = executor.execute { uow =>
        for {
          nonExistantFoo <- uow.get[Foo](1) // read from repo
          _ = assert(nonExistantFoo == None)
          nonExistantBar <- uow.get[Bar](1) // read from repo
          _ <- uow.create(Foo(1, "foo"))
          _ <- uow.create(Bar(1, "bar")) // create a different entity with same value id
          foo1 <- uow.get[Foo](1) // read insert from cache
          _ = assert(foo1 == Some(Foo(1, "foo")))
          bar1 <- uow.get[Bar](1) // read other insert from cache same value id
          _ = assert(bar1 == Some(Bar(1, "bar")))
        } yield foo1
      } // implicit commit
    }
  }
}

object App {
  import scala.concurrent.Await
  import scala.concurrent.duration._

  def main(args: Array[String]): Unit = {
    implicit val fooRepoInstance: Repository[ZookeeperBackingStore, Foo] =
      new ZookeeperRepository[Foo]

    implicit val barRepoInstance: Repository[ZookeeperBackingStore, Bar] = new ZookeeperRepository[Bar]

    implicit val unitOfWorkExecutorInstance: UnitOfWorkExecutor[ZookeeperBackingStore] =
      new DefaultUnitOfWorkExecutor[ZookeeperBackingStore, Foo]

    Await.result(new MultiEntityExample[ZookeeperBackingStore].multiTest, 1.second)
  }
}
