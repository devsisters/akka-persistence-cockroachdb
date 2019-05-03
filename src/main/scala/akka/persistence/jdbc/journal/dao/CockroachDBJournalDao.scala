package akka.persistence.jdbc.journal.dao

import java.sql.SQLException

import scala.util.{ Success, Failure }

import akka.NotUsed
import akka.persistence.jdbc.JournalRow
import akka.persistence.jdbc.config.JournalConfig
import akka.persistence.jdbc.serialization.FlowPersistentReprSerializer
import akka.persistence.{ AtomicWrite, PersistentRepr }
import akka.serialization.Serialization
import akka.stream.{ Materializer, OverflowStrategy, QueueOfferResult }
import akka.stream.scaladsl.{ Keep, Sink, Source }
import slick.jdbc.JdbcProfile
import slick.jdbc.JdbcBackend._

import scala.collection.immutable
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.Try

class CockroachDBJournalDao(val db: Database, val profile: JdbcProfile, val journalConfig: JournalConfig, serialization: Serialization)(implicit val ec: ExecutionContext, val mat: Materializer) extends JournalDao {
  import journalConfig.daoConfig.{ batchSize, bufferSize, parallelism }
  import profile.api._

  val queries: JournalQueries = new JournalQueries(profile, journalConfig.journalTableConfiguration)
  val serializer: FlowPersistentReprSerializer[JournalRow] = new ByteArrayJournalSerializer(serialization, journalConfig.pluginConfig.tagSeparator)

  def retryUntilSuccessful[R](savepoint: java.sql.Savepoint, query: DBIOAction[R, NoStream, Effect.All]): DBIOAction[R, NoStream, Effect.All] = {
    val action = for {
      result <- query
      _ <- SimpleDBIO(_.connection.releaseSavepoint(savepoint))
    } yield result

    action.asTry
      .flatMap {
        case Success(value) =>
          DBIO.successful(value)
        case Failure(exc: SQLException) =>
          if (exc.getSQLState == "40001")
            for {
              _ <- SimpleDBIO(_.connection.rollback(savepoint))
              r <- retryUntilSuccessful(savepoint, query)
            } yield r
          else
            DBIO.failed(exc)
        case Failure(exc) =>
          DBIO.failed(exc)
      }
  }

  def retryableAction[R](query: DBIOAction[R, NoStream, Effect.All]): DBIOAction[R, NoStream, Effect.All] = {
    (for {
      _ <- SimpleDBIO(_.connection.setAutoCommit(false))
      savepoint <- SimpleDBIO(_.connection.setSavepoint("cockroach_restart"))
      result <- retryUntilSuccessful(savepoint, query)
      _ <- SimpleDBIO(_.connection.commit())
      _ <- SimpleDBIO(_.connection.setAutoCommit(true))
    } yield result).withPinnedSession
  }

  private val writeQueue =
    Source.queue[(Promise[Unit], Seq[JournalRow])](bufferSize, OverflowStrategy.dropNew)
      .batchWeighted[(Seq[Promise[Unit]], Seq[JournalRow])](batchSize, _._2.size, tup => Vector(tup._1) -> tup._2) {
        case ((promises, rows), (newPromise, newRows)) => (promises :+ newPromise) -> (rows ++ newRows)
      }
      .mapAsync(parallelism) {
        case (promises, rows) =>
          writeJournalRows(rows)
            .map(unit => promises.foreach(_.success(unit)))
            .recover { case t => promises.foreach(_.failure(t)) }
      }
      .toMat(Sink.ignore)(Keep.left)
      .run()

  private def queueWriteJournalRows(xs: Seq[JournalRow]): Future[Unit] = {
    val promise = Promise[Unit]()

    writeQueue.offer(promise -> xs).flatMap {
      case QueueOfferResult.Enqueued =>
        promise.future
      case QueueOfferResult.Failure(t) =>
        Future.failed(new Exception("Failed to write journal row batch", t))
      case QueueOfferResult.Dropped =>
        Future.failed(new Exception(s"Failed to enqueue journal row batch write, the queue buffer was full ($bufferSize elements) please check the jdbc-journal.bufferSize setting"))
      case QueueOfferResult.QueueClosed =>
        Future.failed(new Exception("Failed to enqueue journal row batch write, the queue was closed"))
    }
  }

  def writeJournalRows(xs: Seq[JournalRow]): Future[Unit] =
    db.run(retryableAction(queries.writeJournalRows(xs))).map(_ => ())

  override def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
    val serializedTries = serializer.serialize(messages)

    // If serialization fails for some AtomicWrites, the other AtomicWrites may still be written
    val rowsToWrite = for {
      serializeTry <- serializedTries
      row <- serializeTry.getOrElse(Seq.empty)
    } yield row

    def resultWhenWriteComplete =
      if (serializedTries.forall(_.isSuccess)) Nil else serializedTries.map(_.map(_ => ()))

    queueWriteJournalRows(rowsToWrite).map(_ => resultWhenWriteComplete)
  }

  override def delete(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    val actions = for {
      _ <- queries.markJournalMessagesAsDeleted(persistenceId, toSequenceNr)
      highestMarkedSequenceNr <- highestMarkedSequenceNr(persistenceId)
      _ <- queries.delete(persistenceId, highestMarkedSequenceNr.getOrElse(0L) - 1)
    } yield ()

    db.run(retryableAction(actions))
  }

  private def highestMarkedSequenceNr(persistenceId: String) =
    queries.highestMarkedSequenceNrForPersistenceId(persistenceId).result.headOption

  override def highestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    for {
      maybeHighestSeqNo <- db.run(retryableAction(queries.highestSequenceNrForPersistenceId(persistenceId).result.headOption))
    } yield maybeHighestSeqNo.getOrElse(0L)

  override def messages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long): Source[Try[PersistentRepr], NotUsed] =
    Source.fromPublisher(db.stream(queries.messagesQuery(persistenceId, fromSequenceNr, toSequenceNr, max).result))
      .via(serializer.deserializeFlowWithoutTags)
}
