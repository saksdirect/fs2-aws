package fs2.aws

import cats.effect.Async
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync
import fs2.Pipe
import org.scanamo.DynamoReadError.describe
import org.scanamo.{DynamoReadError, ScanamoCats, Table}


package object dynamodb {

  /**
    * Create an [[fs2.Stream]] for the `scan` result of the specified table
    * @param table The scanamo `Table[T]` object
    * @param pageSize The scan operation is paginated, default is 25
    * @param client The dynamodb client
    * @return A Stream of T items.  A [[RuntimeException]] will be raised if dynamo couldn't read an item.
    */
  def scanTable[F[_]: Async, T](table: Table[T], pageSize: Int = 25)(
    implicit client: AmazonDynamoDBAsync
  ): fs2.Stream[F, T] =
    scanTableMaybe(table, pageSize).through(raiseIfError)

  def scanTableMaybe[F[_]: Async, T](table: Table[T], pageSize: Int = 25)(
    implicit client: AmazonDynamoDBAsync
  ): fs2.Stream[F, Either[DynamoReadError, T]] = {

    type SF[A] = fs2.Stream[F, A]

    ScanamoCats[F](client)
      .execT(ScanamoCats.ToStream)(table.scanPaginatedM[SF](pageSize))
      .flatMap(fs2.Stream.emits)
  }

  def queryTable[F[_]: Async, T](
                                  table: Table[T],
                                  query: org.scanamo.query.Query[_],
                                  pageSize: Int = 25
                                )(
                                  implicit client: AmazonDynamoDBAsync
                                ): fs2.Stream[F, T] = queryTableMaybe(table, query).through(raiseIfError)

  def queryTableMaybe[F[_]: Async, T](
                                       table: Table[T],
                                       query: org.scanamo.query.Query[_],
                                       pageSize: Int = 25
                                     )(
                                       implicit client: AmazonDynamoDBAsync
                                     ): fs2.Stream[F, Either[DynamoReadError, T]] = {

    type SF[A] = fs2.Stream[F, A]

    ScanamoCats[F](client)
      .execT(ScanamoCats.ToStream)(table.queryPaginatedM[SF](query, pageSize))
      .flatMap(fs2.Stream.emits)
  }

  private[this] def raiseIfError[F[_]: Async, T]: Pipe[F, Either[DynamoReadError, T], T] =
    _.evalMap[F, T] {
      case Left(dre) => Async[F].raiseError(new RuntimeException(describe(dre)))
      case Right(t)  => Async[F].pure(t)
    }

}
