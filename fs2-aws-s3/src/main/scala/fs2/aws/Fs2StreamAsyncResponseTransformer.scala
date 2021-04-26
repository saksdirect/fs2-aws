package fs2.aws

import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture

import cats.effect.ConcurrentEffect
import fs2.{ Chunk, Stream }
import fs2.interop.reactivestreams.PublisherOps

import software.amazon.awssdk.core.async.{ AsyncResponseTransformer, SdkPublisher }

class Fs2StreamAsyncResponseTransformer[F[_]: ConcurrentEffect, ResponseT]
    extends AsyncResponseTransformer[ResponseT, (ResponseT, Stream[F, Byte])] {

  private val future              = new CompletableFuture[(ResponseT, Stream[F, Byte])]()
  private var response: ResponseT = _

  override def prepare(): CompletableFuture[(ResponseT, Stream[F, Byte])] = future

  override def onResponse(response: ResponseT): Unit = this.response = response

  override def onStream(publisher: SdkPublisher[ByteBuffer]): Unit = future.complete(
    (response, publisher.toStream[F].flatMap(bb => Stream.chunk(Chunk.byteBuffer(bb))))
  )

  override def exceptionOccurred(error: Throwable): Unit = future.completeExceptionally(error)
}

object Fs2StreamAsyncResponseTransformer {
  def apply[F[_]: ConcurrentEffect, ResponseT]: Fs2StreamAsyncResponseTransformer[F, ResponseT] =
    new Fs2StreamAsyncResponseTransformer[F, ResponseT]
}
