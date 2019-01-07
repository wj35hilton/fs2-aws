package fs2.aws

import java.io.InputStream

import fs2.{Stream, Pull, Chunk}
import fs2.aws.internal._
import cats.effect.{ContextShift, Effect}
import cats.implicits._
import software.amazon.awssdk.services.s3.model._
import software.amazon.awssdk.core.sync.RequestBody

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

package object s3 {
  type PartETag = String

  def readS3FileMultipart[F[_]](
      bucket: String,
      key: String,
      chunkSize: Int,
      s3Client: S3Client[F] = new S3Client[F] {})(implicit F: Effect[F]): fs2.Stream[F, Byte] = {
    def go(offset: Int)(implicit F: Effect[F]): fs2.Pull[F, Byte, Unit] =
      fs2.Pull
        .acquire[F, Either[Throwable, InputStream]](
          s3Client.getObjectContentOrError(
            GetObjectRequest
              .builder()
              .bucket(bucket)
              .key(key)
              .range(s"$offset-${offset + chunkSize}")
              .build())) {
          //todo: properly log the error
          case Left(e)  => F.delay(() => e.printStackTrace())
          case Right(s) => F.delay(s.close())
        }
        .flatMap {
          case Right(s3_is) =>
            Pull.eval(F.delay {
              val is: InputStream = s3_is
              val buf             = new Array[Byte](chunkSize)
              val len             = is.read(buf)
              if (len < 0) None else Some(Chunk.bytes(buf, 0, len))
            })
          case Left(_) => Pull.eval(F.delay(None))
        }
        .flatMap {
          case Some(o) => Pull.output(o) >> go(offset + o.size)
          case None    => Pull.done
        }

    go(0).stream

  }

  def readS3File[F[_]](bucket: String,
                       key: String,
                       blockingEC: ExecutionContext,
                       s3Client: S3Client[F] = new S3Client[F] {})(
      implicit F: Effect[F],
      cs: ContextShift[F]): Stream[F, Byte] = {
    val request = GetObjectRequest.builder().bucket(bucket).key(key).build()
    fs2.io.readInputStream(
      s3Client.getObjectStream(request),
      chunkSize = 8192,
      blockingExecutionContext = blockingEC,
      closeAfterUse = true
    )
  }

  def uploadS3FileMultipart[F[_]](
      bucket: String,
      key: String,
      objectMetadata: Map[String, String] = Map.empty,
      s3Client: S3Client[F] = new S3Client[F] {})(implicit F: Effect[F]): fs2.Sink[F, Byte] = {
    def uploadPart(uploadId: String): fs2.Pipe[F, (Chunk[Byte], Int), CompletedPart] =
      _.flatMap({
        case (c, i) =>
          Stream.eval(
            s3Client
              .uploadPart(
                UploadPartRequest
                  .builder()
                  .bucket(bucket)
                  .key(key)
                  .uploadId(uploadId)
                  .partNumber(i)
                  .contentLength(c.size)
                  .build(),
                RequestBody.fromBytes(c.toArray)
              )
              .flatMap(
                r =>
                  F.delay(
                    CompletedPart.builder().partNumber(i).eTag(r.eTag).build()
                )
              )
          )
      })

    def completeUpload(uploadId: String): fs2.Sink[F, List[CompletedPart]] =
      _.flatMap { parts =>
        val completedMultipartUpload: CompletedMultipartUpload =
          CompletedMultipartUpload.builder.parts(parts: _*).build()
        Stream.eval_(
          s3Client.completeMultipartUpload(
            CompleteMultipartUploadRequest.builder
              .bucket(bucket)
              .key(key)
              .uploadId(uploadId)
              .multipartUpload(completedMultipartUpload)
              .build
          )
        )
      }

    in =>
      {
        val imuF: F[CreateMultipartUploadResponse] = objectMetadata match {
          case map if map.toSeq == Nil =>
            s3Client.createMultipartUpload(
              CreateMultipartUploadRequest.builder().bucket(bucket).key(key).build())
          case map =>
            s3Client.createMultipartUpload(
              CreateMultipartUploadRequest
                .builder()
                .bucket(bucket)
                .key(key)
                .metadata(map.asJava)
                .build())
        }
        Stream
          .eval(imuF)
          .flatMap(
            m =>
              in.chunks
                .zip(Stream.iterate(1)(_ + 1))
                .through(uploadPart(m.uploadId))
                .fold[List[CompletedPart]](List())(_ :+ _)
                .to(completeUpload(m.uploadId)))
      }
  }

  def listFiles[F[_]](bucketName: String, s3Client: S3Client[F] = new S3Client[F] {})(
      implicit F: Effect[F]): Stream[F, S3Object] = {
    val req = ListObjectsV2Request.builder.bucket(bucketName).build
    Stream
      .eval(s3Client.listObjects(req))
      .flatMap(resp => Stream.fromIterator(resp.contents.iterator.asScala))
  }
}
