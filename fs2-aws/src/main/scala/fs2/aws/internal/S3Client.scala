package fs2.aws.internal

import java.io.InputStream

import cats.effect.Effect
import software.amazon.awssdk.services.s3.model._
import software.amazon.awssdk.services.s3.{S3Client => AwsS3Client}
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable
import software.amazon.awssdk.core.sync.RequestBody

import scala.util.control.Exception

private[aws] trait S3Client[F[_]] {
  private lazy val client: AwsS3Client = AwsS3Client.create()

  def getObjectContentOrError(getObjectRequest: GetObjectRequest)(
      implicit F: Effect[F]): F[Either[Throwable, InputStream]] =
    F.delay(Exception.nonFatalCatch either client.getObject(getObjectRequest))

  def getObjectContent(getObjectRequest: GetObjectRequest)(implicit F: Effect[F]): F[InputStream] =
    F.delay(client.getObject(getObjectRequest))

  def createMultipartUpload(createMultipartUploadRequest: CreateMultipartUploadRequest)(
      implicit F: Effect[F]): F[CreateMultipartUploadResponse] =
    F.delay(client.createMultipartUpload(createMultipartUploadRequest))

  def uploadPart(uploadPartRequest: UploadPartRequest, data: RequestBody)(
      implicit F: Effect[F]): F[UploadPartResponse] =
    F.delay(client.uploadPart(uploadPartRequest, data))

  def completeMultipartUpload(completeMultipartUploadRequest: CompleteMultipartUploadRequest)(
      implicit F: Effect[F]): F[CompleteMultipartUploadResponse] =
    F.delay(client.completeMultipartUpload(completeMultipartUploadRequest))

  def getObjectStream(request: GetObjectRequest)(implicit F: Effect[F]): F[InputStream] =
    F.delay(client.getObject(request))

  def listObjects(request: ListObjectsV2Request)(implicit F: Effect[F]): F[ListObjectsV2Iterable] =
    F.delay(client.listObjectsV2Paginator(request))

}
