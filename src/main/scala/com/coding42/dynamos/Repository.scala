package com.coding42.dynamos

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, GetItemRequest, PutItemRequest}

import scala.compat.java8.FutureConverters._
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import Repository._

class Repository[A](tableName: String, client: DynamoDbAsyncClient)(implicit ec: ExecutionContext, writer: DynamosWriter[A], reader: DynamosReader[A]) {

  def add(a: A): Future[Unit] = {
    val attributes = a.toDynamoDb.m()
    val request = PutItemRequest.builder().tableName(tableName).item(attributes).build()

    client.putItem(request).toScala.flatMap {
      case r if r.sdkHttpResponse().isSuccessful => Future.successful(())
      case _ => Future.failed(new Exception("Failed to put item"))
    }
  }

  def getByParams(params: Seq[Param]): Future[Option[A]] = {

    val attributes =  build(params: _*)
    val request = GetItemRequest
      .builder()
      .tableName(tableName)
      .key(attributes.asJava)
      .build()
    client.getItem(request).toScala.flatMap {
      case r if r.sdkHttpResponse().isSuccessful => Dynamos.fromDynamo(r.item()) match {
        case Left(value) => Future.failed(new Exception(value.field))
        case Right(v) => Future.successful(v)
      }
      case _ => Future.failed(new Exception("Failed to get item"))
    }
  }





}

object Repository {

  sealed trait Param
  case class IntParam(key: String, value: Int) extends Param
  case class StringParam(key: String, value: String) extends Param

  private def build(params: Param*): Map[String, AttributeValue] = params.map {
    case IntParam(key, value) =>
      key -> AttributeValue.builder().n(value.toString).build()
    case StringParam(key, value) =>
      key -> AttributeValue.builder().s(value).build()
  }.toMap

}
