/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.benchmarks.actions

import io.gatling.core.Predef._
import io.gatling.core.feeder.Feeder
import io.gatling.core.structure.ChainBuilder
import io.gatling.http.Predef._
import org.apache.polaris.benchmarks.parameters.{DatasetParameters, WorkloadParameters}
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicReference

/**
 * Actions for performance testing S3 sign requests. This class provides methods to sign
 * S3 requests for table files.
 *
 * @param dp Dataset parameters controlling the dataset generation
 * @param wp Workload parameters controlling the workload configuration
 * @param accessToken Reference to the authentication token shared across actions
 */
case class S3SignActions(
    dp: DatasetParameters,
    wp: WorkloadParameters,
    accessToken: AtomicReference[String]
) {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Creates a Gatling Feeder that generates table identities for S3 sign requests.
   * This feeder provides catalog, namespace, and table names for existing tables.
   *
   * @return An iterator providing table identity details
   */
  def s3SignFeeder(): Feeder[Any] = Iterator
    .from(0)
    .flatMap { _ =>
      dp.nAryTree.lastLevelOrdinals.iterator
        .flatMap { namespaceId =>
          val positionInLevel = namespaceId - dp.nAryTree.lastLevelOrdinals.head
          val parentNamespacePath: Seq[String] = dp.nAryTree
            .pathToRoot(namespaceId)
            .map(ordinal => s"NS_$ordinal")
          Range(0, dp.numTablesPerNs)
            .map { j =>
              val tableId = positionInLevel * dp.numTablesPerNs + j
              val catalogName = "C_0"
              val tableName = s"T_$tableId"

              Map(
                "catalogName" -> catalogName,
                "multipartNamespace" -> parentNamespacePath.mkString("%1F"),
                "tableName" -> tableName,
                "region" -> wp.s3SignRequests.region,
                "method" -> "GET"
              )
            }
        }
        .take(dp.numTables)
    }

  /**
   * Fetches table metadata and extracts the table location. This is used to get the
   * actual S3 location of the table before signing requests.
   */
  val fetchTableLocation: ChainBuilder = exec(
    http("Fetch Table for S3 Sign")
      .get("/api/catalog/v1/#{catalogName}/namespaces/#{multipartNamespace}/tables/#{tableName}")
      .header("Authorization", session => s"Bearer ${accessToken.get()}")
      .check(status.is(200))
      .check(jsonPath("$.metadata.location").saveAs("tableLocation"))
  )

  /**
   * Signs an S3 request by calling the S3 sign endpoint. This operation sends a POST request
   * to the S3 sign API with the region, method, URI, and headers. The URI is constructed from
   * the table location retrieved in the previous step.
   */
  val signS3Request: ChainBuilder = exec { session =>
    // Get the table location from the session (e.g., "s3://bucket/path/to/table")
    val tableLocation = session("tableLocation").as[String]
    val region = session("region").as[String]

    // Extract bucket name and path from S3 location
    val bucketName = tableLocation.stripPrefix("s3://").split("/")(0)
    val s3Path = tableLocation.stripPrefix(s"s3://$bucketName/")

    // Construct a file path within the table (e.g., data/file.parquet or metadata/metadata.json)
    val filePath = s"$s3Path/data/file.parquet"

    // Construct HTTP URL for S3
    val s3Domain = if (region == "us-east-1") "s3.amazonaws.com" else s"s3.$region.amazonaws.com"
    val fileUri = s"https://$bucketName.$s3Domain/$filePath"

    // Save the constructed URI to the session
    session.set("uri", fileUri)
  }.exec(
    http("Sign S3 Request")
      .post("/api/s3-sign/v1/#{catalogName}/namespaces/#{multipartNamespace}/tables/#{tableName}")
      .header("Authorization", session => s"Bearer ${accessToken.get()}")
      .header("Content-Type", "application/json")
      .body(
        StringBody(
          """{
            |  "region": "#{region}",
            |  "method": "#{method}",
            |  "uri": "#{uri}",
            |  "headers": {}
            |}""".stripMargin
        )
      )
      .check(status.is(200))
      .check(jsonPath("$.uri").exists)
      .check(jsonPath("$.headers").exists)
  )
}

