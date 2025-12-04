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
import org.apache.polaris.benchmarks.RetryOnHttpCodes.{
  retryOnHttpStatus,
  HttpRequestBuilderWithStatusSave
}
import org.apache.polaris.benchmarks.parameters.ConnectionParameters
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicReference

/**
 * Actions for performance testing authentication operations. This class provides methods to
 * authenticate and manage access tokens for API requests.
 *
 * @param cp Connection parameters containing client credentials
 * @param accessToken Reference to the authentication token shared across actions
 * @param maxRetries Maximum number of retry attempts for failed operations
 * @param retryableHttpCodes HTTP status codes that should trigger a retry
 */
case class AuthenticationActions(
    cp: ConnectionParameters,
    accessToken: AtomicReference[String],
    maxRetries: Int = 10,
    retryableHttpCodes: Set[Int] = Set(500)
) {
  private val logger = LoggerFactory.getLogger(getClass)

  // Store principal credentials after creation
  private val principalClientId = new AtomicReference[String](null)
  private val principalClientSecret = new AtomicReference[String](null)

  /**
   * Creates a Gatling Feeder that provides root authentication credentials. The feeder continuously
   * supplies root client ID and client secret from the connection parameters for use in authentication
   * requests.
   *
   * @return An iterator providing root client credentials
   */
  def rootFeeder(): Feeder[String] = Iterator.continually(
    Map(
      "clientId" -> cp.rootClientId,
      "clientSecret" -> cp.rootClientSecret
    )
  )

  /**
   * Creates a Gatling Feeder that provides the principal name from configuration.
   * This is used when creating the principal.
   *
   * @return An iterator providing the principal name
   */
  def principalNameFeeder(): Feeder[String] = Iterator.continually(
    Map(
      "principalName" -> cp.principalName
    )
  )

  /**
   * Creates a Gatling Feeder that provides principal authentication credentials. The feeder continuously
   * supplies principal client ID and client secret that were obtained when the principal was created.
   *
   * @return An iterator providing principal client credentials
   */
  def principalFeeder(): Feeder[String] = Iterator.continually(
    Map(
      "clientId" -> principalClientId.get(),
      "clientSecret" -> principalClientSecret.get()
    )
  )

  /**
   * Stores principal credentials in the atomic references for later use in authentication.
   * This should be called after creating a principal to capture its credentials from the session.
   * The credentials are expected to be in session variables "principalClientId" and "principalClientSecret".
   */
  val savePrincipalCredentials: ChainBuilder = exec { session =>
    session("principalClientId").asOption[String].foreach { id =>
      principalClientId.set(id)
      logger.info(s"Saved principal client ID: $id")
    }
    session("principalClientSecret").asOption[String].foreach { secret =>
      principalClientSecret.set(secret)
      logger.info("Saved principal client secret")
    }
    session
  }

  /**
   * Authenticates using principal credentials and saves the access token as a session attribute. The
   * credentials should be provided via [[principalFeeder]]. This operation performs an OAuth2 client
   * credentials flow, requesting full principal roles, and stores the received access token in
   * both the Gatling session and the shared AtomicReference.
   *
   * Use this for benchmark operations after the principal has been set up with necessary roles and grants.
   *
   * There is no limit to the maximum number of users that can authenticate concurrently.
   */
  val authPrincipalAndSaveAccessToken: ChainBuilder =
    retryOnHttpStatus(maxRetries, retryableHttpCodes, "Authenticate Principal")(
      http("Authenticate Principal")
        .post("/api/catalog/v1/oauth/tokens")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .formParam("grant_type", "client_credentials")
        .formParam("client_id", "#{clientId}")
        .formParam("client_secret", "#{clientSecret}")
        .formParam("scope", "PRINCIPAL_ROLE:ALL")
        .saveHttpStatusCode()
        .check(status.is(200))
        .check(jsonPath("$.access_token").saveAs("accessToken"))
    )
      .exec { session =>
        if (session.contains("accessToken")) {
          accessToken.set(session("accessToken").as[String])
        }
        session
      }

  /**
   * Authenticates using root credentials and saves the access token as a session attribute. The
   * credentials should be provided via [[rootFeeder]]. This operation performs an OAuth2 client
   * credentials flow, requesting full principal roles, and stores the received access token in
   * both the Gatling session and the shared AtomicReference.
   *
   * Use this for administrative operations (creating principals, roles, grants).
   *
   * There is no limit to the maximum number of users that can authenticate concurrently.
   */
  val authRootAndSaveAccessToken: ChainBuilder =
    retryOnHttpStatus(maxRetries, retryableHttpCodes, "Authenticate Root")(
      http("Authenticate Root")
        .post("/api/catalog/v1/oauth/tokens")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .formParam("grant_type", "client_credentials")
        .formParam("client_id", "#{clientId}")
        .formParam("client_secret", "#{clientSecret}")
        .formParam("scope", "PRINCIPAL_ROLE:ALL")
        .saveHttpStatusCode()
        .check(status.is(200))
        .check(jsonPath("$.access_token").saveAs("accessToken"))
    )
      .exec { session =>
        if (session.contains("accessToken")) {
          accessToken.set(session("accessToken").as[String])
        }
        session
      }

  /**
   * Restores the current access token from the shared reference into the Gatling session. This
   * operation is useful when a scenario needs to reuse an authentication token from a previous
   * scenario.
   */
  val restoreAccessTokenInSession: ChainBuilder =
    exec(session => session.set("accessToken", accessToken.get()))
}
