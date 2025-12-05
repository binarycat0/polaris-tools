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
package org.apache.polaris.benchmarks.simulations

import io.gatling.core.Predef._
import io.gatling.core.structure.ScenarioBuilder
import io.gatling.http.Predef.http
import org.apache.polaris.benchmarks.actions.{AuthenticationActions, CatalogActions, PrincipalActions, S3SignActions}
import org.apache.polaris.benchmarks.parameters.BenchmarkConfig.config
import org.apache.polaris.benchmarks.parameters.{
  ConnectionParameters,
  DatasetParameters,
  WorkloadParameters
}
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.duration._

/**
 * This simulation tests the S3 sign endpoint by sending POST requests to sign S3 requests
 * for table files. It measures the latency of the S3 sign endpoint under load.
 */
class S3SignRequests extends Simulation {
  private val logger = LoggerFactory.getLogger(getClass)

  // --------------------------------------------------------------------------------
  // Load parameters
  // --------------------------------------------------------------------------------
  val connParams: ConnectionParameters = config.connectionParameters
  val catParams = config.catalogParameters
  val dp: DatasetParameters = config.datasetParameters
  val wp: WorkloadParameters = config.workloadParameters

  // --------------------------------------------------------------------------------
  // Helper values
  // --------------------------------------------------------------------------------
  private val rootAccessToken: AtomicReference[String] = new AtomicReference()
  private val principalAccessToken: AtomicReference[String] = new AtomicReference()
  private val shouldRefreshToken: AtomicBoolean = new AtomicBoolean(true)

  private val authActions = AuthenticationActions(connParams, rootAccessToken, principalAccessToken)
  private val catalogActions = CatalogActions(catParams, dp, rootAccessToken)
  private val principalActions = PrincipalActions(rootAccessToken, authActions)
  private val s3SignActions = S3SignActions(dp, wp, principalAccessToken)

  // --------------------------------------------------------------------------------
  // Principal setup scenario:
  // * Create principal, roles, and grant necessary privileges
  // --------------------------------------------------------------------------------
  val setupPrincipal: ScenarioBuilder =
    scenario("Setup principal with catalog access")
      .exec { session =>
        session
          .set("principalName", connParams.principalName)
          .set("principalRoleName", s"${connParams.principalName}_role")
          .set("catalogName", "C_0")
          .set("catalogRoleName", s"${connParams.principalName}_catalog_role")
      }
      .exec(principalActions.setupPrincipalWithAccess(
        catalogActions.createCatalogRole,
        catalogActions.grantCatalogPrivileges
      ))

  // --------------------------------------------------------------------------------
  // Authentication related workloads:
  // * Authenticate and store the access token for later use every minute
  // * Wait for an OAuth token to be available
  // * Stop the token refresh loop
  // --------------------------------------------------------------------------------
  val continuouslyRefreshOauthToken: ScenarioBuilder =
    scenario("Authenticate every minute using the Iceberg REST API")
      .asLongAs(_ => shouldRefreshToken.get())(
        feed(authActions.principalFeeder())
          .exec(authActions.authPrincipalAndSaveAccessToken)
          .pause(1.minute)
      )

  val waitForAuthentication: ScenarioBuilder =
    scenario("Wait for the authentication token to be available")
      .asLongAs(_ => principalAccessToken.get() == null)(
        pause(1.second)
      )

  val stopRefreshingToken: ScenarioBuilder =
    scenario("Stop refreshing the authentication token")
      .exec { session =>
        shouldRefreshToken.set(false)
        session
      }

  // --------------------------------------------------------------------------------
  // S3 Sign workload:
  // * Fetch table metadata to get the actual table location
  // * Sign S3 requests for table files using the retrieved location
  // --------------------------------------------------------------------------------
  val s3SignScenario: ScenarioBuilder =
    scenario("Sign S3 requests for table files")
      .feed(s3SignActions.s3SignFeeder())
      .exec(s3SignActions.fetchTableLocation)
      .exec(s3SignActions.signS3Request)

  // --------------------------------------------------------------------------------
  // Build up the HTTP protocol configuration and set up the simulation
  // --------------------------------------------------------------------------------
  private val httpProtocol = http
    .baseUrl(connParams.baseUrl)
    .acceptHeader("application/json")
    .contentTypeHeader("application/json")

  private val throughput = wp.s3SignRequests.throughput
  private val durationInMinutes = wp.s3SignRequests.durationInMinutes

  setUp(
    continuouslyRefreshOauthToken.inject(atOnceUsers(1)).protocols(httpProtocol),
    waitForAuthentication
      .inject(atOnceUsers(1))
      .andThen(setupPrincipal.inject(atOnceUsers(1)).protocols(httpProtocol))
      .andThen(
        s3SignScenario
          .inject(
            constantUsersPerSec(throughput).during(durationInMinutes.minutes)
          )
          .protocols(httpProtocol)
      )
      .andThen(stopRefreshingToken.inject(atOnceUsers(1)).protocols(httpProtocol))
  )
}

