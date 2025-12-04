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
import io.gatling.core.structure.ChainBuilder
import io.gatling.http.Predef._
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicReference

/**
 * Actions for managing principals, principal roles, catalog roles, and privileges.
 * This class provides methods to set up the necessary access control for benchmarks.
 *
 * @param accessToken Reference to the authentication token shared across actions
 */
case class PrincipalActions(accessToken: AtomicReference[String]) {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Creates a principal if it doesn't already exist. Returns 409 if the principal already exists,
   * which is acceptable. The API returns PrincipalWithCredentials on success (201).
   */
  val createPrincipal: ChainBuilder = exec(
    http("Create Principal")
      .post("/api/management/v1/principals")
      .header("Authorization", "Bearer #{accessToken}")
      .header("Content-Type", "application/json")
      .body(
        StringBody(
          """{
            |  "principal": {
            |    "name": "#{principalName}"
            |  },
            |  "credentialRotationRequired": false
            |}""".stripMargin
        )
      )
      .check(status.in(201, 409))
      .checkIf(status.is(201)) {
        jsonPath("$.credentials.clientId").saveAs("principalClientId")
      }
      .checkIf(status.is(201)) {
        jsonPath("$.credentials.clientSecret").saveAs("principalClientSecret")
      }
  )

  /**
   * Creates a principal role if it doesn't already exist. Returns 201 on success, 409 if already exists.
   */
  val createPrincipalRole: ChainBuilder = exec(
    http("Create Principal Role")
      .post("/api/management/v1/principal-roles")
      .header("Authorization", "Bearer #{accessToken}")
      .header("Content-Type", "application/json")
      .body(
        StringBody(
          """{
            |  "principalRole": {
            |    "name": "#{principalRoleName}"
            |  }
            |}""".stripMargin
        )
      )
      .check(status.in(201, 409))
  )

  /**
   * Creates a catalog role if it doesn't already exist. Returns 201 on success, 409 if already exists.
   */
  val createCatalogRole: ChainBuilder = exec(
    http("Create Catalog Role")
      .post("/api/management/v1/catalogs/#{catalogName}/catalog-roles")
      .header("Authorization", "Bearer #{accessToken}")
      .header("Content-Type", "application/json")
      .body(
        StringBody(
          """{
            |  "catalogRole": {
            |    "name": "#{catalogRoleName}"
            |  }
            |}""".stripMargin
        )
      )
      .check(status.in(201, 409))
  )

  /**
   * Grants a principal role to a principal. According to the API spec, this is a PUT request
   * with a request body containing the principal role.
   */
  val grantPrincipalRole: ChainBuilder = exec(
    http("Grant Principal Role to Principal")
      .put("/api/management/v1/principals/#{principalName}/principal-roles")
      .header("Authorization", "Bearer #{accessToken}")
      .header("Content-Type", "application/json")
      .body(
        StringBody(
          """{
            |  "principalRole": {
            |    "name": "#{principalRoleName}"
            |  }
            |}""".stripMargin
        )
      )
      .check(status.in(201, 404))
  )

  /**
   * Grants a catalog role to a principal role. According to the API spec, this is a PUT request
   * to /principal-roles/{principalRoleName}/catalog-roles/{catalogName} with a request body.
   */
  val grantCatalogRole: ChainBuilder = exec(
    http("Grant Catalog Role to Principal Role")
      .put(
        "/api/management/v1/principal-roles/#{principalRoleName}/catalog-roles/#{catalogName}"
      )
      .header("Authorization", "Bearer #{accessToken}")
      .header("Content-Type", "application/json")
      .body(
        StringBody(
          """{
            |  "catalogRole": {
            |    "name": "#{catalogRoleName}"
            |  }
            |}""".stripMargin
        )
      )
      .check(status.in(201, 403, 404))
  )

  /**
   * Grants CATALOG_MANAGE_CONTENT privilege to a catalog role. According to the API spec,
   * this is a PUT request with an AddGrantRequest body containing a GrantResource.
   */
  val grantCatalogManageContent: ChainBuilder = exec(
    http("Grant CATALOG_MANAGE_CONTENT Privilege")
      .put("/api/management/v1/catalogs/#{catalogName}/catalog-roles/#{catalogRoleName}/grants")
      .header("Authorization", "Bearer #{accessToken}")
      .header("Content-Type", "application/json")
      .body(
        StringBody(
          """{
            |  "grant": {
            |    "type": "catalog",
            |    "privilege": "CATALOG_MANAGE_CONTENT"
            |  }
            |}""".stripMargin
        )
      )
      .check(status.in(201, 403, 404))
  )

  /**
   * Complete setup: creates principal, roles, and grants all necessary privileges.
   */
  val setupPrincipalWithAccess: ChainBuilder = exec(
    createPrincipal,
    createPrincipalRole,
    createCatalogRole,
    grantPrincipalRole,
    grantCatalogRole,
    grantCatalogManageContent
  )
}

