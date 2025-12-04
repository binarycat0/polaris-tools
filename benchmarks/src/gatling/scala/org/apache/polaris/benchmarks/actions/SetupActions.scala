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
import org.slf4j.LoggerFactory

/**
 * Orchestrates the complete setup flow for benchmarks, managing the proper separation between root
 * and principal authentication.
 *
 * The setup follows the Polaris Management API specification:
 *   1. Authenticate as root
 *   2. Create catalog (requires root token)
 *   3. Create catalog role for the catalog (requires root token)
 *   4. Create principal and save credentials (requires root token)
 *   5. Create principal role (requires root token)
 *   6. Grant principal role to principal (requires root token)
 *   7. Grant catalog role to principal role (requires root token)
 *   8. Grant CATALOG_MANAGE_CONTENT privilege to catalog role (requires root token)
 *   9. Authenticate as principal using saved credentials
 *   10. Perform benchmark operations (requires principal token)
 *
 * @param authActions Authentication actions for root and principal auth
 * @param catalogActions Catalog management actions
 * @param principalActions Principal and role management actions
 */
case class SetupActions(
    authActions: AuthenticationActions,
    catalogActions: CatalogActions,
    principalActions: PrincipalActions
) {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Sets up a single catalog with a dedicated catalog role and grants necessary privileges. This
   * must be executed with root authentication token in the session.
   *
   * Expected session variables:
   *   - catalogName: Name of the catalog to create
   *   - catalogRoleName: Name of the catalog role to create
   *   - principalRoleName: Name of the principal role to grant the catalog role to
   *   - defaultBaseLocation: Base storage location for the catalog
   *   - storageConfigInfo: Storage configuration JSON
   */
  val setupCatalogWithRole: ChainBuilder = exec(
    catalogActions.createCatalog,
    catalogActions.createCatalogRole,
    principalActions.grantCatalogRole,
    catalogActions.grantCatalogPrivileges
  )

  /**
   * Sets up a principal with a principal role. This must be executed with root authentication token
   * in the session.
   *
   * Expected session variables:
   *   - principalName: Name of the principal to create
   *   - principalRoleName: Name of the principal role to create
   *
   * After execution, the principal credentials (principalClientId, principalClientSecret) will be
   * saved in the session and in the AuthenticationActions for later use.
   */
  val setupPrincipalWithRole: ChainBuilder = exec(
    principalActions.createPrincipal,
    authActions.savePrincipalCredentials,
    principalActions.createPrincipalRole,
    principalActions.grantPrincipalRole
  )

  /**
   * Complete setup flow for a single catalog and principal:
   *   1. Authenticate as root
   *   2. Create principal and principal role
   *   3. Create catalog and catalog role
   *   4. Grant catalog role to principal role
   *   5. Authenticate as principal
   *
   * Expected session variables:
   *   - principalName: Name of the principal to create
   *   - principalRoleName: Name of the principal role to create
   *   - catalogName: Name of the catalog to create
   *   - catalogRoleName: Name of the catalog role to create
   *   - defaultBaseLocation: Base storage location for the catalog
   *   - storageConfigInfo: Storage configuration JSON
   */
  val setupSingleCatalogAndPrincipal: ChainBuilder = exec(
    // Phase 1: Root authentication and setup
    feed(authActions.rootFeeder()),
    authActions.authRootAndSaveAccessToken,
    setupPrincipalWithRole,
    setupCatalogWithRole,
    // Phase 2: Principal authentication
    feed(authActions.principalFeeder()),
    authActions.authPrincipalAndSaveAccessToken
  )

  /**
   * Setup flow for creating a principal once and then setting up multiple catalogs. This is more
   * efficient when you need to grant access to multiple catalogs for the same principal.
   *
   * Usage:
   *   1. Call setupPrincipalOnce with principal details
   *   2. For each catalog, call setupAdditionalCatalog with catalog details
   *   3. Finally call authenticateAsPrincipal to switch to principal token
   */
  val setupPrincipalOnce: ChainBuilder = exec(
    feed(authActions.rootFeeder()),
    authActions.authRootAndSaveAccessToken,
    setupPrincipalWithRole
  )

  /**
   * Adds an additional catalog for an already-created principal. Must be called after
   * setupPrincipalOnce and with root token in session.
   *
   * Expected session variables:
   *   - catalogName: Name of the catalog to create
   *   - catalogRoleName: Name of the catalog role to create
   *   - principalRoleName: Name of the principal role (must already exist)
   *   - defaultBaseLocation: Base storage location for the catalog
   *   - storageConfigInfo: Storage configuration JSON
   */
  val setupAdditionalCatalog: ChainBuilder = exec(
    authActions.setRootAccessTokenInSession,
    setupCatalogWithRole
  )

  /**
   * Authenticates as the principal after setup is complete. Call this after setupPrincipalOnce and
   * all setupAdditionalCatalog calls.
   */
  val authenticateAsPrincipal: ChainBuilder = exec(
    feed(authActions.principalFeeder()),
    authActions.authPrincipalAndSaveAccessToken
  )
}
