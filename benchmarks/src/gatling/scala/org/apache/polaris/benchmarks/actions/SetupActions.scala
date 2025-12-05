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
   * After execution:
   *   - The principal credentials (principalClientId, principalClientSecret) will be saved in the
   *     session and in the AuthenticationActions for later use.
   *   - The principal will be authenticated and the access token will be saved in principalAccessToken.
   *
   * Flow:
   *   1. Create principal (using rootToken)
   *   2. Save principal credentials
   *   3. Create principal role (using rootToken)
   *   4. Grant principal role to principal (using rootToken)
   *   5. Authenticate as principal and save token in principalAccessToken
   */
  val setupPrincipalWithRole: ChainBuilder = exec(
    principalActions.createPrincipal,
    authActions.savePrincipalCredentials,
    principalActions.createPrincipalRole,
    principalActions.grantPrincipalRole,
    feed(authActions.principalFeeder()),
    authActions.authPrincipalAndSaveAccessToken
  )

  /**
   * Complete setup flow for a single catalog and principal:
   *   1. Authenticate as root
   *   2. Create principal and principal role
   *   3. Authenticate as principal (done within setupPrincipalWithRole)
   *   4. Create catalog and catalog role
   *   5. Grant catalog role to principal role
   *
   * Expected session variables:
   *   - principalName: Name of the principal to create
   *   - principalRoleName: Name of the principal role to create
   *   - catalogName: Name of the catalog to create
   *   - catalogRoleName: Name of the catalog role to create
   *   - defaultBaseLocation: Base storage location for the catalog
   *   - storageConfigInfo: Storage configuration JSON
   *
   * Note: Principal authentication is now handled within setupPrincipalWithRole, so the
   * principalAccessToken will be available after that step completes.
   */
  val setupSingleCatalogAndPrincipal: ChainBuilder = exec(
    // Phase 1: Root authentication and setup
    feed(authActions.rootFeeder()),
    authActions.authRootAndSaveAccessToken,
    setupPrincipalWithRole,
    setupCatalogWithRole
  )

  /**
   * Setup flow for creating a principal once and then setting up multiple catalogs. This is more
   * efficient when you need to grant access to multiple catalogs for the same principal.
   *
   * Usage:
   *   1. Call setupPrincipalOnce with principal details
   *   2. For each catalog, call setupAdditionalCatalog with catalog details
   *   3. Optionally call authenticateAsPrincipal to refresh the principal token
   *
   * Note: Principal authentication is now handled within setupPrincipalWithRole, so the
   * principalAccessToken will be available immediately after setupPrincipalOnce completes.
   * The authenticateAsPrincipal step is now optional and only needed if you want to refresh
   * the token.
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
    setupCatalogWithRole
  )

  /**
   * Authenticates as the principal. This can be used to refresh the principal token if needed.
   *
   * Note: This is now optional after setupPrincipalOnce since principal authentication is
   * already performed within setupPrincipalWithRole. Only use this if you need to refresh
   * the token or re-authenticate.
   */
  val authenticateAsPrincipal: ChainBuilder = exec(
    feed(authActions.principalFeeder()),
    authActions.authPrincipalAndSaveAccessToken
  )
}
