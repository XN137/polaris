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

package org.apache.polaris.core.persistence;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.LocationBasedEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisEvent;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.ChangeTrackingResult;
import org.apache.polaris.core.persistence.dao.entity.CreateCatalogResult;
import org.apache.polaris.core.persistence.dao.entity.CreatePrincipalResult;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityWithPath;
import org.apache.polaris.core.persistence.dao.entity.GenerateEntityIdResult;
import org.apache.polaris.core.persistence.dao.entity.ListEntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.LoadGrantsResult;
import org.apache.polaris.core.persistence.dao.entity.LoadPolicyMappingsResult;
import org.apache.polaris.core.persistence.dao.entity.PolicyAttachmentResult;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.apache.polaris.core.persistence.dao.entity.PrivilegeResult;
import org.apache.polaris.core.persistence.dao.entity.ResolvedEntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.ResolvedEntityResult;
import org.apache.polaris.core.persistence.dao.entity.ScopedCredentialsResult;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.policy.PolicyEntity;
import org.apache.polaris.core.policy.PolicyType;

/** a MetaStore that delegates to PolarisMetaStoreManager using the given CallContext */
public class ManagerBasedMetaStore implements MetaStore {

  private final PolarisMetaStoreManager metaStoreManager;
  private final PolarisCallContext callContext;

  public ManagerBasedMetaStore(PolarisMetaStoreManager metaStoreManager, CallContext callContext) {
    this.metaStoreManager = metaStoreManager;
    this.callContext = callContext.getPolarisCallContext();
  }

  @Nonnull
  @Override
  public BaseResult purge() {
    return metaStoreManager.purge(callContext);
  }

  @Nonnull
  @Override
  public EntityResult readEntityByName(
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType,
      @Nonnull String name) {
    return metaStoreManager.readEntityByName(
        callContext, catalogPath, entityType, entitySubType, name);
  }

  @Nonnull
  @Override
  public ListEntitiesResult listEntities(
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType,
      @Nonnull PageToken pageToken) {
    return metaStoreManager.listEntities(
        callContext, catalogPath, entityType, entitySubType, pageToken);
  }

  @Nonnull
  @Override
  public Page<PolarisBaseEntity> listFullEntities(
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType,
      @Nonnull PageToken pageToken) {
    return metaStoreManager.listFullEntities(
        callContext, catalogPath, entityType, entitySubType, pageToken);
  }

  @Nonnull
  @Override
  public List<PolarisBaseEntity> listFullEntitiesAll(
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType) {
    return metaStoreManager.listFullEntitiesAll(
        callContext, catalogPath, entityType, entitySubType);
  }

  @Nonnull
  @Override
  public GenerateEntityIdResult generateNewEntityId() {
    return metaStoreManager.generateNewEntityId(callContext);
  }

  @Nonnull
  @Override
  public CreatePrincipalResult createPrincipal(@Nonnull PrincipalEntity principal) {
    return metaStoreManager.createPrincipal(callContext, principal);
  }

  @Nonnull
  @Override
  public CreateCatalogResult createCatalog(
      @Nonnull PolarisBaseEntity catalog, @Nonnull List<PolarisEntityCore> principalRoles) {
    return metaStoreManager.createCatalog(callContext, catalog, principalRoles);
  }

  @Nonnull
  @Override
  public EntityResult createEntityIfNotExists(
      @Nullable List<PolarisEntityCore> catalogPath, @Nonnull PolarisBaseEntity entity) {
    return metaStoreManager.createEntityIfNotExists(callContext, catalogPath, entity);
  }

  @Nonnull
  @Override
  public EntitiesResult createEntitiesIfNotExist(
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull List<? extends PolarisBaseEntity> entities) {
    return metaStoreManager.createEntitiesIfNotExist(callContext, catalogPath, entities);
  }

  @Nonnull
  @Override
  public EntityResult updateEntityPropertiesIfNotChanged(
      @Nullable List<PolarisEntityCore> catalogPath, @Nonnull PolarisBaseEntity entity) {
    return metaStoreManager.updateEntityPropertiesIfNotChanged(callContext, catalogPath, entity);
  }

  @Nonnull
  @Override
  public EntitiesResult updateEntitiesPropertiesIfNotChanged(
      @Nonnull List<EntityWithPath> entities) {
    return metaStoreManager.updateEntitiesPropertiesIfNotChanged(callContext, entities);
  }

  @Nonnull
  @Override
  public EntityResult renameEntity(
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entityToRename,
      @Nullable List<PolarisEntityCore> newCatalogPath,
      @Nonnull PolarisEntity renamedEntity) {
    return metaStoreManager.renameEntity(
        callContext, catalogPath, entityToRename, newCatalogPath, renamedEntity);
  }

  @Nonnull
  @Override
  public DropEntityResult dropEntityIfExists(
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entityToDrop,
      @Nullable Map<String, String> cleanupProperties,
      boolean cleanup) {
    return metaStoreManager.dropEntityIfExists(
        callContext, catalogPath, entityToDrop, cleanupProperties, cleanup);
  }

  @Nonnull
  @Override
  public EntityResult loadEntity(
      long entityCatalogId, long entityId, @Nonnull PolarisEntityType entityType) {
    return metaStoreManager.loadEntity(callContext, entityCatalogId, entityId, entityType);
  }

  @Nonnull
  @Override
  public EntitiesResult loadTasks(String executorId, PageToken pageToken) {
    return metaStoreManager.loadTasks(callContext, executorId, pageToken);
  }

  @Nonnull
  @Override
  public ChangeTrackingResult loadEntitiesChangeTracking(@Nonnull List<PolarisEntityId> entityIds) {
    return metaStoreManager.loadEntitiesChangeTracking(callContext, entityIds);
  }

  @Nonnull
  @Override
  public ResolvedEntityResult loadResolvedEntityById(
      long entityCatalogId, long entityId, PolarisEntityType entityType) {
    return metaStoreManager.loadResolvedEntityById(
        callContext, entityCatalogId, entityId, entityType);
  }

  @Nonnull
  @Override
  public ResolvedEntityResult loadResolvedEntityByName(
      long entityCatalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull String entityName) {
    return metaStoreManager.loadResolvedEntityByName(
        callContext, entityCatalogId, parentId, entityType, entityName);
  }

  @Nonnull
  @Override
  public ResolvedEntitiesResult loadResolvedEntities(
      @Nonnull PolarisEntityType entityType, @Nonnull List<PolarisEntityId> entityIds) {
    return metaStoreManager.loadResolvedEntities(callContext, entityType, entityIds);
  }

  @Nonnull
  @Override
  public ResolvedEntityResult refreshResolvedEntity(
      int entityVersion,
      int entityGrantRecordsVersion,
      @Nonnull PolarisEntityType entityType,
      long entityCatalogId,
      long entityId) {
    return metaStoreManager.refreshResolvedEntity(
        callContext,
        entityVersion,
        entityGrantRecordsVersion,
        entityType,
        entityCatalogId,
        entityId);
  }

  @Nonnull
  @Override
  public PrivilegeResult grantUsageOnRoleToGrantee(
      @Nullable PolarisEntityCore catalog,
      @Nonnull PolarisEntityCore role,
      @Nonnull PolarisEntityCore grantee) {
    return metaStoreManager.grantUsageOnRoleToGrantee(callContext, catalog, role, grantee);
  }

  @Nonnull
  @Override
  public PrivilegeResult revokeUsageOnRoleFromGrantee(
      @Nullable PolarisEntityCore catalog,
      @Nonnull PolarisEntityCore role,
      @Nonnull PolarisEntityCore grantee) {
    return metaStoreManager.revokeUsageOnRoleFromGrantee(callContext, catalog, role, grantee);
  }

  @Nonnull
  @Override
  public PrivilegeResult grantPrivilegeOnSecurableToRole(
      @Nonnull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore securable,
      @Nonnull PolarisPrivilege privilege) {
    return metaStoreManager.grantPrivilegeOnSecurableToRole(
        callContext, grantee, catalogPath, securable, privilege);
  }

  @Nonnull
  @Override
  public PrivilegeResult revokePrivilegeOnSecurableFromRole(
      @Nonnull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore securable,
      @Nonnull PolarisPrivilege privilege) {
    return metaStoreManager.revokePrivilegeOnSecurableFromRole(
        callContext, grantee, catalogPath, securable, privilege);
  }

  @Nonnull
  @Override
  public LoadGrantsResult loadGrantsOnSecurable(PolarisEntityCore securable) {
    return metaStoreManager.loadGrantsOnSecurable(callContext, securable);
  }

  @Nonnull
  @Override
  public LoadGrantsResult loadGrantsToGrantee(PolarisEntityCore grantee) {
    return metaStoreManager.loadGrantsToGrantee(callContext, grantee);
  }

  @Nonnull
  @Override
  public PolicyAttachmentResult attachPolicyToEntity(
      @Nonnull List<PolarisEntityCore> targetCatalogPath,
      @Nonnull PolarisEntityCore target,
      @Nonnull List<PolarisEntityCore> policyCatalogPath,
      @Nonnull PolicyEntity policy,
      Map<String, String> parameters) {
    return metaStoreManager.attachPolicyToEntity(
        callContext, targetCatalogPath, target, policyCatalogPath, policy, parameters);
  }

  @Nonnull
  @Override
  public PolicyAttachmentResult detachPolicyFromEntity(
      @Nonnull List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore target,
      @Nonnull List<PolarisEntityCore> policyCatalogPath,
      @Nonnull PolicyEntity policy) {
    return metaStoreManager.detachPolicyFromEntity(
        callContext, catalogPath, target, policyCatalogPath, policy);
  }

  @Nonnull
  @Override
  public LoadPolicyMappingsResult loadPoliciesOnEntity(@Nonnull PolarisEntityCore target) {
    return metaStoreManager.loadPoliciesOnEntity(callContext, target);
  }

  @Nonnull
  @Override
  public LoadPolicyMappingsResult loadPoliciesOnEntityByType(
      @Nonnull PolarisEntityCore target, @Nonnull PolicyType policyType) {
    return metaStoreManager.loadPoliciesOnEntityByType(callContext, target, policyType);
  }

  @Override
  public <T extends PolarisEntity & LocationBasedEntity>
      Optional<Optional<String>> hasOverlappingSiblings(@Nonnull T entity) {
    return metaStoreManager.hasOverlappingSiblings(callContext, entity);
  }

  @Override
  public boolean requiresEntityReload() {
    return metaStoreManager.requiresEntityReload();
  }

  @Override
  public Optional<PrincipalEntity> findRootPrincipal() {
    return metaStoreManager.findRootPrincipal(callContext);
  }

  @Override
  public Optional<PrincipalEntity> findPrincipalById(long principalId) {
    return metaStoreManager.findPrincipalById(callContext, principalId);
  }

  @Override
  public Optional<PrincipalEntity> findPrincipalByName(String principalName) {
    return metaStoreManager.findPrincipalByName(callContext, principalName);
  }

  @Nonnull
  @Override
  public ScopedCredentialsResult getSubscopedCredsForEntity(
      long catalogId,
      long entityId,
      PolarisEntityType entityType,
      boolean allowListOperation,
      @Nonnull Set<String> allowedReadLocations,
      @Nonnull Set<String> allowedWriteLocations,
      Optional<String> refreshCredentialsEndpoint) {
    return metaStoreManager.getSubscopedCredsForEntity(
        callContext,
        catalogId,
        entityId,
        entityType,
        allowListOperation,
        allowedReadLocations,
        allowedWriteLocations,
        refreshCredentialsEndpoint);
  }

  @Nonnull
  @Override
  public PrincipalSecretsResult loadPrincipalSecrets(@Nonnull String clientId) {
    return metaStoreManager.loadPrincipalSecrets(callContext, clientId);
  }

  @Nonnull
  @Override
  public PrincipalSecretsResult rotatePrincipalSecrets(
      @Nonnull String clientId, long principalId, boolean reset, @Nonnull String oldSecretHash) {
    return metaStoreManager.rotatePrincipalSecrets(
        callContext, clientId, principalId, reset, oldSecretHash);
  }

  @Nonnull
  @Override
  public PrincipalSecretsResult resetPrincipalSecrets(
      long principalId, @Nonnull String resolvedClientId, String customClientSecret) {
    return metaStoreManager.resetPrincipalSecrets(
        callContext, principalId, resolvedClientId, customClientSecret);
  }

  @Override
  public void deletePrincipalSecrets(@Nonnull String clientId, long principalId) {
    metaStoreManager.deletePrincipalSecrets(callContext, clientId, principalId);
  }

  @Override
  public void writeEvents(@Nonnull List<PolarisEvent> polarisEvents) {
    metaStoreManager.writeEvents(callContext, polarisEvents);
  }
}
