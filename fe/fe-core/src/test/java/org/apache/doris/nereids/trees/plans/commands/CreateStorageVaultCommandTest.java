// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.StorageVault;
import org.apache.doris.catalog.StorageVaultMgr;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableMap;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class CreateStorageVaultCommandTest extends TestWithFeService {
    private String vaultName;

    @Override
    protected void runBeforeAll() throws Exception {
        vaultName = "hdfs_nereids";
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testValidateNormal(@Mocked AccessControllerManager accessManager) {
        new Expectations() {
            {
                Env.getCurrentEnv().getAccessManager();
                minTimes = 0;
                result = accessManager;

                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;
            }
        };

        Config.cloud_unique_id = "not_empty_nereids";
        ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
                .put("type", "hdfs")
                .build();
        CreateStorageVaultCommand command = new CreateStorageVaultCommand(true, vaultName, properties);
        Assertions.assertDoesNotThrow(() -> command.validate());
        Assertions.assertEquals(vaultName, command.getVaultName());
        Assertions.assertEquals(StorageVault.StorageVaultType.HDFS, command.getVaultType());

        // testUnsupportedResourceType
        ImmutableMap<String, String> properties1 = ImmutableMap.<String, String>builder()
                .put("type", "hadoop")
                .build();
        CreateStorageVaultCommand command1 = new CreateStorageVaultCommand(true, vaultName, properties1);
        Assertions.assertThrows(AnalysisException.class, () -> command1.validate());
        Config.cloud_unique_id = "";
    }

    @Test
    public void testCreateS3VaultWithPathFormat(@Mocked AccessControllerManager accessManager) throws Exception {
        new Expectations() {
            {
                Env.getCurrentEnv().getAccessManager();
                minTimes = 0;
                result = accessManager;

                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;
            }
        };
        List<Cloud.AlterObjStoreInfoRequest> requests = new ArrayList<>();
        new MockUp<MetaServiceProxy>(MetaServiceProxy.class) {
            @Mock
            public Cloud.AlterObjStoreInfoResponse alterStorageVault(Cloud.AlterObjStoreInfoRequest request) {
                requests.add(request);
                return Cloud.AlterObjStoreInfoResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.OK).build())
                        .setStorageVaultId("1")
                        .build();
            }
        };

        Config.cloud_unique_id = "not_empty_nereids";
        String originMetaServiceEndpoint = Config.meta_service_endpoint;
        Config.meta_service_endpoint = "127.0.0.1:20121";
        try {
            // The parser hands the command an immutable map.
            ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
                    .put("type", "S3")
                    .put("s3.endpoint", "s3.us-east-1.amazonaws.com")
                    .put("s3.region", "us-east-1")
                    .put("s3.root.path", "vault_root")
                    .put("s3.bucket", "vault_bucket")
                    .put("s3.access_key", "ak")
                    .put("s3.secret_key", "sk")
                    .put("provider", "S3")
                    .put("s3_validity_check", "false")
                    .put("path_version", "1")
                    .put("shard_num", "1024")
                    .build();
            CreateStorageVaultCommand command = new CreateStorageVaultCommand(false, "s3_sharded", properties);
            Assertions.assertDoesNotThrow(() -> command.validate());
            Assertions.assertEquals(StorageVault.StorageVaultType.S3, command.getVaultType());
            Assertions.assertEquals(1, command.getPathVersion());
            Assertions.assertEquals(1024, command.getNumShard());
            Assertions.assertFalse(command.getProperties().containsKey("path_version"));
            Assertions.assertFalse(command.getProperties().containsKey("shard_num"));

            StorageVault vault = StorageVault.fromCommand(command);
            Assertions.assertEquals(1, vault.getPathVersion());
            Assertions.assertEquals(1024, vault.getNumShard());

            new StorageVaultMgr(new SystemInfoService()).createS3Vault(vault);
            Assertions.assertEquals(1, requests.size());
            Cloud.AlterObjStoreInfoRequest request = requests.get(0);
            Assertions.assertEquals(Cloud.AlterObjStoreInfoRequest.Operation.ADD_S3_VAULT, request.getOp());
            Cloud.StorageVaultPB vaultPB = request.getVault();
            Assertions.assertEquals("s3_sharded", vaultPB.getName());
            Assertions.assertTrue(vaultPB.hasPathFormat());
            Assertions.assertEquals(1, vaultPB.getPathFormat().getPathVersion());
            Assertions.assertEquals(1024, vaultPB.getPathFormat().getShardNum());
            Assertions.assertEquals("vault_bucket", vaultPB.getObjInfo().getBucket());
            Assertions.assertEquals("vault_root", vaultPB.getObjInfo().getPrefix());
        } finally {
            Config.cloud_unique_id = "";
            Config.meta_service_endpoint = originMetaServiceEndpoint;
        }
    }
}
