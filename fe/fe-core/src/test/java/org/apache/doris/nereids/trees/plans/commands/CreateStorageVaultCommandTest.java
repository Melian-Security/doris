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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    @Test
    public void testRejectInvalidPathFormat(@Mocked AccessControllerManager accessManager) {
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
        try {
            String[][] invalid = {
                    {"1", "0"},       // version 1 divides by shard_num
                    {"1", "-8"},
                    {"1", "65537"},   // above the bound
                    {"1", null},      // version 1 without shard_num
                    {"2", "16"},      // unknown version
                    {"-1", null},
                    {"abc", null},    // not an integer
                    {"1", "1k"},
                    {null, "16"},     // shard_num without version 1
            };
            for (String[] pathFormat : invalid) {
                CreateStorageVaultCommand command =
                        new CreateStorageVaultCommand(false, "s3_invalid", s3Properties(pathFormat[0], pathFormat[1]));
                Assertions.assertThrows(AnalysisException.class, command::validate,
                        "path_version=" + pathFormat[0] + " shard_num=" + pathFormat[1]);
            }

            String[][] valid = {{"1", "1"}, {"1", "65536"}, {"0", null}, {"0", "0"}, {null, null}};
            for (String[] pathFormat : valid) {
                CreateStorageVaultCommand command =
                        new CreateStorageVaultCommand(false, "s3_valid", s3Properties(pathFormat[0], pathFormat[1]));
                Assertions.assertDoesNotThrow(command::validate,
                        "path_version=" + pathFormat[0] + " shard_num=" + pathFormat[1]);
            }
        } finally {
            Config.cloud_unique_id = "";
        }
    }

    @Test
    public void testHdfsVaultWithPathFormat(@Mocked AccessControllerManager accessManager) {
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
        try {
            // Each property alone and both together; the command path precedes type dispatch.
            String[][] cases = {{"1", "64", "1", "64"}, {"0", null, "0", "0"}, {null, null, "0", "0"}};
            for (String[] c : cases) {
                Map<String, String> properties = new HashMap<>();
                properties.put("type", "hdfs");
                properties.put("fs.defaultFS", "hdfs://127.0.0.1:8020");
                if (c[0] != null) {
                    properties.put("path_version", c[0]);
                }
                if (c[1] != null) {
                    properties.put("shard_num", c[1]);
                }
                CreateStorageVaultCommand command =
                        new CreateStorageVaultCommand(false, "hdfs_sharded", ImmutableMap.copyOf(properties));
                Assertions.assertDoesNotThrow(command::validate);
                Assertions.assertEquals(StorageVault.StorageVaultType.HDFS, command.getVaultType());
                Assertions.assertEquals(Integer.parseInt(c[2]), command.getPathVersion());
                Assertions.assertEquals(Integer.parseInt(c[3]), command.getNumShard());
                Assertions.assertFalse(command.getProperties().containsKey("path_version"));
                Assertions.assertFalse(command.getProperties().containsKey("shard_num"));
                Assertions.assertEquals("hdfs://127.0.0.1:8020", command.getProperties().get("fs.defaultFS"));
            }
            Map<String, String> invalid = new HashMap<>();
            invalid.put("type", "hdfs");
            invalid.put("path_version", "1");
            invalid.put("shard_num", "0");
            CreateStorageVaultCommand command =
                    new CreateStorageVaultCommand(false, "hdfs_invalid", ImmutableMap.copyOf(invalid));
            Assertions.assertThrows(AnalysisException.class, command::validate);
        } finally {
            Config.cloud_unique_id = "";
        }
    }

    @Test
    public void testShowStorageVaultPathFormat() {
        Cloud.StorageVaultPB.Builder vault = Cloud.StorageVaultPB.newBuilder()
                .setName("s3_vault")
                .setId("3")
                .setObjInfo(Cloud.ObjectStoreInfoPB.newBuilder().setBucket("b").setPrefix("p")
                        .setUsePathStyle(true));
        String v0 = StorageVault.convertToShowStorageVaultProperties(vault.build()).get(2);
        Assertions.assertEquals("bucket: \"b\" prefix: \"p\" use_path_style: true", v0);

        vault.setPathFormat(Cloud.StorageVaultPB.PathFormat.newBuilder().setPathVersion(0).setShardNum(0));
        Assertions.assertEquals(v0, StorageVault.convertToShowStorageVaultProperties(vault.build()).get(2));

        vault.setPathFormat(Cloud.StorageVaultPB.PathFormat.newBuilder().setPathVersion(1).setShardNum(1024));
        Assertions.assertEquals(v0 + " path_version: 1 shard_num: 1024",
                StorageVault.convertToShowStorageVaultProperties(vault.build()).get(2));
    }

    private static Map<String, String> s3Properties(String pathVersion, String shardNum) {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "S3");
        properties.put("s3.endpoint", "s3.us-east-1.amazonaws.com");
        properties.put("s3.region", "us-east-1");
        properties.put("s3.root.path", "vault_root");
        properties.put("s3.bucket", "vault_bucket");
        properties.put("provider", "S3");
        if (pathVersion != null) {
            properties.put("path_version", pathVersion);
        }
        if (shardNum != null) {
            properties.put("shard_num", shardNum);
        }
        return ImmutableMap.copyOf(properties);
    }
}
