/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark;

import org.apache.paimon.fs.Path;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.RegisterExtension;

/** ITCase for using S3 in Spark. */
// @ExtendWith(ParameterizedTestExtension.class)
public class SparkS3ITCase {

    @RegisterExtension
    public static final MinioTestContainer MINIO_CONTAINER = new MinioTestContainer();

    private static SparkSession spark = null;

    @BeforeAll
    public static void startMetastoreAndSpark() {
        // String path = MINIO_CONTAINER.getS3UriForDefaultBucket() + "/" + UUID.randomUUID();
        String path = "file:///Users/zhouchao8/hdd-mac/paimon";
        Path warehousePath = new Path(path);
        spark = SparkSession.builder().master("local[2]").getOrCreate();
        spark.conf().set("spark.sql.catalog.paimon", SparkCatalog.class.getName());
        spark.conf().set("spark.sql.catalog.paimon.warehouse", warehousePath.toString());
        MINIO_CONTAINER
                .getS3ConfigOptions()
                .forEach((k, v) -> spark.conf().set("spark.sql.catalog.paimon." + k, v));
        // spark.sql("CREATE DATABASE default.db");
        spark.sql("USE default.db");
    }

    @AfterAll
    public static void stopMetastoreAndSpark() {
        if (spark != null) {
            spark.stop();
            spark = null;
        }
    }

    //    @Parameters(name = "{0}")
    //    public static Collection<String> parameters() {
    //        return Arrays.asList("avro", "orc", "parquet");
    //    }
    //
    //    private final String format;
    //
    //    public SparkS3ITCase(String format) {
    //        this.format = format;
    //    }

    //    @AfterEach
    //    public void afterEach() {
    //        spark.sql("DROP TABLE T");
    //    }

    @TestTemplate
    public void testWriteRead() {
        //        spark.sql(
        //                String.format(
        //                        "CREATE TABLE T (a INT, b INT, c STRING) TBLPROPERTIES"
        //                                + " ('primary-key'='a', 'bucket'='4',
        // 'file.format'='%s')",
        //                        format));
        //        spark.sql("INSERT INTO T VALUES (1, 2, '3')").collectAsList();
        spark.sql("SELECT * FROM customer").show();
        // assertThat(rows.toString()).isEqualTo("[[1,2,3]]");
    }
}
