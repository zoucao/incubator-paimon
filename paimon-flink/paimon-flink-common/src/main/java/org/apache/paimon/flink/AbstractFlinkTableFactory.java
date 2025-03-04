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

package org.apache.paimon.flink;

import org.apache.paimon.CoreOptions.LogChangelogMode;
import org.apache.paimon.CoreOptions.LogConsistency;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.flink.log.LogStoreTableFactory;
import org.apache.paimon.flink.sink.FlinkTableSink;
import org.apache.paimon.flink.source.DataTableSource;
import org.apache.paimon.flink.source.SystemTableSource;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.paimon.CoreOptions.LOG_CHANGELOG_MODE;
import static org.apache.paimon.CoreOptions.LOG_CONSISTENCY;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOG_SYSTEM;
import static org.apache.paimon.flink.FlinkConnectorOptions.NONE;
import static org.apache.paimon.flink.LogicalTypeConversion.toLogicalType;
import static org.apache.paimon.flink.log.LogStoreTableFactory.discoverLogStoreFactory;

/** Abstract paimon factory to create table source and table sink. */
public abstract class AbstractFlinkTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        CatalogTable origin = context.getCatalogTable().getOrigin();
        boolean isStreamingMode =
                context.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                        == RuntimeExecutionMode.STREAMING;
        if (origin instanceof SystemCatalogTable) {
            return new SystemTableSource(((SystemCatalogTable) origin).table(), isStreamingMode);
        } else {
            return new DataTableSource(
                    context.getObjectIdentifier(),
                    buildPaimonTable(context),
                    isStreamingMode,
                    context,
                    createOptionalLogStoreFactory(context).orElse(null));
        }
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        return new FlinkTableSink(
                context.getObjectIdentifier(),
                buildPaimonTable(context),
                context,
                createOptionalLogStoreFactory(context).orElse(null));
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }

    // ~ Tools ------------------------------------------------------------------

    static Optional<LogStoreTableFactory> createOptionalLogStoreFactory(
            DynamicTableFactory.Context context) {
        return createOptionalLogStoreFactory(
                context.getClassLoader(), context.getCatalogTable().getOptions());
    }

    static Optional<LogStoreTableFactory> createOptionalLogStoreFactory(
            ClassLoader classLoader, Map<String, String> options) {
        Options configOptions = new Options();
        options.forEach(configOptions::setString);

        if (configOptions.get(LOG_SYSTEM).equalsIgnoreCase(NONE)) {
            // Use file store continuous reading
            validateFileStoreContinuous(configOptions);
            return Optional.empty();
        }

        return Optional.of(discoverLogStoreFactory(classLoader, configOptions.get(LOG_SYSTEM)));
    }

    private static void validateFileStoreContinuous(Options options) {
        LogChangelogMode changelogMode = options.get(LOG_CHANGELOG_MODE);
        if (changelogMode == LogChangelogMode.UPSERT) {
            throw new ValidationException(
                    "File store continuous reading dose not support upsert changelog mode.");
        }
        LogConsistency consistency = options.get(LOG_CONSISTENCY);
        if (consistency == LogConsistency.EVENTUAL) {
            throw new ValidationException(
                    "File store continuous reading dose not support eventual consistency mode.");
        }
    }

    static CatalogContext createCatalogContext(DynamicTableFactory.Context context) {
        return CatalogContext.create(
                Options.fromMap(context.getCatalogTable().getOptions()), new FlinkFileIOLoader());
    }

    static Table buildPaimonTable(DynamicTableFactory.Context context) {
        CatalogTable origin = context.getCatalogTable().getOrigin();
        Table table;
        if (origin instanceof DataCatalogTable) {
            table = ((DataCatalogTable) origin).table().copy(origin.getOptions());
        } else {
            table = FileStoreTableFactory.create(createCatalogContext(context));
        }

        Schema schema = FlinkCatalog.fromCatalogTable(context.getCatalogTable());

        RowType rowType = toLogicalType(schema.rowType());
        List<String> partitionKeys = schema.partitionKeys();
        List<String> primaryKeys = schema.primaryKeys();

        // compare fields to ignore the outside nullability and nested fields' comments
        Preconditions.checkArgument(
                schemaEquals(toLogicalType(table.rowType()), rowType),
                "Flink schema and store schema are not the same, "
                        + "store schema is %s, Flink schema is %s",
                table.rowType(),
                rowType);

        Preconditions.checkArgument(
                table.partitionKeys().equals(partitionKeys),
                "Flink partitionKeys and store partitionKeys are not the same, "
                        + "store partitionKeys is %s, Flink partitionKeys is %s",
                table.partitionKeys(),
                partitionKeys);

        Preconditions.checkArgument(
                table.primaryKeys().equals(primaryKeys),
                "Flink primaryKeys and store primaryKeys are not the same, "
                        + "store primaryKeys is %s, Flink primaryKeys is %s",
                table.primaryKeys(),
                primaryKeys);

        return table;
    }

    @VisibleForTesting
    static boolean schemaEquals(RowType rowType1, RowType rowType2) {
        List<RowType.RowField> fieldList1 = rowType1.getFields();
        List<RowType.RowField> fieldList2 = rowType2.getFields();
        if (fieldList1.size() != fieldList2.size()) {
            return false;
        }
        for (int i = 0; i < fieldList1.size(); i++) {
            RowType.RowField f1 = fieldList1.get(i);
            RowType.RowField f2 = fieldList2.get(i);
            if (!f1.getName().equals(f2.getName()) || !f1.getType().equals(f2.getType())) {
                return false;
            }
        }
        return true;
    }
}
