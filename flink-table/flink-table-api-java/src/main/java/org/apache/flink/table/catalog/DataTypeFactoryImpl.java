/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.UnresolvedDataType;
import org.apache.flink.table.types.extraction.DataTypeExtractor;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.UnresolvedUserDefinedType;
import org.apache.flink.table.types.logical.utils.LogicalTypeDuplicator;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.TypeInfoDataTypeConverter;

import javax.annotation.Nullable;

import java.util.function.Supplier;

import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/** Implementation of a {@link DataTypeFactory}. */
@Internal
final class DataTypeFactoryImpl implements DataTypeFactory {

    private final LogicalTypeResolver logicalTypeResolver = new LogicalTypeResolver();

    private final ClassLoader userClassLoader;

    private final Supplier<ExecutionConfig> executionConfigSupplier;

    DataTypeFactoryImpl(
            ClassLoader userClassLoader,
            ReadableConfig tableConfig,
            @Nullable ExecutionConfig parentExecutionConfig) {
        this.userClassLoader = userClassLoader;
        this.executionConfigSupplier =
                createSerializerExecutionConfig(userClassLoader, tableConfig, parentExecutionConfig);
    }

    @Override
    public DataType createDataType(AbstractDataType<?> abstractDataType) {
        if (abstractDataType instanceof DataType) {
            return (DataType) abstractDataType;
        } else if (abstractDataType instanceof UnresolvedDataType) {
            return ((UnresolvedDataType) abstractDataType).toDataType(this);
        }
        throw new ValidationException("Unsupported abstract data type.");
    }

    @Override
    public DataType createDataType(String typeString) {
        return fromLogicalToDataType(createLogicalType(typeString));
    }

    @Override
    public DataType createDataType(UnresolvedIdentifier identifier) {
        return fromLogicalToDataType(createLogicalType(identifier));
    }

    @Override
    public <T> DataType createDataType(Class<T> clazz) {
        return DataTypeExtractor.extractFromType(this, clazz);
    }

    @Override
    public <T> DataType createDataType(TypeInformation<T> typeInfo) {
        return TypeInfoDataTypeConverter.toDataType(this, typeInfo);
    }

    @Override
    public <T> DataType createRawDataType(Class<T> clazz) {
        // we assume that a RAW type is nullable by default
        return DataTypes.RAW(clazz, new KryoSerializer<>(clazz, executionConfigSupplier.get()));
    }

    @Override
    public <T> DataType createRawDataType(TypeInformation<T> typeInfo) {
        // we assume that a RAW type is nullable by default
        return DataTypes.RAW(
                typeInfo.getTypeClass(), typeInfo.createSerializer(executionConfigSupplier.get()));
    }

    @Override
    public LogicalType createLogicalType(String typeString) {
        final LogicalType parsedType = LogicalTypeParser.parse(typeString, userClassLoader);
        return parsedType.accept(logicalTypeResolver);
    }

    @Override
    public LogicalType createLogicalType(UnresolvedIdentifier identifier) {
        if (!identifier.getDatabaseName().isPresent()) {
            return createLogicalType(identifier.getObjectName());
        }
        return resolveType(identifier);
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Creates a lazy {@link ExecutionConfig} that contains options for {@link TypeSerializer}s with
     * information from existing {@link ExecutionConfig} (if available) enriched with table {@link
     * ReadableConfig}.
     */
    private static Supplier<ExecutionConfig> createSerializerExecutionConfig(
            ClassLoader userClassLoader, ReadableConfig tableConfig, ExecutionConfig parentExecutionConfig) {
        return () -> {
            final ExecutionConfig newExecutionConfig = new ExecutionConfig();

            if (parentExecutionConfig != null) {
                if (parentExecutionConfig.isForceKryoEnabled()) {
                    newExecutionConfig.enableForceKryo();
                }

                if (parentExecutionConfig.isForceAvroEnabled()) {
                    newExecutionConfig.enableForceAvro();
                }

                parentExecutionConfig
                        .getDefaultKryoSerializers()
                        .forEach(
                                (c, s) ->
                                        newExecutionConfig.addDefaultKryoSerializer(
                                                c, s.getSerializer()));

                parentExecutionConfig
                        .getDefaultKryoSerializerClasses()
                        .forEach(newExecutionConfig::addDefaultKryoSerializer);

                parentExecutionConfig
                        .getRegisteredKryoTypes()
                        .forEach(newExecutionConfig::registerKryoType);

                parentExecutionConfig
                        .getRegisteredTypesWithKryoSerializerClasses()
                        .forEach(newExecutionConfig::registerTypeWithKryoSerializer);

                parentExecutionConfig
                        .getRegisteredTypesWithKryoSerializers()
                        .forEach(
                                (c, s) ->
                                        newExecutionConfig.registerTypeWithKryoSerializer(
                                                c, s.getSerializer()));
            }

            newExecutionConfig.configure(tableConfig, userClassLoader);

            return newExecutionConfig;
        };
    }

    /** Resolves all {@link UnresolvedUserDefinedType}s. */
    private class LogicalTypeResolver extends LogicalTypeDuplicator {

        @Override
        protected LogicalType defaultMethod(LogicalType logicalType) {
            if (logicalType.is(LogicalTypeRoot.UNRESOLVED)) {
                final UnresolvedUserDefinedType unresolvedType =
                        (UnresolvedUserDefinedType) logicalType;
                return resolveType(unresolvedType.getUnresolvedIdentifier())
                        .copy(unresolvedType.isNullable());
            }
            return logicalType;
        }
    }

    private LogicalType resolveType(UnresolvedIdentifier identifier) {
        assert identifier != null;
        // TODO validate implementation class of structured types when converting from LogicalType
        //  to DataType
        throw new TableException("User-defined types are not supported yet.");
    }
}
