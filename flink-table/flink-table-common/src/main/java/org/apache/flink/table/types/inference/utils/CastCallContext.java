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

package org.apache.flink.table.types.inference.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsAvoidingCast;

/**
 * Helper context that deals with adapted arguments.
 *
 * <p>For example, if an argument needs to be casted to a target type, an expression that was a
 * literal before is not a literal anymore in this call context.
 */
@Internal
public final class CastCallContext implements CallContext {

    private final CallContext delegateContext;

    private final @Nullable DataType outputDataType;

    private List<DataType> adaptedArgumentDataTypes;

    public CastCallContext(CallContext originalContext, @Nullable DataType outputDataType) {
        this.delegateContext = originalContext;
        this.adaptedArgumentDataTypes = originalContext.getArgumentDataTypes();
        this.outputDataType = outputDataType;
    }

    public void setExpectedArguments(List<DataType> expectedArguments) {
        Preconditions.checkArgument(this.adaptedArgumentDataTypes.size() == expectedArguments.size());
        this.adaptedArgumentDataTypes = expectedArguments;
    }

    @Override
    public DataTypeFactory getDataTypeFactory() {
        return delegateContext.getDataTypeFactory();
    }

    @Override
    public FunctionDefinition getFunctionDefinition() {
        return delegateContext.getFunctionDefinition();
    }

    @Override
    public boolean isArgumentLiteral(int pos) {
        if (isCasted(pos)) {
            return false;
        }
        return delegateContext.isArgumentLiteral(pos);
    }

    @Override
    public boolean isArgumentNull(int pos) {
        // null remains null regardless of casting
        return delegateContext.isArgumentNull(pos);
    }

    @Override
    public <T> Optional<T> getArgumentValue(int pos, Class<T> clazz) {
        if (isCasted(pos)) {
            return Optional.empty();
        }
        return delegateContext.getArgumentValue(pos, clazz);
    }

    @Override
    public Optional<TableSemantics> getTableSemantics(int pos) {
        // table arguments remain regardless of casting
        return delegateContext.getTableSemantics(pos);
    }

    @Override
    public String getName() {
        return delegateContext.getName();
    }

    @Override
    public List<DataType> getArgumentDataTypes() {
        return adaptedArgumentDataTypes;
    }

    @Override
    public Optional<DataType> getOutputDataType() {
        return Optional.ofNullable(outputDataType);
    }

    @Override
    public boolean isGroupedAggregation() {
        return delegateContext.isGroupedAggregation();
    }

    private boolean isCasted(int pos) {
        final LogicalType originalType =
                delegateContext.getArgumentDataTypes().get(pos).getLogicalType();
        final LogicalType expectedType = adaptedArgumentDataTypes.get(pos).getLogicalType();
        return !supportsAvoidingCast(originalType, expectedType);
    }
}
