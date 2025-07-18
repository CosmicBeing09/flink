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

package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.JsonExistsOnError;
import org.apache.flink.table.api.JsonQueryOnEmptyOrError;
import org.apache.flink.table.api.JsonQueryWrapper;
import org.apache.flink.table.api.JsonType;
import org.apache.flink.table.api.JsonValueOnEmptyOrError;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.TimeIntervalUnit;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.apache.flink.table.expressions.ApiExpressionUtils.MILLIS_PER_DAY;
import static org.apache.flink.table.expressions.ApiExpressionUtils.MILLIS_PER_HOUR;
import static org.apache.flink.table.expressions.ApiExpressionUtils.MILLIS_PER_MINUTE;
import static org.apache.flink.table.expressions.ApiExpressionUtils.MILLIS_PER_SECOND;
import static org.apache.flink.table.expressions.ApiExpressionUtils.objectToExpression;
import static org.apache.flink.table.expressions.ApiExpressionUtils.tableRef;
import static org.apache.flink.table.expressions.ApiExpressionUtils.toMilliInterval;
import static org.apache.flink.table.expressions.ApiExpressionUtils.toMonthInterval;
import static org.apache.flink.table.expressions.ApiExpressionUtils.typeLiteral;
import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedCall;
import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.ABS;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.ACOS;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.AND;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.ARRAY_AGG;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.ARRAY_APPEND;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.ARRAY_CONCAT;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.ARRAY_CONTAINS;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.ARRAY_DISTINCT;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.ARRAY_ELEMENT;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.ARRAY_EXCEPT;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.ARRAY_MAX;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.ARRAY_MIN;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.ARRAY_POSITION;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.ARRAY_PREPEND;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.ARRAY_REMOVE;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.ARRAY_REVERSE;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.ARRAY_SLICE;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.ARRAY_UNION;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.ASCII;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.ASIN;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.AT;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.ATAN;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.AVG;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.BETWEEN;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.BIN;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.CARDINALITY;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.CAST;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.CEIL;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.CHAR_LENGTH;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.CHR;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.COLLECT;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.COS;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.COSH;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.COT;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.COUNT;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.DECODE;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.DEGREES;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.DISTINCT;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.DIVIDE;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.ENCODE;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.EQUALS;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.EXP;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.EXTRACT;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.FIRST_VALUE;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.FLATTEN;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.FLOOR;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.FROM_BASE64;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.GET;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.GREATER_THAN;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.HEX;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.IF;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.IF_NULL;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.IN;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.INIT_CAP;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.INSTR;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.IS_FALSE;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.IS_JSON;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.IS_NOT_FALSE;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.IS_NOT_NULL;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.IS_NOT_TRUE;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.IS_NULL;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.IS_TRUE;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.JSON_EXISTS;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.JSON_QUERY;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.JSON_VALUE;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.LAST_VALUE;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.LEFT;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.LESS_THAN;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.LIKE;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.LISTAGG;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.LN;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.LOCATE;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.LOG;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.LOG10;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.LOG2;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.LOWER;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.LPAD;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.LTRIM;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.MAP_ENTRIES;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.MAP_KEYS;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.MAP_UNION;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.MAP_VALUES;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.MAX;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.MD5;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.MIN;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.MINUS;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.MOD;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.NOT;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.NOT_BETWEEN;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.NOT_EQUALS;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.OR;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.ORDER_ASC;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.ORDER_DESC;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.OVER;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.OVERLAY;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.PARSE_URL;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.PLUS;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.POSITION;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.POWER;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.PROCTIME;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.RADIANS;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.REGEXP;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.REGEXP_EXTRACT;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.REGEXP_REPLACE;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.REPEAT;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.REPLACE;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.REVERSE;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.RIGHT;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.ROUND;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.ROWTIME;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.RPAD;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.RTRIM;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.SHA1;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.SHA2;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.SHA224;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.SHA256;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.SHA384;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.SHA512;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.SIGN;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.SIMILAR;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.SIN;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.SINH;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.SPLIT_INDEX;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.SQRT;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.STDDEV_POP;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.STDDEV_SAMP;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.STR_TO_MAP;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.SUBSTR;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.SUBSTRING;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.SUM;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.SUM0;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.TAN;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.TANH;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.TIMES;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.TO_BASE64;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.TRIM;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.TRUNCATE;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.TRY_CAST;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.UPPER;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.VAR_POP;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.VAR_SAMP;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.WINDOW_END;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.WINDOW_START;
import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;

/**
 * These are Java and Scala common operations that can be used to construct an {@link Expression}
 * AST for expression operations.
 *
 * @param <InType> The accepted type of input expressions, it is {@code Expression} for Scala and
 *     {@code Object} for Java. Generally the expression DSL works on expressions, the reason why
 *     Java accepts Object is to remove cumbersome call to {@code lit()} for literals. Scala
 *     alleviates this problem via implicit conversions.
 * @param <OutType> The produced type of the DSL. It is {@code ApiExpression} for Java and {@code
 *     Expression} for Scala. In Scala the infix operations are included via implicit conversions.
 *     In Java we introduced a wrapper that enables the operations without pulling them through the
 *     whole stack.
 */
@PublicEvolving
public abstract class BaseExpressions<InType, OutType> {
    protected abstract Expression toExprInternal();

    protected abstract OutType toApiSpecificExpressionInternal(Expression expression);

    /**
     * Specifies a name for an expression i.e. a field.
     *
     * @param name name for one field
     * @param extraNames additional names if the expression expands to multiple fields
     */
    public OutType as(String name, String... extraNames) {
        return toApiSpecificExpressionInternal(
                ApiExpressionUtils.unresolvedCall(
                        BuiltInFunctionDefinitions.AS,
                        Stream.concat(
                                        Stream.of(toExprInternal(), ApiExpressionUtils.valueLiteral(name)),
                                        Stream.of(extraNames).map(ApiExpressionUtils::valueLiteral))
                                .toArray(Expression[]::new)));
    }

    /**
     * Returns an ARRAY that contains the elements from array1 that are not in array2. If no
     * elements remain after excluding the elements in array2 from array1, the function returns an
     * empty ARRAY.
     *
     * <p>If one or both arguments are NULL, the function returns NULL. The order of the elements
     * from array1 is kept.
     */
    public OutType arrayExcept(InType array) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(ARRAY_EXCEPT, toExprInternal(), objectToExpression(array)));
    }

    /**
     * Boolean AND in three-valued logic. This is an infix notation. See also {@link
     * Expressions#and(Object, Object, Object...)} for prefix notation with multiple arguments.
     *
     * @see Expressions#and(Object, Object, Object...)
     */
    public OutType and(InType other) {
        return toApiSpecificExpressionInternal(unresolvedCall(AND, toExprInternal(), objectToExpression(other)));
    }

    /**
     * Boolean OR in three-valued logic. This is an infix notation. See also {@link
     * Expressions#or(Object, Object, Object...)} for prefix notation with multiple arguments.
     *
     * @see Expressions#or(Object, Object, Object...)
     */
    public OutType or(InType other) {
        return toApiSpecificExpressionInternal(unresolvedCall(OR, toExprInternal(), objectToExpression(other)));
    }

    /**
     * Inverts a given boolean expression.
     *
     * <p>This method supports a three-valued logic by preserving {@code NULL}. This means if the
     * input expression is {@code NULL}, the result will also be {@code NULL}.
     *
     * <p>The resulting type is nullable if and only if the input type is nullable.
     *
     * <p>Examples:
     *
     * <pre>{@code
     * lit(true).not() // false
     * lit(false).not() // true
     * lit(null, DataTypes.BOOLEAN()).not() // null
     * }</pre>
     */
    public OutType not() {
        return toApiSpecificExpressionInternal(unresolvedCall(NOT, toExprInternal()));
    }

    /** Greater than. */
    public OutType isGreater(InType other) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(GREATER_THAN, toExprInternal(), objectToExpression(other)));
    }

    /** Greater than or equal. */
    public OutType isGreaterOrEqual(InType other) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(GREATER_THAN_OR_EQUAL, toExprInternal(), objectToExpression(other)));
    }

    /** Less than. */
    public OutType isLess(InType other) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(LESS_THAN, toExprInternal(), objectToExpression(other)));
    }

    /** Less than or equal. */
    public OutType isLessOrEqual(InType other) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(LESS_THAN_OR_EQUAL, toExprInternal(), objectToExpression(other)));
    }

    /** Equals. */
    public OutType isEqual(InType other) {
        return toApiSpecificExpressionInternal(unresolvedCall(EQUALS, toExprInternal(), objectToExpression(other)));
    }

    /** Not equal. */
    public OutType isNotEqual(InType other) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(NOT_EQUALS, toExprInternal(), objectToExpression(other)));
    }

    /** Returns left plus right. */
    public OutType plus(InType other) {
        return toApiSpecificExpressionInternal(unresolvedCall(PLUS, toExprInternal(), objectToExpression(other)));
    }

    /** Returns left minus right. */
    public OutType minus(InType other) {
        return toApiSpecificExpressionInternal(unresolvedCall(MINUS, toExprInternal(), objectToExpression(other)));
    }

    /** Returns left divided by right. */
    public OutType dividedBy(InType other) {
        return toApiSpecificExpressionInternal(unresolvedCall(DIVIDE, toExprInternal(), objectToExpression(other)));
    }

    /** Returns left multiplied by right. */
    public OutType times(InType other) {
        return toApiSpecificExpressionInternal(unresolvedCall(TIMES, toExprInternal(), objectToExpression(other)));
    }

    /**
     * Returns true if the given expression is between lowerBound and upperBound (both inclusive).
     * False otherwise. The parameters must be numeric types or identical comparable types.
     *
     * @param lowerBound numeric or comparable expression
     * @param upperBound numeric or comparable expression
     */
    public OutType between(InType lowerBound, InType upperBound) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(
                        BETWEEN,
                        toExprInternal(),
                        objectToExpression(lowerBound),
                        objectToExpression(upperBound)));
    }

    /**
     * Returns true if the given expression is not between lowerBound and upperBound (both
     * inclusive). False otherwise. The parameters must be numeric types or identical comparable
     * types.
     *
     * @param lowerBound numeric or comparable expression
     * @param upperBound numeric or comparable expression
     */
    public OutType notBetween(InType lowerBound, InType upperBound) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(
                        NOT_BETWEEN,
                        toExprInternal(),
                        objectToExpression(lowerBound),
                        objectToExpression(upperBound)));
    }

    /**
     * Ternary conditional operator that decides which of two other expressions should be evaluated
     * based on a evaluated boolean condition.
     *
     * <p>e.g. lit(42).isGreater(5).then("A", "B") leads to "A"
     *
     * @param ifTrue expression to be evaluated if condition holds
     * @param ifFalse expression to be evaluated if condition does not hold
     */
    public OutType then(InType ifTrue, InType ifFalse) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(
                        IF, toExprInternal(), objectToExpression(ifTrue), objectToExpression(ifFalse)));
    }

    /**
     * Returns {@code nullReplacement} if the given expression is NULL; otherwise the expression is
     * returned.
     *
     * <p>This function returns a data type that is very specific in terms of nullability. The
     * returned type is the common type of both arguments but only nullable if the {@code
     * nullReplacement} is nullable.
     *
     * <p>The function allows to pass nullable columns into a function or table that is declared
     * with a NOT NULL constraint.
     *
     * <p>E.g., <code>$('nullable_column').ifNull(5)</code> returns never NULL.
     */
    public OutType ifNull(InType nullReplacement) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(IF_NULL, toExprInternal(), objectToExpression(nullReplacement)));
    }

    /** Returns true if the given expression is null. */
    public OutType isNull() {
        return toApiSpecificExpressionInternal(unresolvedCall(IS_NULL, toExprInternal()));
    }

    /** Returns true if the given expression is not null. */
    public OutType isNotNull() {
        return toApiSpecificExpressionInternal(unresolvedCall(IS_NOT_NULL, toExprInternal()));
    }

    /** Returns true if given boolean expression is true. False otherwise (for null and false). */
    public OutType isTrue() {
        return toApiSpecificExpressionInternal(unresolvedCall(IS_TRUE, toExprInternal()));
    }

    /** Returns true if given boolean expression is false. False otherwise (for null and true). */
    public OutType isFalse() {
        return toApiSpecificExpressionInternal(unresolvedCall(IS_FALSE, toExprInternal()));
    }

    /**
     * Returns true if given boolean expression is not true (for null and false). False otherwise.
     */
    public OutType isNotTrue() {
        return toApiSpecificExpressionInternal(unresolvedCall(IS_NOT_TRUE, toExprInternal()));
    }

    /**
     * Returns true if given boolean expression is not false (for null and true). False otherwise.
     */
    public OutType isNotFalse() {
        return toApiSpecificExpressionInternal(unresolvedCall(IS_NOT_FALSE, toExprInternal()));
    }

    /**
     * Similar to a SQL distinct aggregation clause such as COUNT(DISTINCT a), declares that an
     * aggregation function is only applied on distinct input values.
     *
     * <p>For example:
     *
     * <pre>{@code
     * orders
     *  .groupBy($("a"))
     *  .select($("a"), $("b").sum().distinct().as("d"))
     * }</pre>
     */
    public OutType distinct() {
        return toApiSpecificExpressionInternal(unresolvedCall(DISTINCT, toExprInternal()));
    }

    /**
     * Returns the sum of the numeric field across all input values. If all values are null, null is
     * returned.
     */
    public OutType sum() {
        return toApiSpecificExpressionInternal(unresolvedCall(SUM, toExprInternal()));
    }

    /**
     * Returns the sum of the numeric field across all input values. If all values are null, 0 is
     * returned.
     */
    public OutType sum0() {
        return toApiSpecificExpressionInternal(unresolvedCall(SUM0, toExprInternal()));
    }

    /** Returns the minimum value of field across all input values. */
    public OutType min() {
        return toApiSpecificExpressionInternal(unresolvedCall(MIN, toExprInternal()));
    }

    /** Returns the maximum value of field across all input values. */
    public OutType max() {
        return toApiSpecificExpressionInternal(unresolvedCall(MAX, toExprInternal()));
    }

    /** Returns the number of input rows for which the field is not null. */
    public OutType count() {
        return toApiSpecificExpressionInternal(unresolvedCall(COUNT, toExprInternal()));
    }

    /** Returns the average (arithmetic mean) of the numeric field across all input values. */
    public OutType avg() {
        return toApiSpecificExpressionInternal(unresolvedCall(AVG, toExprInternal()));
    }

    /** Returns the first value of field across all input values. */
    public OutType firstValue() {
        return toApiSpecificExpressionInternal(unresolvedCall(FIRST_VALUE, toExprInternal()));
    }

    /** Returns the last value of field across all input values. */
    public OutType lastValue() {
        return toApiSpecificExpressionInternal(unresolvedCall(LAST_VALUE, toExprInternal()));
    }

    /**
     * Concatenates the values of string expressions and places separator(,) values between them.
     * The separator is not added at the end of string.
     */
    public OutType listAgg() {
        return toApiSpecificExpressionInternal(unresolvedCall(LISTAGG, toExprInternal(), valueLiteral(",")));
    }

    /**
     * Concatenates the values of string expressions and places separator values between them. The
     * separator is not added at the end of string. The default value of separator is ‘,’.
     *
     * @param separator string containing the character
     */
    public OutType listAgg(String separator) {
        return toApiSpecificExpressionInternal(unresolvedCall(LISTAGG, toExprInternal(), valueLiteral(separator)));
    }

    /** Returns the population standard deviation of an expression (the square root of varPop()). */
    public OutType stddevPop() {
        return toApiSpecificExpressionInternal(unresolvedCall(STDDEV_POP, toExprInternal()));
    }

    /** Returns the sample standard deviation of an expression (the square root of varSamp()). */
    public OutType stddevSamp() {
        return toApiSpecificExpressionInternal(unresolvedCall(STDDEV_SAMP, toExprInternal()));
    }

    /** Returns the population standard variance of an expression. */
    public OutType varPop() {
        return toApiSpecificExpressionInternal(unresolvedCall(VAR_POP, toExprInternal()));
    }

    /** Returns the sample variance of a given expression. */
    public OutType varSamp() {
        return toApiSpecificExpressionInternal(unresolvedCall(VAR_SAMP, toExprInternal()));
    }

    /** Returns multiset aggregate of a given expression. */
    public OutType collect() {
        return toApiSpecificExpressionInternal(unresolvedCall(COLLECT, toExprInternal()));
    }

    /** Returns array aggregate of a given expression. */
    public OutType arrayAgg() {
        return toApiSpecificExpressionInternal(unresolvedCall(ARRAY_AGG, toExprInternal()));
    }

    /**
     * Returns a new value being cast to {@code toType}. A cast error throws an exception and fails
     * the job. When performing a cast operation that may fail, like {@link DataTypes#STRING()} to
     * {@link DataTypes#INT()}, one should rather use {@link #tryCast(DataType)}, in order to handle
     * errors. If {@link ExecutionConfigOptions#TABLE_EXEC_LEGACY_CAST_BEHAVIOUR} is enabled, this
     * function behaves like {@link #tryCast(DataType)}.
     *
     * <p>E.g. {@code "42".cast(DataTypes.INT())} returns {@code 42}; {@code
     * null.cast(DataTypes.STRING())} returns {@code null} of type {@link DataTypes#STRING()};
     * {@code "non-number".cast(DataTypes.INT())} throws an exception and fails the job.
     */
    public OutType cast(DataType toType) {
        return toApiSpecificExpressionInternal(unresolvedCall(CAST, toExprInternal(), typeLiteral(toType)));
    }

    /**
     * Like {@link #cast(DataType)}, but in case of error, returns {@code null} rather than failing
     * the job.
     *
     * <p>E.g. {@code "42".tryCast(DataTypes.INT())} returns {@code 42}; {@code
     * null.tryCast(DataTypes.STRING())} returns {@code null} of type {@link DataTypes#STRING()};
     * {@code "non-number".tryCast(DataTypes.INT())} returns {@code null} of type {@link
     * DataTypes#INT()}; {@code coalesce("non-number".tryCast(DataTypes.INT()), 0)} returns {@code
     * 0} of type {@link DataTypes#INT()}.
     */
    public OutType tryCast(DataType toType) {
        return toApiSpecificExpressionInternal(unresolvedCall(TRY_CAST, toExprInternal(), typeLiteral(toType)));
    }

    /**
     * @deprecated This method will be removed in future versions as it uses the old type system. It
     *     is recommended to use {@link #cast(DataType)} instead which uses the new type system
     *     based on {@link org.apache.flink.table.api.DataTypes}. Please make sure to use either the
     *     old or the new type system consistently to avoid unintended behavior. See the website
     *     documentation for more information.
     */
    @Deprecated
    public OutType cast(TypeInformation<?> toType) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(CAST, toExprInternal(), typeLiteral(fromLegacyInfoToDataType(toType))));
    }

    /** Specifies ascending order of an expression i.e. a field for orderBy unresolvedCall. */
    public OutType asc() {
        return toApiSpecificExpressionInternal(unresolvedCall(ORDER_ASC, toExprInternal()));
    }

    /** Specifies descending order of an expression i.e. a field for orderBy unresolvedCall. */
    public OutType desc() {
        return toApiSpecificExpressionInternal(unresolvedCall(ORDER_DESC, toExprInternal()));
    }

    /**
     * Returns true if an expression exists in a given list of expressions. This is a shorthand for
     * multiple OR conditions.
     *
     * <p>If the testing set contains null, the result will be null if the element can not be found
     * and true if it can be found. If the element is null, the result is always null.
     *
     * <p>e.g. lit("42").in(1, 2, 3) leads to false.
     */
    @SafeVarargs
    public final OutType in(InType... elements) {
        Expression[] args =
                Stream.concat(
                                Stream.of(toExprInternal()),
                                Arrays.stream(elements).map(ApiExpressionUtils::objectToExpression))
                        .toArray(Expression[]::new);
        return toApiSpecificExpressionInternal(unresolvedCall(IN, args));
    }

    /**
     * Returns true if an expression exists in a given table sub-query. The sub-query table must
     * consist of one column. This column must have the same data type as the expression.
     *
     * <p>Note: This operation is not supported in a streaming environment yet.
     */
    public OutType in(Table table) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(IN, toExprInternal(), tableRef(table.toString(), table)));
    }

    /** Returns the start time (inclusive) of a window when applied on a window reference. */
    public OutType start() {
        return toApiSpecificExpressionInternal(unresolvedCall(WINDOW_START, toExprInternal()));
    }

    /**
     * Returns the end time (exclusive) of a window when applied on a window reference.
     *
     * <p>e.g. if a window ends at 10:59:59.999 this property will return 11:00:00.000.
     */
    public OutType end() {
        return toApiSpecificExpressionInternal(unresolvedCall(WINDOW_END, toExprInternal()));
    }

    /** Calculates the remainder of division the given number by another one. */
    public OutType mod(InType other) {
        return toApiSpecificExpressionInternal(unresolvedCall(MOD, toExprInternal(), objectToExpression(other)));
    }

    /** Calculates the Euler's number raised to the given power. */
    public OutType exp() {
        return toApiSpecificExpressionInternal(unresolvedCall(EXP, toExprInternal()));
    }

    /** Calculates the base 10 logarithm of the given value. */
    public OutType log10() {
        return toApiSpecificExpressionInternal(unresolvedCall(LOG10, toExprInternal()));
    }

    /** Calculates the base 2 logarithm of the given value. */
    public OutType log2() {
        return toApiSpecificExpressionInternal(unresolvedCall(LOG2, toExprInternal()));
    }

    /** Calculates the natural logarithm of the given value. */
    public OutType ln() {
        return toApiSpecificExpressionInternal(unresolvedCall(LN, toExprInternal()));
    }

    /** Calculates the natural logarithm of the given value. */
    public OutType log() {
        return toApiSpecificExpressionInternal(unresolvedCall(LOG, toExprInternal()));
    }

    /** Calculates the logarithm of the given value to the given base. */
    public OutType log(InType base) {
        return toApiSpecificExpressionInternal(unresolvedCall(LOG, objectToExpression(base), toExprInternal()));
    }

    /** Calculates the given number raised to the power of the other value. */
    public OutType power(InType other) {
        return toApiSpecificExpressionInternal(unresolvedCall(POWER, toExprInternal(), objectToExpression(other)));
    }

    /** Calculates the hyperbolic cosine of a given value. */
    public OutType cosh() {
        return toApiSpecificExpressionInternal(unresolvedCall(COSH, toExprInternal()));
    }

    /** Calculates the square root of a given value. */
    public OutType sqrt() {
        return toApiSpecificExpressionInternal(unresolvedCall(SQRT, toExprInternal()));
    }

    /** Calculates the absolute value of given value. */
    public OutType abs() {
        return toApiSpecificExpressionInternal(unresolvedCall(ABS, toExprInternal()));
    }

    /** Calculates the largest integer less than or equal to a given number. */
    public OutType floor() {
        return toApiSpecificExpressionInternal(unresolvedCall(FLOOR, toExprInternal()));
    }

    /** Calculates the hyperbolic sine of a given value. */
    public OutType sinh() {
        return toApiSpecificExpressionInternal(unresolvedCall(SINH, toExprInternal()));
    }

    /** Calculates the smallest integer greater than or equal to a given number. */
    public OutType ceil() {
        return toApiSpecificExpressionInternal(unresolvedCall(CEIL, toExprInternal()));
    }

    /** Calculates the sine of a given number. */
    public OutType sin() {
        return toApiSpecificExpressionInternal(unresolvedCall(SIN, toExprInternal()));
    }

    /** Calculates the cosine of a given number. */
    public OutType cos() {
        return toApiSpecificExpressionInternal(unresolvedCall(COS, toExprInternal()));
    }

    /** Calculates the tangent of a given number. */
    public OutType tan() {
        return toApiSpecificExpressionInternal(unresolvedCall(TAN, toExprInternal()));
    }

    /** Calculates the cotangent of a given number. */
    public OutType cot() {
        return toApiSpecificExpressionInternal(unresolvedCall(COT, toExprInternal()));
    }

    /** Calculates the arc sine of a given number. */
    public OutType asin() {
        return toApiSpecificExpressionInternal(unresolvedCall(ASIN, toExprInternal()));
    }

    /** Calculates the arc cosine of a given number. */
    public OutType acos() {
        return toApiSpecificExpressionInternal(unresolvedCall(ACOS, toExprInternal()));
    }

    /** Calculates the arc tangent of a given number. */
    public OutType atan() {
        return toApiSpecificExpressionInternal(unresolvedCall(ATAN, toExprInternal()));
    }

    /** Calculates the hyperbolic tangent of a given number. */
    public OutType tanh() {
        return toApiSpecificExpressionInternal(unresolvedCall(TANH, toExprInternal()));
    }

    /** Converts numeric from radians to degrees. */
    public OutType degrees() {
        return toApiSpecificExpressionInternal(unresolvedCall(DEGREES, toExprInternal()));
    }

    /** Converts numeric from degrees to radians. */
    public OutType radians() {
        return toApiSpecificExpressionInternal(unresolvedCall(RADIANS, toExprInternal()));
    }

    /** Calculates the signum of a given number. */
    public OutType sign() {
        return toApiSpecificExpressionInternal(unresolvedCall(SIGN, toExprInternal()));
    }

    /** Rounds the given number to integer places right to the decimal point. */
    public OutType round(InType places) {
        return toApiSpecificExpressionInternal(unresolvedCall(ROUND, toExprInternal(), objectToExpression(places)));
    }

    /**
     * Returns a string representation of an integer numeric value in binary format. Returns null if
     * numeric is null. E.g. "4" leads to "100", "12" leads to "1100".
     */
    public OutType bin() {
        return toApiSpecificExpressionInternal(unresolvedCall(BIN, toExprInternal()));
    }

    /**
     * Returns a string representation of an integer numeric value or a string in hex format.
     * Returns null if numeric or string is null.
     *
     * <p>E.g. a numeric 20 leads to "14", a numeric 100 leads to "64", and a string "hello,world"
     * leads to "68656c6c6f2c776f726c64".
     */
    public OutType hex() {
        return toApiSpecificExpressionInternal(unresolvedCall(HEX, toExprInternal()));
    }

    /**
     * Returns a number of truncated to n decimal places. If n is 0,the result has no decimal point
     * or fractional part. n can be negative to cause n digits left of the decimal point of the
     * value to become zero. E.g. truncate(42.345, 2) to 42.34.
     */
    public OutType truncate(InType n) {
        return toApiSpecificExpressionInternal(unresolvedCall(TRUNCATE, toExprInternal(), objectToExpression(n)));
    }

    /** Returns a number of truncated to 0 decimal places. E.g. truncate(42.345) to 42.0. */
    public OutType truncate() {
        return toApiSpecificExpressionInternal(unresolvedCall(TRUNCATE, toExprInternal()));
    }

    // String operations

    /**
     * Creates a substring of the given string at given index for a given length.
     *
     * @param beginIndex first character of the substring (starting at 1, inclusive)
     * @param length number of characters of the substring
     */
    public OutType substring(InType beginIndex, InType length) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(
                        SUBSTRING,
                        toExprInternal(),
                        objectToExpression(beginIndex),
                        objectToExpression(length)));
    }

    /**
     * Creates a substring of the given string beginning at the given index to the end.
     *
     * @param beginIndex first character of the substring (starting at 1, inclusive)
     */
    public OutType substring(InType beginIndex) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(SUBSTRING, toExprInternal(), objectToExpression(beginIndex)));
    }

    /**
     * Creates a substring of the given string at given index for a given length.
     *
     * @param beginIndex first character of the substring (starting at 1, inclusive)
     * @param length number of characters of the substring
     */
    public OutType substr(InType beginIndex, InType length) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(
                        SUBSTR,
                        toExprInternal(),
                        objectToExpression(beginIndex),
                        objectToExpression(length)));
    }

    /**
     * Creates a substring of the given string beginning at the given index to the end.
     *
     * @param beginIndex first character of the substring (starting at 1, inclusive)
     */
    public OutType substr(InType beginIndex) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(SUBSTR, toExprInternal(), objectToExpression(beginIndex)));
    }

    /** Removes leading space characters from the given string. */
    public OutType trimLeading() {
        return toApiSpecificExpressionInternal(
                unresolvedCall(
                        TRIM,
                        valueLiteral(true),
                        valueLiteral(false),
                        valueLiteral(" "),
                        toExprInternal()));
    }

    /**
     * Removes leading characters from the given string.
     *
     * @param character string containing the character
     */
    public OutType trimLeading(InType character) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(
                        TRIM,
                        valueLiteral(true),
                        valueLiteral(false),
                        objectToExpression(character),
                        toExprInternal()));
    }

    /** Removes trailing space characters from the given string. */
    public OutType trimTrailing() {
        return toApiSpecificExpressionInternal(
                unresolvedCall(
                        TRIM,
                        valueLiteral(false),
                        valueLiteral(true),
                        valueLiteral(" "),
                        toExprInternal()));
    }

    /**
     * Removes trailing characters from the given string.
     *
     * @param character string containing the character
     */
    public OutType trimTrailing(InType character) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(
                        TRIM,
                        valueLiteral(false),
                        valueLiteral(true),
                        objectToExpression(character),
                        toExprInternal()));
    }

    /** Removes leading and trailing space characters from the given string. */
    public OutType trim() {
        return toApiSpecificExpressionInternal(
                unresolvedCall(
                        TRIM, valueLiteral(true), valueLiteral(true), valueLiteral(" "), toExprInternal()));
    }

    /**
     * Removes leading and trailing characters from the given string.
     *
     * @param character string containing the character
     */
    public OutType trim(InType character) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(
                        TRIM,
                        valueLiteral(true),
                        valueLiteral(true),
                        objectToExpression(character),
                        toExprInternal()));
    }

    /**
     * Returns a new string which replaces all the occurrences of the search target with the
     * replacement string (non-overlapping).
     */
    public OutType replace(InType search, InType replacement) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(
                        REPLACE,
                        toExprInternal(),
                        objectToExpression(search),
                        objectToExpression(replacement)));
    }

    /** Returns the length of a string. */
    public OutType charLength() {
        return toApiSpecificExpressionInternal(unresolvedCall(CHAR_LENGTH, toExprInternal()));
    }

    /**
     * Returns all of the characters in a string in upper case using the rules of the default
     * locale.
     */
    public OutType upperCase() {
        return toApiSpecificExpressionInternal(unresolvedCall(UPPER, toExprInternal()));
    }

    /**
     * Returns all of the characters in a string in lower case using the rules of the default
     * locale.
     */
    public OutType lowerCase() {
        return toApiSpecificExpressionInternal(unresolvedCall(LOWER, toExprInternal()));
    }

    /**
     * Converts the initial letter of each word in a string to uppercase. Assumes a string
     * containing only [A-Za-z0-9], everything else is treated as whitespace.
     */
    public OutType initCap() {
        return toApiSpecificExpressionInternal(unresolvedCall(INIT_CAP, toExprInternal()));
    }

    /**
     * Returns true, if a string matches the specified LIKE pattern.
     *
     * <p>e.g. "Jo_n%" matches all strings that start with "Jo(arbitrary letter)n"
     */
    public OutType like(InType pattern) {
        return toApiSpecificExpressionInternal(unresolvedCall(LIKE, toExprInternal(), objectToExpression(pattern)));
    }

    /**
     * Returns true, if a string matches the specified SQL regex pattern.
     *
     * <p>e.g. "A+" matches all strings that consist of at least one A
     */
    public OutType similar(InType pattern) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(SIMILAR, toExprInternal(), objectToExpression(pattern)));
    }

    /**
     * Returns the position of string in an other string starting at 1. Returns 0 if string could
     * not be found.
     *
     * <p>e.g. lit("a").position("bbbbba") leads to 6
     */
    public OutType position(InType haystack) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(POSITION, toExprInternal(), objectToExpression(haystack)));
    }

    /**
     * Returns a string left-padded with the given pad string to a length of len characters. If the
     * string is longer than len, the return value is shortened to len characters.
     *
     * <p>e.g. lit("hi").lpad(4, "??") returns "??hi", lit("hi").lpad(1, '??') returns "h"
     */
    public OutType lpad(InType len, InType pad) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(LPAD, toExprInternal(), objectToExpression(len), objectToExpression(pad)));
    }

    /**
     * Returns a string right-padded with the given pad string to a length of len characters. If the
     * string is longer than len, the return value is shortened to len characters.
     *
     * <p>e.g. lit("hi").rpad(4, "??") returns "hi??", lit("hi").rpad(1, '??') returns "h"
     */
    public OutType rpad(InType len, InType pad) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(RPAD, toExprInternal(), objectToExpression(len), objectToExpression(pad)));
    }

    /**
     * Defines an aggregation to be used for a previously specified over window.
     *
     * <p>For example:
     *
     * <pre>{@code
     * table
     *   .window(Over partitionBy 'c orderBy 'rowtime preceding 2.rows following CURRENT_ROW as 'w)
     *   .select('c, 'a, 'a.count over 'w, 'a.sum over 'w)
     * }</pre>
     */
    public OutType over(InType alias) {
        return toApiSpecificExpressionInternal(unresolvedCall(OVER, toExprInternal(), objectToExpression(alias)));
    }

    /**
     * Replaces a substring of string with a string starting at a position (starting at 1).
     *
     * <p>e.g. lit("xxxxxtest").overlay("xxxx", 6) leads to "xxxxxxxxx"
     */
    public OutType overlay(InType newString, InType starting) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(
                        OVERLAY,
                        toExprInternal(),
                        objectToExpression(newString),
                        objectToExpression(starting)));
    }

    /**
     * Replaces a substring of string with a string starting at a position (starting at 1). The
     * length specifies how many characters should be removed.
     *
     * <p>e.g. lit("xxxxxtest").overlay("xxxx", 6, 2) leads to "xxxxxxxxxst"
     */
    public OutType overlay(InType newString, InType starting, InType length) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(
                        OVERLAY,
                        toExprInternal(),
                        objectToExpression(newString),
                        objectToExpression(starting),
                        objectToExpression(length)));
    }

    /**
     * Returns TRUE if any (possibly empty) substring matches the Java regular expression, otherwise
     * FALSE. Returns NULL if any of arguments is NULL.
     */
    public OutType regexp(InType regex) {
        return toApiSpecificExpressionInternal(unresolvedCall(REGEXP, toExprInternal(), objectToExpression(regex)));
    }

    /**
     * Returns a string with all substrings that match the regular expression consecutively being
     * replaced.
     */
    public OutType regexpReplace(InType regex, InType replacement) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(
                        REGEXP_REPLACE,
                        toExprInternal(),
                        objectToExpression(regex),
                        objectToExpression(replacement)));
    }

    /**
     * Returns a string extracted with a specified regular expression and a regex match group index.
     */
    public OutType regexpExtract(InType regex, InType extractIndex) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(
                        REGEXP_EXTRACT,
                        toExprInternal(),
                        objectToExpression(regex),
                        objectToExpression(extractIndex)));
    }

    /** Returns a string extracted with a specified regular expression. */
    public OutType regexpExtract(InType regex) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(REGEXP_EXTRACT, toExprInternal(), objectToExpression(regex)));
    }

    /** Returns the base string decoded with base64. */
    public OutType fromBase64() {
        return toApiSpecificExpressionInternal(unresolvedCall(FROM_BASE64, toExprInternal()));
    }

    /** Returns the base64-encoded result of the input string. */
    public OutType toBase64() {
        return toApiSpecificExpressionInternal(unresolvedCall(TO_BASE64, toExprInternal()));
    }

    /** Returns the numeric value of the first character of the input string. */
    public OutType ascii() {
        return toApiSpecificExpressionInternal(unresolvedCall(ASCII, toExprInternal()));
    }

    /** Returns the ASCII character result of the input integer. */
    public OutType chr() {
        return toApiSpecificExpressionInternal(unresolvedCall(CHR, toExprInternal()));
    }

    /** Decodes the first argument into a String using the provided character set. */
    public OutType decode(InType charset) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(DECODE, toExprInternal(), objectToExpression(charset)));
    }

    /** Encodes the string into a BINARY using the provided character set. */
    public OutType encode(InType charset) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(ENCODE, toExprInternal(), objectToExpression(charset)));
    }

    /** Returns the leftmost integer characters from the input string. */
    public OutType left(InType len) {
        return toApiSpecificExpressionInternal(unresolvedCall(LEFT, toExprInternal(), objectToExpression(len)));
    }

    /** Returns the rightmost integer characters from the input string. */
    public OutType right(InType len) {
        return toApiSpecificExpressionInternal(unresolvedCall(RIGHT, toExprInternal(), objectToExpression(len)));
    }

    /** Returns the position of the first occurrence of the input string. */
    public OutType instr(InType str) {
        return toApiSpecificExpressionInternal(unresolvedCall(INSTR, toExprInternal(), objectToExpression(str)));
    }

    /** Returns the position of the first occurrence in the input string. */
    public OutType locate(InType str) {
        return toApiSpecificExpressionInternal(unresolvedCall(LOCATE, toExprInternal(), objectToExpression(str)));
    }

    /** Returns the position of the first occurrence in the input string after position integer. */
    public OutType locate(InType str, InType pos) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(LOCATE, toExprInternal(), objectToExpression(str), objectToExpression(pos)));
    }

    /**
     * Parse url and return various parameter of the URL. If accept any null arguments, return null.
     */
    public OutType parseUrl(InType partToExtract) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(PARSE_URL, toExprInternal(), objectToExpression(partToExtract)));
    }

    /**
     * Parse url and return various parameter of the URL. If accept any null arguments, return null.
     */
    public OutType parseUrl(InType partToExtract, InType key) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(
                        PARSE_URL,
                        toExprInternal(),
                        objectToExpression(partToExtract),
                        objectToExpression(key)));
    }

    /** Returns a string that removes the left whitespaces from the given string. */
    public OutType ltrim() {
        return toApiSpecificExpressionInternal(unresolvedCall(LTRIM, toExprInternal()));
    }

    /** Returns a string that removes the right whitespaces from the given string. */
    public OutType rtrim() {
        return toApiSpecificExpressionInternal(unresolvedCall(RTRIM, toExprInternal()));
    }

    /** Returns a string that repeats the base string n times. */
    public OutType repeat(InType n) {
        return toApiSpecificExpressionInternal(unresolvedCall(REPEAT, toExprInternal(), objectToExpression(n)));
    }

    /**
     * Reverse each character in current string.
     *
     * @return a new string which character order is reverse to current string.
     */
    public OutType reverse() {
        return toApiSpecificExpressionInternal(unresolvedCall(REVERSE, toExprInternal()));
    }

    /**
     * Split target string with custom separator and pick the index-th(start with 0) result.
     *
     * @param separator custom separator.
     * @param index index of the result which you want.
     * @return the string at the index of split results.
     */
    public OutType splitIndex(InType separator, InType index) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(
                        SPLIT_INDEX,
                        toExprInternal(),
                        objectToExpression(separator),
                        objectToExpression(index)));
    }

    /**
     * Creates a map by parsing text. Split text into key-value pairs using two delimiters. The
     * first delimiter separates pairs, and the second delimiter separates key and value. If only
     * one parameter is given, default delimiters are used: ',' as delimiter1 and '=' as delimiter2.
     * Both delimiters are treated as regular expressions.
     *
     * @return the map
     */
    public OutType strToMap() {
        return toApiSpecificExpressionInternal(unresolvedCall(STR_TO_MAP, toExprInternal()));
    }

    /**
     * Creates a map by parsing text. Split text into key-value pairs using two delimiters. The
     * first delimiter separates pairs, and the second delimiter separates key and value. Both
     * {@code listDelimiter} and {@code keyValueDelimiter} are treated as regular expressions.
     *
     * @param listDelimiter the delimiter to separates pairs
     * @param keyValueDelimiter the delimiter to separates key and value
     * @return the map
     */
    public OutType strToMap(InType listDelimiter, InType keyValueDelimiter) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(
                        STR_TO_MAP,
                        toExprInternal(),
                        objectToExpression(listDelimiter),
                        objectToExpression(keyValueDelimiter)));
    }

    // Temporal operations

    /** Parses a date string in the form "yyyy-MM-dd" to a SQL Date. */
    public OutType toDate() {
        return toApiSpecificExpressionInternal(
                unresolvedCall(
                        CAST,
                        toExprInternal(),
                        typeLiteral(fromLegacyInfoToDataType(SqlTimeTypeInfo.DATE))));
    }

    /** Parses a time string in the form "HH:mm:ss" to a SQL Time. */
    public OutType toTime() {
        return toApiSpecificExpressionInternal(
                unresolvedCall(
                        CAST,
                        toExprInternal(),
                        typeLiteral(fromLegacyInfoToDataType(SqlTimeTypeInfo.TIME))));
    }

    /** Parses a timestamp string in the form "yyyy-MM-dd HH:mm:ss[.SSS]" to a SQL Timestamp. */
    public OutType toTimestamp() {
        return toApiSpecificExpressionInternal(
                unresolvedCall(
                        CAST,
                        toExprInternal(),
                        typeLiteral(fromLegacyInfoToDataType(SqlTimeTypeInfo.TIMESTAMP))));
    }

    /**
     * Extracts parts of a time point or time interval. Returns the part as a long value.
     *
     * <p>e.g. lit("2006-06-05").toDate().extract(DAY) leads to 5
     */
    public OutType extract(TimeIntervalUnit timeIntervalUnit) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(EXTRACT, valueLiteral(timeIntervalUnit), toExprInternal()));
    }

    /**
     * Rounds down a time point to the given unit.
     *
     * <p>e.g. lit("12:44:31").toDate().floor(MINUTE) leads to 12:44:00
     */
    public OutType floor(TimeIntervalUnit timeIntervalUnit) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(FLOOR, toExprInternal(), valueLiteral(timeIntervalUnit)));
    }

    /**
     * Rounds up a time point to the given unit.
     *
     * <p>e.g. lit("12:44:31").toDate().ceil(MINUTE) leads to 12:45:00
     */
    public OutType ceil(TimeIntervalUnit timeIntervalUnit) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(CEIL, toExprInternal(), valueLiteral(timeIntervalUnit)));
    }

    // Advanced type helper functions

    /**
     * Accesses the field of a Flink composite type (such as Tuple, POJO, etc.) by name and returns
     * it's value.
     *
     * @param name name of the field (similar to Flink's field expressions)
     */
    public OutType get(String name) {
        return toApiSpecificExpressionInternal(unresolvedCall(GET, toExprInternal(), valueLiteral(name)));
    }

    /**
     * Accesses the field of a Flink composite type (such as Tuple, POJO, etc.) by index and returns
     * it's value.
     *
     * @param index position of the field
     */
    public OutType get(int index) {
        return toApiSpecificExpressionInternal(unresolvedCall(GET, toExprInternal(), valueLiteral(index)));
    }

    /**
     * Converts a Flink composite type (such as Tuple, POJO, etc.) and all of its direct subtypes
     * into a flat representation where every subtype is a separate field.
     */
    public OutType flatten() {
        return toApiSpecificExpressionInternal(unresolvedCall(FLATTEN, toExprInternal()));
    }

    /**
     * Accesses the element of an array or map based on a key or an index (starting at 1).
     *
     * @param index key or position of the element (array index starting at 1)
     */
    public OutType at(InType index) {
        return toApiSpecificExpressionInternal(unresolvedCall(AT, toExprInternal(), objectToExpression(index)));
    }

    /** Returns the number of elements of an array or number of entries of a map. */
    public OutType cardinality() {
        return toApiSpecificExpressionInternal(unresolvedCall(CARDINALITY, toExprInternal()));
    }

    /**
     * Returns the sole element of an array with a single element. Returns null if the array is
     * empty. Throws an exception if the array has more than one element.
     */
    public OutType element() {
        return toApiSpecificExpressionInternal(unresolvedCall(ARRAY_ELEMENT, toExprInternal()));
    }

    /**
     * Appends an element to the end of the array and returns the result.
     *
     * <p>If the array itself is null, the function will return null. If an element to add is null,
     * the null element will be added to the end of the array. The given element is cast implicitly
     * to the array's element type if necessary.
     */
    public OutType arrayAppend(InType element) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(ARRAY_APPEND, toExprInternal(), objectToExpression(element)));
    }

    /**
     * Returns whether the given element exists in an array.
     *
     * <p>Checking for null elements in the array is supported. If the array itself is null, the
     * function will return null. The given element is cast implicitly to the array's element type
     * if necessary.
     */
    public OutType arrayContains(InType needle) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(ARRAY_CONTAINS, toExprInternal(), objectToExpression(needle)));
    }

    /**
     * Returns an array with unique elements.
     *
     * <p>If the array itself is null, the function will return null. Keeps ordering of elements.
     */
    public OutType arrayDistinct() {
        return toApiSpecificExpressionInternal(unresolvedCall(ARRAY_DISTINCT, toExprInternal()));
    }

    /**
     * Returns the position of the first occurrence of element in the given array as int. Returns 0
     * if the given value could not be found in the array. Returns null if either of the arguments
     * are null
     *
     * <p>NOTE: that this is not zero based, but 1-based index. The first element in the array has
     * index 1.
     */
    public OutType arrayPosition(InType needle) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(ARRAY_POSITION, toExprInternal(), objectToExpression(needle)));
    }

    /**
     * Appends an element to the beginning of the array and returns the result.
     *
     * <p>If the array itself is null, the function will return null. If an element to add is null,
     * the null element will be added to the beginning of the array. The given element is cast
     * implicitly to the array's element type if necessary.
     */
    public OutType arrayPrepend(InType element) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(ARRAY_PREPEND, toExprInternal(), objectToExpression(element)));
    }

    /**
     * Removes all elements that equal to element from array.
     *
     * <p>If the array itself is null, the function will return null. Keeps ordering of elements.
     */
    public OutType arrayRemove(InType needle) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(ARRAY_REMOVE, toExprInternal(), objectToExpression(needle)));
    }

    /**
     * Returns an array in reverse order.
     *
     * <p>If the array itself is null, the function will return null.
     */
    public OutType arrayReverse() {
        return toApiSpecificExpressionInternal(unresolvedCall(ARRAY_REVERSE, toExprInternal()));
    }

    /**
     * Returns a subarray of the input array between 'start_offset' and 'end_offset' inclusive. The
     * offsets are 1-based however 0 is also treated as the beginning of the array. Positive values
     * are counted from the beginning of the array while negative from the end. If 'end_offset' is
     * omitted then this offset is treated as the length of the array. If 'start_offset' is after
     * 'end_offset' or both are out of array bounds an empty array will be returned.
     *
     * <p>Returns null if any input is null.
     */
    public OutType arraySlice(InType startOffset, InType endOffset) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(
                        ARRAY_SLICE,
                        toExprInternal(),
                        objectToExpression(startOffset),
                        objectToExpression(endOffset)));
    }

    public OutType arraySlice(InType startOffset) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(ARRAY_SLICE, toExprInternal(), objectToExpression(startOffset)));
    }

    /**
     * Returns an array of the elements in the union of array1 and array2, without duplicates.
     *
     * <p>If any of the array is null, the function will return null.
     */
    public OutType arrayUnion(InType array) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(ARRAY_UNION, toExprInternal(), objectToExpression(array)));
    }

    /**
     * Returns an array that is the result of concatenating at least one array. This array contains
     * all the elements in the first array, followed by all the elements in the second array, and so
     * forth, up to the Nth array.
     *
     * <p>If any input array is NULL, the function returns NULL.
     */
    public OutType arrayConcat(InType... arrays) {
        arrays = convertToArrays(arrays);
        Expression[] args =
                Stream.concat(
                                Stream.of(toExprInternal()),
                                Arrays.stream(arrays).map(ApiExpressionUtils::objectToExpression))
                        .toArray(Expression[]::new);
        return toApiSpecificExpressionInternal(unresolvedCall(ARRAY_CONCAT, args));
    }

    private InType[] convertToArrays(InType[] arrays) {
        if (arrays == null || arrays.length == 0) {
            return arrays;
        }
        InType notNullArray = null;
        for (int i = 0; i < arrays.length; ++i) {
            if (arrays[i] != null) {
                notNullArray = arrays[i];
            }
        }
        if (!(notNullArray instanceof Object[])) {
            return (InType[]) new Object[] {arrays};
        } else {
            return arrays;
        }
    }

    /**
     * Returns the maximum value from the array.
     *
     * <p>if array itself is null, the function returns null.
     */
    public OutType arrayMax() {
        return toApiSpecificExpressionInternal(unresolvedCall(ARRAY_MAX, toExprInternal()));
    }

    /**
     * Returns the minimum value from the array.
     *
     * <p>if array itself is null, the function returns null.
     */
    public OutType arrayMin() {
        return toApiSpecificExpressionInternal(unresolvedCall(ARRAY_MIN, toExprInternal()));
    }

    /** Returns the keys of the map as an array. */
    public OutType mapKeys() {
        return toApiSpecificExpressionInternal(unresolvedCall(MAP_KEYS, toExprInternal()));
    }

    /** Returns the values of the map as an array. */
    public OutType mapValues() {
        return toApiSpecificExpressionInternal(unresolvedCall(MAP_VALUES, toExprInternal()));
    }

    /** Returns an array of all entries in the given map. */
    public OutType mapEntries() {
        return toApiSpecificExpressionInternal(unresolvedCall(MAP_ENTRIES, toExprInternal()));
    }

    /**
     * Returns a map created by merging at least one map. These maps should have a common map type.
     * If there are overlapping keys, the value from 'map2' will overwrite the value from 'map1',
     * the value from 'map3' will overwrite the value from 'map2', the value from 'mapn' will
     * overwrite the value from 'map(n-1)'. If any of maps is null, return null.
     */
    public OutType mapUnion(InType... inputs) {
        Expression[] args =
                Stream.concat(
                                Stream.of(toExprInternal()),
                                Arrays.stream(inputs).map(ApiExpressionUtils::objectToExpression))
                        .toArray(Expression[]::new);
        return toApiSpecificExpressionInternal(unresolvedCall(MAP_UNION, args));
    }

    // Time definition

    /**
     * Declares a field as the rowtime attribute for indicating, accessing, and working in Flink's
     * event time.
     */
    public OutType rowtime() {
        return toApiSpecificExpressionInternal(unresolvedCall(ROWTIME, toExprInternal()));
    }

    /**
     * Declares a field as the proctime attribute for indicating, accessing, and working in Flink's
     * processing time.
     */
    public OutType proctime() {
        return toApiSpecificExpressionInternal(unresolvedCall(PROCTIME, toExprInternal()));
    }

    /**
     * Creates an interval of the given number of years.
     *
     * <p>The produced expression is of type {@code DataTypes.INTERVAL}
     */
    public OutType year() {
        return toApiSpecificExpressionInternal(toMonthInterval(toExprInternal(), 12));
    }

    /** Creates an interval of the given number of years. */
    public OutType years() {
        return year();
    }

    /** Creates an interval of the given number of quarters. */
    public OutType quarter() {
        return toApiSpecificExpressionInternal(toMonthInterval(toExprInternal(), 3));
    }

    /** Creates an interval of the given number of quarters. */
    public OutType quarters() {
        return quarter();
    }

    /** Creates an interval of the given number of months. */
    public OutType month() {
        return toApiSpecificExpressionInternal(toMonthInterval(toExprInternal(), 1));
    }

    /** Creates an interval of the given number of months. */
    public OutType months() {
        return month();
    }

    /** Creates an interval of the given number of weeks. */
    public OutType week() {
        return toApiSpecificExpressionInternal(toMilliInterval(toExprInternal(), 7 * MILLIS_PER_DAY));
    }

    /** Creates an interval of the given number of weeks. */
    public OutType weeks() {
        return week();
    }

    /** Creates an interval of the given number of days. */
    public OutType day() {
        return toApiSpecificExpressionInternal(toMilliInterval(toExprInternal(), MILLIS_PER_DAY));
    }

    /** Creates an interval of the given number of days. */
    public OutType days() {
        return day();
    }

    /** Creates an interval of the given number of hours. */
    public OutType hour() {
        return toApiSpecificExpressionInternal(toMilliInterval(toExprInternal(), MILLIS_PER_HOUR));
    }

    /** Creates an interval of the given number of hours. */
    public OutType hours() {
        return hour();
    }

    /** Creates an interval of the given number of minutes. */
    public OutType minute() {
        return toApiSpecificExpressionInternal(toMilliInterval(toExprInternal(), MILLIS_PER_MINUTE));
    }

    /** Creates an interval of the given number of minutes. */
    public OutType minutes() {
        return minute();
    }

    /** Creates an interval of the given number of seconds. */
    public OutType second() {
        return toApiSpecificExpressionInternal(toMilliInterval(toExprInternal(), MILLIS_PER_SECOND));
    }

    /** Creates an interval of the given number of seconds. */
    public OutType seconds() {
        return second();
    }

    /** Creates an interval of the given number of milliseconds. */
    public OutType milli() {
        return toApiSpecificExpressionInternal(toMilliInterval(toExprInternal(), 1));
    }

    /** Creates an interval of the given number of milliseconds. */
    public OutType millis() {
        return milli();
    }

    // Hash functions

    /**
     * Returns the MD5 hash of the string argument; null if string is null.
     *
     * @return string of 32 hexadecimal digits or null
     */
    public OutType md5() {
        return toApiSpecificExpressionInternal(unresolvedCall(MD5, toExprInternal()));
    }

    /**
     * Returns the SHA-1 hash of the string argument; null if string is null.
     *
     * @return string of 40 hexadecimal digits or null
     */
    public OutType sha1() {
        return toApiSpecificExpressionInternal(unresolvedCall(SHA1, toExprInternal()));
    }

    /**
     * Returns the SHA-224 hash of the string argument; null if string is null.
     *
     * @return string of 56 hexadecimal digits or null
     */
    public OutType sha224() {
        return toApiSpecificExpressionInternal(unresolvedCall(SHA224, toExprInternal()));
    }

    /**
     * Returns the SHA-256 hash of the string argument; null if string is null.
     *
     * @return string of 64 hexadecimal digits or null
     */
    public OutType sha256() {
        return toApiSpecificExpressionInternal(unresolvedCall(SHA256, toExprInternal()));
    }

    /**
     * Returns the SHA-384 hash of the string argument; null if string is null.
     *
     * @return string of 96 hexadecimal digits or null
     */
    public OutType sha384() {
        return toApiSpecificExpressionInternal(unresolvedCall(SHA384, toExprInternal()));
    }

    /**
     * Returns the SHA-512 hash of the string argument; null if string is null.
     *
     * @return string of 128 hexadecimal digits or null
     */
    public OutType sha512() {
        return toApiSpecificExpressionInternal(unresolvedCall(SHA512, toExprInternal()));
    }

    /**
     * Returns the hash for the given string expression using the SHA-2 family of hash functions
     * (SHA-224, SHA-256, SHA-384, or SHA-512).
     *
     * @param hashLength bit length of the result (either 224, 256, 384, or 512)
     * @return string or null if one of the arguments is null.
     */
    public OutType sha2(InType hashLength) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(SHA2, toExprInternal(), objectToExpression(hashLength)));
    }

    // JSON functions

    /**
     * Determine whether a given string is valid JSON.
     *
     * <p>Specifying the optional {@param type} argument puts a constraint on which type of JSON
     * object is allowed. If the string is valid JSON, but not that type, {@code false} is returned.
     * The default is {@link JsonType#VALUE}.
     *
     * <p>Examples:
     *
     * <pre>{@code
     * lit("1").isJson() // true
     * lit("[]").isJson() // true
     * lit("{}").isJson() // true
     *
     * lit("\"abc\"").isJson() // true
     * lit("abc").isJson() // false
     * nullOf(DataTypes.STRING()).isJson() // false
     *
     * lit("1").isJson(JsonType.SCALAR) // true
     * lit("1").isJson(JsonType.ARRAY) // false
     * lit("1").isJson(JsonType.OBJECT) // false
     *
     * lit("{}").isJson(JsonType.SCALAR) // false
     * lit("{}").isJson(JsonType.ARRAY) // false
     * lit("{}").isJson(JsonType.OBJECT) // true
     * }</pre>
     *
     * @param type The type of JSON object to validate against.
     * @return {@code true} if the string is a valid JSON of the given {@param type}, {@code false}
     *     otherwise.
     */
    public OutType isJson(JsonType type) {
        return toApiSpecificExpressionInternal(unresolvedCall(IS_JSON, toExprInternal(), valueLiteral(type)));
    }

    /**
     * Determine whether a given string is valid JSON.
     *
     * <p>This is a shortcut for {@code isJson(JsonType.VALUE)}. See {@link #isJson(JsonType)}.
     *
     * @return {@code true} if the string is a valid JSON value, {@code false} otherwise.
     */
    public OutType isJson() {
        return toApiSpecificExpressionInternal(unresolvedCall(IS_JSON, toExprInternal()));
    }

    /**
     * Returns whether a JSON string satisfies a given search criterion.
     *
     * <p>This follows the ISO/IEC TR 19075-6 specification for JSON support in SQL.
     *
     * <p>Examples:
     *
     * <pre>{@code
     * // true
     * lit("{\"a\": true}").jsonExists("$.a")
     * // false
     * lit("{\"a\": true}").jsonExists("$.b")
     * // true
     * lit("{\"a\": [{ \"b\": 1 }]}").jsonExists("$.a[0].b")
     *
     * // true
     * lit("{\"a\": true}").jsonExists("strict $.b", JsonExistsOnError.TRUE)
     * // false
     * lit("{\"a\": true}").jsonExists("strict $.b", JsonExistsOnError.FALSE)
     * }</pre>
     *
     * @param path JSON path to search for.
     * @param onError Behavior in case of an error.
     * @return {@code true} if the JSON string satisfies the search criterion.
     */
    public OutType jsonExists(String path, JsonExistsOnError onError) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(JSON_EXISTS, toExprInternal(), valueLiteral(path), valueLiteral(onError)));
    }

    /**
     * Determines whether a JSON string satisfies a given search criterion.
     *
     * <p>This follows the ISO/IEC TR 19075-6 specification for JSON support in SQL.
     *
     * <p>Examples:
     *
     * <pre>{@code
     * // true
     * lit("{\"a\": true}").jsonExists("$.a")
     * // false
     * lit("{\"a\": true}").jsonExists("$.b")
     * // true
     * lit("{\"a\": [{ \"b\": 1 }]}").jsonExists("$.a[0].b")
     *
     * // true
     * lit("{\"a\": true}").jsonExists("strict $.b", JsonExistsOnError.TRUE)
     * // false
     * lit("{\"a\": true}").jsonExists("strict $.b", JsonExistsOnError.FALSE)
     * }</pre>
     *
     * @param path JSON path to search for.
     * @return {@code true} if the JSON string satisfies the search criterion.
     */
    public OutType jsonExists(String path) {
        return toApiSpecificExpressionInternal(unresolvedCall(JSON_EXISTS, toExprInternal(), valueLiteral(path)));
    }

    /**
     * Extracts a scalar from a JSON string.
     *
     * <p>This method searches a JSON string for a given path expression and returns the value if
     * the value at that path is scalar. Non-scalar values cannot be returned. By default, the value
     * is returned as {@link DataTypes#STRING()}. Using {@param returningType} a different type can
     * be chosen, with the following types being supported:
     *
     * <ul>
     *   <li>{@link DataTypes#STRING()}
     *   <li>{@link DataTypes#BOOLEAN()}
     *   <li>{@link DataTypes#INT()}
     *   <li>{@link DataTypes#DOUBLE()}
     * </ul>
     *
     * <p>For empty path expressions or errors a behavior can be defined to either return {@code
     * null}, raise an error or return a defined default value instead.
     *
     * <p>See {@link #jsonQuery(String, JsonQueryWrapper, JsonQueryOnEmptyOrError,
     * JsonQueryOnEmptyOrError)} for extracting non-scalar values from a JSON string.
     *
     * <p>Examples:
     *
     * <pre>{@code
     * // STRING: "true"
     * lit("{\"a\": true}").jsonValue("$.a")
     *
     * // DOUBLE: 0.998
     * lit("{\"a.b\": [0.998,0.996]}").jsonValue("$.['a.b'][0]", DataTypes.DOUBLE())
     *
     * // BOOLEAN: true
     * lit("{\"a\": true}").jsonValue("$.a", DataTypes.BOOLEAN())
     *
     * // BOOLEAN: "false"
     * lit("{\"a\": true}").jsonValue("lax $.b",
     *     JsonValueOnEmptyOrError.DEFAULT, false, JsonValueOnEmptyOrError.NULL, null)
     *
     * // BOOLEAN: "false"
     * lit("{\"a\": true}").jsonValue("strict $.b",
     *     JsonValueOnEmptyOrError.NULL, null, JsonValueOnEmptyOrError.DEFAULT, false)
     * }</pre>
     *
     * @param path JSON path to extract.
     * @param returningType Type to convert the extracted scalar to, otherwise defaults to {@link
     *     DataTypes#STRING()}.
     * @param onEmpty Behavior in case the path expression is empty.
     * @param defaultOnEmpty Default value to return if the path expression is empty and {@param
     *     onEmpty} is set to {@link JsonValueOnEmptyOrError#DEFAULT}.
     * @param onError Behavior in case of an error.
     * @param defaultOnError Default value to return if there is an error and {@param onError} is
     *     set to {@link JsonValueOnEmptyOrError#DEFAULT}.
     * @return The extracted scalar value.
     */
    public OutType jsonValue(
            String path,
            DataType returningType,
            JsonValueOnEmptyOrError onEmpty,
            InType defaultOnEmpty,
            JsonValueOnEmptyOrError onError,
            InType defaultOnError) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(
                        JSON_VALUE,
                        toExprInternal(),
                        valueLiteral(path),
                        typeLiteral(returningType),
                        valueLiteral(onEmpty),
                        objectToExpression(defaultOnEmpty),
                        valueLiteral(onError),
                        objectToExpression(defaultOnError)));
    }

    /**
     * Extracts a scalar from a JSON string.
     *
     * <p>This method searches a JSON string for a given path expression and returns the value if
     * the value at that path is scalar. Non-scalar values cannot be returned. By default, the value
     * is returned as {@link DataTypes#STRING()}.
     *
     * <p>See also {@link #jsonValue(String, DataType, JsonValueOnEmptyOrError, Object,
     * JsonValueOnEmptyOrError, Object)}.
     *
     * @param path JSON path to extract.
     * @param returningType Type to convert the extracted scalar to, otherwise defaults to {@link
     *     DataTypes#STRING()}.
     * @return The extracted scalar value.
     */
    public OutType jsonValue(String path, DataType returningType) {
        return jsonValue(
                path,
                returningType,
                JsonValueOnEmptyOrError.NULL,
                null,
                JsonValueOnEmptyOrError.NULL,
                null);
    }

    /**
     * Extracts a scalar from a JSON string.
     *
     * <p>This method searches a JSON string for a given path expression and returns the value if
     * the value at that path is scalar. Non-scalar values cannot be returned. By default, the value
     * is returned as {@link DataTypes#STRING()}.
     *
     * <p>See also {@link #jsonValue(String, DataType, JsonValueOnEmptyOrError, Object,
     * JsonValueOnEmptyOrError, Object)}.
     *
     * <p>This is a convenience method using {@link JsonValueOnEmptyOrError#DEFAULT} for both empty
     * and error cases with the same default value.
     *
     * @param path JSON path to extract.
     * @param returningType Type to convert the extracted scalar to, otherwise defaults to {@link
     *     DataTypes#STRING()}.
     * @return The extracted scalar value.
     */
    public OutType jsonValue(String path, DataType returningType, InType defaultOnEmptyOrError) {
        return jsonValue(
                path,
                returningType,
                JsonValueOnEmptyOrError.DEFAULT,
                defaultOnEmptyOrError,
                JsonValueOnEmptyOrError.DEFAULT,
                defaultOnEmptyOrError);
    }

    /**
     * Extracts a scalar from a JSON string.
     *
     * <p>This method searches a JSON string for a given path expression and returns the value if
     * the value at that path is scalar. Non-scalar values cannot be returned. By default, the value
     * is returned as {@link DataTypes#STRING()}.
     *
     * <p>See also {@link #jsonValue(String, DataType, JsonValueOnEmptyOrError, Object,
     * JsonValueOnEmptyOrError, Object)}.
     *
     * @param path JSON path to extract.
     * @return The extracted scalar value.
     */
    public OutType jsonValue(String path) {
        return jsonValue(path, DataTypes.STRING());
    }

    /**
     * Extracts JSON values from a JSON string.
     *
     * <p>This follows the ISO/IEC TR 19075-6 specification for JSON support in SQL. The result is
     * always returned as a {@link DataTypes#STRING()}.
     *
     * <p>The {@param wrappingBehavior} determines whether the extracted value should be wrapped
     * into an array, and whether to do so unconditionally or only if the value itself isn't an
     * array already.
     *
     * <p>{@param onEmpty} and {@param onError} determine the behavior in case the path expression
     * is empty, or in case an error was raised, respectively. By default, in both cases {@code
     * null} is returned. Other choices are to use an empty array, an empty object, or to raise an
     * error.
     *
     * <p>See {@link #jsonValue(String, DataType, JsonValueOnEmptyOrError, Object,
     * JsonValueOnEmptyOrError, Object)} for extracting scalars from a JSON string.
     *
     * <p>Examples:
     *
     * <pre>{@code
     * lit("{ \"a\": { \"b\": 1 } }").jsonQuery("$.a") // "{ \"b\": 1 }"
     * lit("[1, 2]").jsonQuery("$") // "[1, 2]"
     * nullOf(DataTypes.STRING()).jsonQuery("$") // null
     *
     * // Wrap result into an array
     * lit("{}").jsonQuery("$", JsonQueryWrapper.CONDITIONAL_ARRAY) // "[{}]"
     * lit("[1, 2]").jsonQuery("$", JsonQueryWrapper.CONDITIONAL_ARRAY) // "[1, 2]"
     * lit("[1, 2]").jsonQuery("$", JsonQueryWrapper.UNCONDITIONAL_ARRAY) // "[[1, 2]]"
     *
     * // Scalars must be wrapped to be returned
     * lit(1).jsonQuery("$") // null
     * lit(1).jsonQuery("$", JsonQueryWrapper.CONDITIONAL_ARRAY) // "[1]"
     *
     * // Behavior if path expression is empty / there is an error
     * // "{}"
     * lit("{}").jsonQuery("lax $.invalid", JsonQueryWrapper.WITHOUT_ARRAY,
     *     JsonQueryOnEmptyOrError.EMPTY_OBJECT, JsonQueryOnEmptyOrError.NULL)
     * // "[]"
     * lit("{}").jsonQuery("strict $.invalid", JsonQueryWrapper.WITHOUT_ARRAY,
     *     JsonQueryOnEmptyOrError.NULL, JsonQueryOnEmptyOrError.EMPTY_ARRAY)
     * }</pre>
     *
     * @param path JSON path to search for.
     * @param wrappingBehavior Determine if and when to wrap the resulting value into an array.
     * @param onEmpty Behavior in case the path expression is empty.
     * @param onError Behavior in case of an error.
     * @return The extracted JSON value.
     */
    public OutType jsonQuery(
            String path,
            JsonQueryWrapper wrappingBehavior,
            JsonQueryOnEmptyOrError onEmpty,
            JsonQueryOnEmptyOrError onError) {
        return toApiSpecificExpressionInternal(
                unresolvedCall(
                        JSON_QUERY,
                        toExprInternal(),
                        valueLiteral(path),
                        valueLiteral(wrappingBehavior),
                        valueLiteral(onEmpty),
                        valueLiteral(onError)));
    }

    /**
     * Extracts JSON values from a JSON string.
     *
     * <p>The {@param wrappingBehavior} determines whether the extracted value should be wrapped
     * into an array, and whether to do so unconditionally or only if the value itself isn't an
     * array already.
     *
     * <p>See also {@link #jsonQuery(String, JsonQueryWrapper, JsonQueryOnEmptyOrError,
     * JsonQueryOnEmptyOrError)}.
     *
     * @param path JSON path to search for.
     * @param wrappingBehavior Determine if and when to wrap the resulting value into an array.
     * @return The extracted JSON value.
     */
    public OutType jsonQuery(String path, JsonQueryWrapper wrappingBehavior) {
        return jsonQuery(
                path, wrappingBehavior, JsonQueryOnEmptyOrError.NULL, JsonQueryOnEmptyOrError.NULL);
    }

    /**
     * Extracts JSON values from a JSON string.
     *
     * <p>See also {@link #jsonQuery(String, JsonQueryWrapper, JsonQueryOnEmptyOrError,
     * JsonQueryOnEmptyOrError)}.
     *
     * @param path JSON path to search for.
     * @return The extracted JSON value.
     */
    public OutType jsonQuery(String path) {
        return jsonQuery(path, JsonQueryWrapper.WITHOUT_ARRAY);
    }
}
