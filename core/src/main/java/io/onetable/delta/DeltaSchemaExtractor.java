/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package io.onetable.delta;

import static io.onetable.collectors.CustomCollectors.toList;
import static io.onetable.delta.DeltaPartitionExtractor.DELTA_GENERATION_EXPRESSION;
import static io.onetable.schema.SchemaUtils.getFullyQualifiedPath;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import io.onetable.exception.NotSupportedException;
import io.onetable.exception.SchemaExtractorException;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;

/**
 * Converts between Delta and OneTable schemas. Some items to be aware of:
 *
 * <ul>
 *   <li>Delta schemas are represented as Spark StructTypes which do not have enums so the enum
 *       types are lost when converting from Onetable to Delta Lake representations
 *   <li>Delta does not have a fixed length byte array option so {@link OneType#FIXED} is simply
 *       translated to a {@link org.apache.spark.sql.types.BinaryType}
 *   <li>Similarly, {@link OneType#TIMESTAMP_NTZ} is translated to a long in Delta Lake
 * </ul>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DeltaSchemaExtractor {
  private static final String DELTA_COLUMN_MAPPING_ID = "delta.columnMapping.id";
  private static final DeltaSchemaExtractor INSTANCE = new DeltaSchemaExtractor();

  public static DeltaSchemaExtractor getInstance() {
    return INSTANCE;
  }

  public StructType fromOneSchema(OneSchema oneSchema) {
    StructField[] fields =
        oneSchema.getFields().stream()
            .map(
                field ->
                    new StructField(
                        field.getName(),
                        convertFieldType(field),
                        field.getSchema().isNullable(),
                        Metadata.empty()))
            .toArray(StructField[]::new);
    return new StructType(fields);
  }

  private DataType convertFieldType(OneField field) {
    switch (field.getSchema().getDataType()) {
      case ENUM:
      case STRING:
        return DataTypes.StringType;
      case INT:
        return DataTypes.IntegerType;
      case LONG:
      case TIMESTAMP_NTZ:
        return DataTypes.LongType;
      case BYTES:
      case FIXED:
      case UUID:
        return DataTypes.BinaryType;
      case BOOLEAN:
        return DataTypes.BooleanType;
      case FLOAT:
        return DataTypes.FloatType;
      case DATE:
        return DataTypes.DateType;
      case TIMESTAMP:
        return DataTypes.TimestampType;
      case DOUBLE:
        return DataTypes.DoubleType;
      case DECIMAL:
        int precision =
            (int) field.getSchema().getMetadata().get(OneSchema.MetadataKey.DECIMAL_PRECISION);
        int scale = (int) field.getSchema().getMetadata().get(OneSchema.MetadataKey.DECIMAL_SCALE);
        return DataTypes.createDecimalType(precision, scale);
      case RECORD:
        return fromOneSchema(field.getSchema());
      case MAP:
        OneField key =
            field.getSchema().getFields().stream()
                .filter(
                    mapField -> OneField.Constants.MAP_KEY_FIELD_NAME.equals(mapField.getName()))
                .findFirst()
                .orElseThrow(() -> new SchemaExtractorException("Invalid map schema"));
        OneField value =
            field.getSchema().getFields().stream()
                .filter(
                    mapField -> OneField.Constants.MAP_VALUE_FIELD_NAME.equals(mapField.getName()))
                .findFirst()
                .orElseThrow(() -> new SchemaExtractorException("Invalid map schema"));
        return DataTypes.createMapType(
            convertFieldType(key), convertFieldType(value), value.getSchema().isNullable());
      case LIST:
        OneField element =
            field.getSchema().getFields().stream()
                .filter(
                    arrayField ->
                        OneField.Constants.ARRAY_ELEMENT_FIELD_NAME.equals(arrayField.getName()))
                .findFirst()
                .orElseThrow(() -> new SchemaExtractorException("Invalid array schema"));
        return DataTypes.createArrayType(
            convertFieldType(element), element.getSchema().isNullable());
      default:
        throw new NotSupportedException("Unsupported type: " + field.getSchema().getDataType());
    }
  }

  public OneSchema toOneSchema(StructType structType) {
    return toOneSchema(structType, null, false, null);
  }

  private OneSchema toOneSchema(
      DataType dataType, String parentPath, boolean nullable, String comment) {
    Map<OneSchema.MetadataKey, Object> metadata = null;
    List<OneField> fields = null;
    OneType type;
    String typeName = dataType.typeName();
    // trims parameters to type name for matching
    int openParenIndex = typeName.indexOf("(");
    String trimmedTypeName = openParenIndex > 0 ? typeName.substring(0, openParenIndex) : typeName;
    switch (trimmedTypeName) {
      case "integer":
        type = OneType.INT;
        break;
      case "string":
        type = OneType.STRING;
        break;
      case "boolean":
        type = OneType.BOOLEAN;
        break;
      case "float":
        type = OneType.FLOAT;
        break;
      case "double":
        type = OneType.DOUBLE;
        break;
      case "binary":
        type = OneType.BYTES;
        break;
      case "long":
        type = OneType.LONG;
        break;
      case "date":
        type = OneType.DATE;
        break;
      case "timestamp":
        type = OneType.TIMESTAMP;
        // Timestamps in Delta are microsecond precision by default
        metadata =
            Collections.singletonMap(
                OneSchema.MetadataKey.TIMESTAMP_PRECISION, OneSchema.MetadataValue.MICROS);
        break;
      case "struct":
        StructType structType = (StructType) dataType;
        fields =
            Arrays.stream(structType.fields())
                .filter(field -> !field.metadata().contains(DELTA_GENERATION_EXPRESSION))
                .map(
                    field -> {
                      Integer fieldId =
                          field.metadata().contains(DELTA_COLUMN_MAPPING_ID)
                              ? (int) field.metadata().getLong(DELTA_COLUMN_MAPPING_ID)
                              : null;
                      String fieldComment =
                          field.getComment().isDefined() ? field.getComment().get() : null;
                      OneSchema schema =
                          toOneSchema(
                              field.dataType(),
                              getFullyQualifiedPath(parentPath, field.name()),
                              field.nullable(),
                              fieldComment);
                      return OneField.builder()
                          .name(field.name())
                          .fieldId(fieldId)
                          .parentPath(parentPath)
                          .schema(schema)
                          .defaultValue(
                              field.nullable() ? OneField.Constants.NULL_DEFAULT_VALUE : null)
                          .build();
                    })
                .collect(toList(structType.fields().length));
        type = OneType.RECORD;
        break;
      case "decimal":
        DecimalType decimalType = (DecimalType) dataType;
        metadata = new HashMap<>(2, 1.0f);
        metadata.put(OneSchema.MetadataKey.DECIMAL_PRECISION, decimalType.precision());
        metadata.put(OneSchema.MetadataKey.DECIMAL_SCALE, decimalType.scale());
        type = OneType.DECIMAL;
        break;
      case "array":
        ArrayType arrayType = (ArrayType) dataType;
        OneSchema elementSchema =
            toOneSchema(
                arrayType.elementType(),
                getFullyQualifiedPath(parentPath, OneField.Constants.ARRAY_ELEMENT_FIELD_NAME),
                arrayType.containsNull(),
                null);
        OneField elementField =
            OneField.builder()
                .name(OneField.Constants.ARRAY_ELEMENT_FIELD_NAME)
                .parentPath(parentPath)
                .schema(elementSchema)
                .build();
        type = OneType.LIST;
        fields = Collections.singletonList(elementField);
        break;
      case "map":
        MapType mapType = (MapType) dataType;
        OneSchema keySchema =
            toOneSchema(
                mapType.keyType(),
                getFullyQualifiedPath(parentPath, OneField.Constants.MAP_VALUE_FIELD_NAME),
                false,
                null);
        OneField keyField =
            OneField.builder()
                .name(OneField.Constants.MAP_KEY_FIELD_NAME)
                .parentPath(parentPath)
                .schema(keySchema)
                .build();
        OneSchema valueSchema =
            toOneSchema(
                mapType.valueType(),
                getFullyQualifiedPath(parentPath, OneField.Constants.MAP_VALUE_FIELD_NAME),
                mapType.valueContainsNull(),
                null);
        OneField valueField =
            OneField.builder()
                .name(OneField.Constants.MAP_VALUE_FIELD_NAME)
                .parentPath(parentPath)
                .schema(valueSchema)
                .build();
        type = OneType.MAP;
        fields = Arrays.asList(keyField, valueField);
        break;
      default:
        throw new NotSupportedException("Unsupported type: " + dataType.typeName());
    }
    return OneSchema.builder()
        .name(trimmedTypeName)
        .dataType(type)
        .comment(comment)
        .isNullable(nullable)
        .metadata(metadata)
        .fields(fields)
        .build();
  }
}
