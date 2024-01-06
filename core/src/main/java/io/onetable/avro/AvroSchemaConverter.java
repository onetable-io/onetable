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
 
package io.onetable.avro;

import static io.onetable.collectors.CustomCollectors.toList;
import static io.onetable.schema.SchemaUtils.getFullyQualifiedPath;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import io.onetable.exception.SchemaExtractorException;
import io.onetable.exception.UnsupportedSchemaTypeException;
import io.onetable.hudi.idtracking.IdTracker;
import io.onetable.hudi.idtracking.models.IdMapping;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;

/**
 * Class that converts Avro Schema {@link Schema} to Canonical Schema {@link OneSchema} and
 * vice-versa. This conversion is fully reversible and there is a strict 1 to 1 mapping between avro
 * data types and canonical data types.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AvroSchemaConverter {
  // avro only supports string keys in maps
  private static final OneField MAP_KEY_FIELD =
      OneField.builder()
          .name(OneField.Constants.MAP_KEY_FIELD_NAME)
          .schema(
              OneSchema.builder()
                  .name("map_key")
                  .dataType(OneType.STRING)
                  .isNullable(false)
                  .build())
          .defaultValue("")
          .build();
  private static final AvroSchemaConverter INSTANCE = new AvroSchemaConverter();
  private static final String ELEMENT = "element";
  private static final String KEY = "key";
  private static final String VALUE = "value";

  public static AvroSchemaConverter getInstance() {
    return INSTANCE;
  }

  public OneSchema toOneSchema(Schema schema) {
    Map<String, IdMapping> fieldNameToIdMapping =
        IdTracker.getInstance()
            .getIdTracking(schema)
            .map(
                idTracking ->
                    idTracking.getIdMappings().stream()
                        .collect(Collectors.toMap(IdMapping::getName, Function.identity())))
            .orElse(Collections.emptyMap());
    return toOneSchema(schema, null, fieldNameToIdMapping);
  }

  /**
   * Converts the Avro {@link Schema} to {@link OneSchema}.
   *
   * @param schema The schema being converted
   * @param parentPath If this schema is nested within another, this will be a dot separated string
   *     representing the path from the top most field to the current schema.
   * @param fieldNameToIdMapping map of fieldName to IdMapping to track field IDs provided by the
   *     source schema. If source schema does not contain IdMappings, map will be empty.
   * @return a converted schema
   */
  private OneSchema toOneSchema(
      Schema schema, String parentPath, Map<String, IdMapping> fieldNameToIdMapping) {
    // TODO - Does not handle recursion in Avro schema
    OneType newDataType;
    Map<OneSchema.MetadataKey, Object> metadata = new HashMap<>();
    switch (schema.getType()) {
      case INT:
        LogicalType logicalType = schema.getLogicalType();
        if (logicalType instanceof LogicalTypes.Date) {
          newDataType = OneType.DATE;
        } else {
          newDataType = OneType.INT;
        }
        break;
      case STRING:
        newDataType = OneType.STRING;
        break;
      case BOOLEAN:
        newDataType = OneType.BOOLEAN;
        break;
      case BYTES:
      case FIXED:
        logicalType = schema.getLogicalType();
        if (logicalType instanceof LogicalTypes.Decimal) {
          metadata.put(
              OneSchema.MetadataKey.DECIMAL_PRECISION,
              ((LogicalTypes.Decimal) logicalType).getPrecision());
          metadata.put(
              OneSchema.MetadataKey.DECIMAL_SCALE, ((LogicalTypes.Decimal) logicalType).getScale());
          if (schema.getType() == Schema.Type.FIXED) {
            metadata.put(OneSchema.MetadataKey.FIXED_BYTES_SIZE, schema.getFixedSize());
          }
          newDataType = OneType.DECIMAL;
          break;
        }
        if (schema.getType() == Schema.Type.FIXED) {
          metadata.put(OneSchema.MetadataKey.FIXED_BYTES_SIZE, schema.getFixedSize());
          newDataType = OneType.FIXED;
        } else {
          newDataType = OneType.BYTES;
        }
        break;
      case DOUBLE:
        newDataType = OneType.DOUBLE;
        break;
      case FLOAT:
        newDataType = OneType.FLOAT;
        break;
      case LONG:
        logicalType = schema.getLogicalType();
        if (logicalType instanceof LogicalTypes.TimestampMillis) {
          newDataType = OneType.TIMESTAMP;
          metadata.put(OneSchema.MetadataKey.TIMESTAMP_PRECISION, OneSchema.MetadataValue.MILLIS);
        } else if (logicalType instanceof LogicalTypes.TimestampMicros) {
          newDataType = OneType.TIMESTAMP;
          metadata.put(OneSchema.MetadataKey.TIMESTAMP_PRECISION, OneSchema.MetadataValue.MICROS);
        } else if (logicalType instanceof LogicalTypes.LocalTimestampMillis) {
          newDataType = OneType.TIMESTAMP_NTZ;
          metadata.put(OneSchema.MetadataKey.TIMESTAMP_PRECISION, OneSchema.MetadataValue.MILLIS);
        } else if (logicalType instanceof LogicalTypes.LocalTimestampMicros) {
          newDataType = OneType.TIMESTAMP_NTZ;
          metadata.put(OneSchema.MetadataKey.TIMESTAMP_PRECISION, OneSchema.MetadataValue.MICROS);
        } else {
          newDataType = OneType.LONG;
        }
        break;
      case ENUM:
        metadata.put(OneSchema.MetadataKey.ENUM_VALUES, schema.getEnumSymbols());
        newDataType = OneType.ENUM;
        break;
      case NULL:
        newDataType = OneType.NULL;
        break;
      case RECORD:
        List<OneField> subFields = new ArrayList<>(schema.getFields().size());
        for (Schema.Field avroField : schema.getFields()) {
          IdMapping idMapping = fieldNameToIdMapping.get(avroField.name());
          OneSchema subFieldSchema =
              toOneSchema(
                  avroField.schema(),
                  getFullyQualifiedPath(parentPath, avroField.name()),
                  getChildIdMap(idMapping));
          Object defaultValue = getDefaultValue(avroField);
          subFields.add(
              OneField.builder()
                  .parentPath(parentPath)
                  .name(avroField.name())
                  .schema(subFieldSchema)
                  .defaultValue(defaultValue)
                  .fieldId(idMapping == null ? null : idMapping.getId())
                  .build());
        }
        return OneSchema.builder()
            .name(schema.getName())
            .comment(schema.getDoc())
            .dataType(OneType.RECORD)
            .fields(subFields)
            .isNullable(schema.isNullable())
            .build();
      case ARRAY:
        IdMapping elementMapping = fieldNameToIdMapping.get(ELEMENT);
        OneSchema elementSchema =
            toOneSchema(
                schema.getElementType(),
                getFullyQualifiedPath(parentPath, OneField.Constants.ARRAY_ELEMENT_FIELD_NAME),
                getChildIdMap(elementMapping));
        OneField elementField =
            OneField.builder()
                .name(OneField.Constants.ARRAY_ELEMENT_FIELD_NAME)
                .parentPath(parentPath)
                .schema(elementSchema)
                .fieldId(elementMapping == null ? null : elementMapping.getId())
                .build();
        return OneSchema.builder()
            .name(schema.getName())
            .dataType(OneType.LIST)
            .comment(schema.getDoc())
            .isNullable(schema.isNullable())
            .fields(Collections.singletonList(elementField))
            .build();
      case MAP:
        IdMapping keyMapping = fieldNameToIdMapping.get(KEY);
        IdMapping valueMapping = fieldNameToIdMapping.get(VALUE);
        OneSchema valueSchema =
            toOneSchema(
                schema.getValueType(),
                getFullyQualifiedPath(parentPath, OneField.Constants.MAP_VALUE_FIELD_NAME),
                getChildIdMap(valueMapping));
        OneField valueField =
            OneField.builder()
                .name(OneField.Constants.MAP_VALUE_FIELD_NAME)
                .parentPath(parentPath)
                .schema(valueSchema)
                .fieldId(valueMapping == null ? null : valueMapping.getId())
                .build();
        return OneSchema.builder()
            .name(schema.getName())
            .dataType(OneType.MAP)
            .comment(schema.getDoc())
            .isNullable(schema.isNullable())
            .fields(
                Arrays.asList(
                    MAP_KEY_FIELD.toBuilder()
                        .parentPath(parentPath)
                        .fieldId(keyMapping == null ? null : keyMapping.getId())
                        .build(),
                    valueField))
            .build();
      case UNION:
        boolean containsUnionWithNull =
            schema.getTypes().stream().anyMatch(t -> t.getType() == Schema.Type.NULL);
        if (containsUnionWithNull) {
          List<Schema> remainingSchemas =
              schema.getTypes().stream()
                  .filter(t -> t.getType() != Schema.Type.NULL)
                  .collect(Collectors.toList());
          if (remainingSchemas.size() == 1) {
            OneSchema restSchema =
                toOneSchema(remainingSchemas.get(0), parentPath, fieldNameToIdMapping);
            return OneSchema.builderFrom(restSchema).isNullable(true).build();
          } else {
            return OneSchema.builderFrom(toOneSchema(Schema.createUnion(remainingSchemas)))
                .isNullable(true)
                .build();
          }
        } else {
          throw new UnsupportedSchemaTypeException(
              String.format("Unsupported complex union type %s", schema));
        }
      default:
        throw new UnsupportedSchemaTypeException(
            String.format("Unsupported schema type %s", schema));
    }
    return OneSchema.builder()
        .name(schema.getName())
        .dataType(newDataType)
        .comment(schema.getDoc())
        .isNullable(schema.isNullable())
        .metadata(metadata.isEmpty() ? null : metadata)
        .build();
  }

  private Map<String, IdMapping> getChildIdMap(IdMapping idMapping) {
    if (idMapping == null) {
      return Collections.emptyMap();
    }
    return idMapping.getFields().stream()
        .collect(Collectors.toMap(IdMapping::getName, Function.identity()));
  }

  private static Object getDefaultValue(Schema.Field avroField) {
    return Schema.Field.NULL_VALUE.equals(avroField.defaultVal())
        ? OneField.Constants.NULL_DEFAULT_VALUE
        : avroField.defaultVal();
  }

  /**
   * Converts the {@link OneSchema} to Avro {@link Schema}.
   *
   * @param oneSchema internal schema representation
   * @return an Avro schema
   */
  public Schema fromOneSchema(OneSchema oneSchema) {
    return fromOneSchema(oneSchema, null);
  }

  /**
   * Internal method for converting the {@link OneSchema} to Avro {@link Schema}.
   *
   * @param oneSchema internal schema representation
   * @param currentPath If this schema is nested within another, this will be a dot separated
   *     string. This is used for the avro namespace to guarantee unique names for nested records.
   * @return an Avro schema
   */
  private Schema fromOneSchema(OneSchema oneSchema, String currentPath) {
    switch (oneSchema.getDataType()) {
      case RECORD:
        List<Schema.Field> fields =
            oneSchema.getFields().stream()
                .map(
                    field ->
                        new Schema.Field(
                            field.getName(),
                            fromOneSchema(
                                field.getSchema(),
                                getFullyQualifiedPath(currentPath, field.getName())),
                            field.getSchema().getComment(),
                            OneField.Constants.NULL_DEFAULT_VALUE == field.getDefaultValue()
                                ? Schema.Field.NULL_VALUE
                                : field.getDefaultValue()))
                .collect(toList(oneSchema.getFields().size()));
        return finalizeSchema(
            Schema.createRecord(
                oneSchema.getName(), oneSchema.getComment(), currentPath, false, fields),
            oneSchema);
      case BYTES:
        return finalizeSchema(Schema.create(Schema.Type.BYTES), oneSchema);
      case BOOLEAN:
        return finalizeSchema(Schema.create(Schema.Type.BOOLEAN), oneSchema);
      case INT:
        return finalizeSchema(Schema.create(Schema.Type.INT), oneSchema);
      case LONG:
        return finalizeSchema(Schema.create(Schema.Type.LONG), oneSchema);
      case STRING:
        return finalizeSchema(Schema.create(Schema.Type.STRING), oneSchema);
      case FLOAT:
        return finalizeSchema(Schema.create(Schema.Type.FLOAT), oneSchema);
      case DOUBLE:
        return finalizeSchema(Schema.create(Schema.Type.DOUBLE), oneSchema);
      case ENUM:
        return finalizeSchema(
            Schema.createEnum(
                oneSchema.getName(),
                oneSchema.getComment(),
                null,
                (List<String>) oneSchema.getMetadata().get(OneSchema.MetadataKey.ENUM_VALUES),
                null),
            oneSchema);
      case DATE:
        return finalizeSchema(
            LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)), oneSchema);
      case TIMESTAMP:
        if (oneSchema.getMetadata().get(OneSchema.MetadataKey.TIMESTAMP_PRECISION)
            == OneSchema.MetadataValue.MICROS) {
          return finalizeSchema(
              LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)),
              oneSchema);
        } else {
          return finalizeSchema(
              LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)),
              oneSchema);
        }
      case TIMESTAMP_NTZ:
        if (oneSchema.getMetadata().get(OneSchema.MetadataKey.TIMESTAMP_PRECISION)
            == OneSchema.MetadataValue.MICROS) {
          return finalizeSchema(
              LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG)),
              oneSchema);
        } else {
          return finalizeSchema(
              LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG)),
              oneSchema);
        }
      case LIST:
        OneField elementField =
            oneSchema.getFields().stream()
                .filter(
                    field -> OneField.Constants.ARRAY_ELEMENT_FIELD_NAME.equals(field.getName()))
                .findFirst()
                .orElseThrow(() -> new SchemaExtractorException("Invalid array schema"));
        return finalizeSchema(
            Schema.createArray(fromOneSchema(elementField.getSchema(), elementField.getPath())),
            oneSchema);
      case MAP:
        OneField valueField =
            oneSchema.getFields().stream()
                .filter(field -> OneField.Constants.MAP_VALUE_FIELD_NAME.equals(field.getName()))
                .findFirst()
                .orElseThrow(() -> new SchemaExtractorException("Invalid map schema"));
        return finalizeSchema(
            Schema.createMap(fromOneSchema(valueField.getSchema(), valueField.getPath())),
            oneSchema);
      case DECIMAL:
        int precision = (int) oneSchema.getMetadata().get(OneSchema.MetadataKey.DECIMAL_PRECISION);
        int scale = (int) oneSchema.getMetadata().get(OneSchema.MetadataKey.DECIMAL_SCALE);
        Integer size =
            (Integer) oneSchema.getMetadata().get(OneSchema.MetadataKey.FIXED_BYTES_SIZE);
        if (size == null) {
          return finalizeSchema(
              LogicalTypes.decimal(precision, scale).addToSchema(Schema.create(Schema.Type.BYTES)),
              oneSchema);
        } else {
          return finalizeSchema(
              LogicalTypes.decimal(precision, scale)
                  .addToSchema(
                      Schema.createFixed(oneSchema.getName(), oneSchema.getComment(), null, size)),
              oneSchema);
        }
      case UUID:
      case FIXED:
        Integer fixedSize =
            (Integer) oneSchema.getMetadata().get(OneSchema.MetadataKey.FIXED_BYTES_SIZE);
        return finalizeSchema(
            Schema.createFixed(oneSchema.getName(), oneSchema.getComment(), null, fixedSize),
            oneSchema);
      default:
        throw new UnsupportedSchemaTypeException(
            "Encountered unhandled type during OneSchema to Avro conversion: "
                + oneSchema.getDataType());
    }
  }

  private String buildCurrentPath(OneField field, String parentPath) {
    return Optional.ofNullable(parentPath)
        .map(path -> path + "." + field.getName())
        .orElse(field.getName());
  }

  private static Schema finalizeSchema(Schema targetSchema, OneSchema inputSchema) {
    if (inputSchema.isNullable()) {
      return Schema.createUnion(Schema.create(Schema.Type.NULL), targetSchema);
    }
    return targetSchema;
  }
}
