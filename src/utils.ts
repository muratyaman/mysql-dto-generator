import { PgColumn, PgDataTypeEnum, PgUdtNameEnum } from 'pgsqlinfo';

const numberDataTypes: PgDataTypeEnum[] = [
  PgDataTypeEnum.bigint,
  PgDataTypeEnum.double_precision,
  PgDataTypeEnum.integer,
  PgDataTypeEnum.numeric,
  PgDataTypeEnum.real,
  PgDataTypeEnum.smallint,
];

const numberUdtNames: PgUdtNameEnum[] = [
  PgUdtNameEnum._int2,
  PgUdtNameEnum.int2,
  PgUdtNameEnum._int4,
  PgUdtNameEnum.int4,
  PgUdtNameEnum.int8,
  PgUdtNameEnum.float4,
  PgUdtNameEnum.float8,
  PgUdtNameEnum._float4,
  PgUdtNameEnum._float8,
  PgUdtNameEnum.money,
  PgUdtNameEnum.numeric,
];

const jsonDataTypes: PgDataTypeEnum[] = [
  PgDataTypeEnum.json,
  PgDataTypeEnum.jsonb,
];

const dateDataTypes: PgDataTypeEnum[] = [
  PgDataTypeEnum.timestamp_with_time_zone,
  PgDataTypeEnum.timestamp_without_time_zone,
];

const stringUdtNames: PgUdtNameEnum[] = [
  PgUdtNameEnum._char,
  PgUdtNameEnum._name,
  PgUdtNameEnum._varchar,
  PgUdtNameEnum._text,
];

const stringDataTypes: PgDataTypeEnum[] = [
  PgDataTypeEnum.character_varying,
  PgDataTypeEnum.char,
  PgDataTypeEnum.name,
  PgDataTypeEnum.text,
  PgDataTypeEnum.time_with_time_zone,
  PgDataTypeEnum.time_without_time_zone,
  PgDataTypeEnum.uuid,
];

const arrayDataTypes: PgDataTypeEnum[] = [
  PgDataTypeEnum.anyarray,
  PgDataTypeEnum.ARRAY,
];

export function getTsType(c: PgColumn): string {
  let tsType = 'any';

  // it can be [], or [][], etc. TODO: array dimensions
  let array = arrayDataTypes.includes(c.data_type as PgDataTypeEnum) ? '[]'.repeat(c.array_dimension ?? 0) : '';

  do {
    if (numberUdtNames.includes(c.udt_name as PgUdtNameEnum)) {
      tsType = 'number'; break;
    }
    if (numberDataTypes.includes(c.data_type as PgDataTypeEnum)) {
      tsType = 'number'; break;
    }
    if (dateDataTypes.includes(c.data_type as PgDataTypeEnum)) {
      tsType = 'Date'; break;
    }
    if (jsonDataTypes.includes(c.data_type as PgDataTypeEnum)) {
      tsType = 'JsonType'; break;
    }
    if (stringUdtNames.includes(c.udt_name as PgUdtNameEnum)) {
      tsType = 'string'; break;
    }
    if (stringDataTypes.includes(c.data_type as PgDataTypeEnum)) {
      tsType = 'string'; break;
    }
    if (c.data_type === 'boolean') {
      tsType = 'boolean'; break;
    }
  } while(false); // run once

  return `${tsType}${array}`;
}
