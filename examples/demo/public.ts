
import { JsonType } from './_types';

/**
 * Model for read & write operations on table options
 */
export interface optionsDtoWritable {

  /**
   * Type: bigint; UDT: pg_catalog.int8
   * Default value: null
   */
  bigint: number;

  /**
   * Type: bigint; UDT: pg_catalog.int8
   * Default value: nextval('options_bigserial_seq'::regclass)
   */
  bigserial: number;

  /**
   * Type: bit(1); UDT: pg_catalog.bit
   * Default value: null
   */
  bit: any;

  /**
   * Type: boolean; UDT: pg_catalog.bool
   * Default value: null
   */
  bool: boolean;

  /**
   * Type: box; UDT: pg_catalog.box
   * Default value: null
   */
  box: any;

  /**
   * Type: bytea; UDT: pg_catalog.bytea
   * Default value: null
   */
  bytea: any;

  /**
   * Type: character(10); UDT: pg_catalog.bpchar
   * Default value: 'def10'::bpchar
   */
  char10: any;

  /**
   * Type: cidr; UDT: pg_catalog.cidr
   * Default value: null
   */
  cidr: any;

  /**
   * Type: circle; UDT: pg_catalog.circle
   * Default value: null
   */
  circle: any;

  /**
   * Type: date; UDT: pg_catalog.date
   * Default value: now()
   */
  date: any;

  /**
   * Type: double precision; UDT: pg_catalog.float8
   * Default value: null
   */
  double: number;

  /**
   * Type: inet; UDT: pg_catalog.inet
   * Default value: null
   */
  inet: any;

  /**
   * Type: ARRAY; UDT: pg_catalog._int4
   * Default value: null
   */
  int_arr_dim2: number[][];

  /**
   * Type: integer; UDT: pg_catalog.int4
   * Default value: null
   */
  integer: number;

  /**
   * Type: interval; UDT: pg_catalog.interval
   * Default value: null
   */
  interval: any;

  /**
   * Type: json; UDT: pg_catalog.json
   * Default value: null
   */
  json: JsonType;

  /**
   * Type: jsonb; UDT: pg_catalog.jsonb
   * Default value: null
   */
  jsonb: JsonType;

  /**
   * Type: line; UDT: pg_catalog.line
   * Default value: null
   */
  line: any;

  /**
   * Type: lseg; UDT: pg_catalog.lseg
   * Default value: null
   */
  lseg: any;

  /**
   * Type: macaddr; UDT: pg_catalog.macaddr
   * Default value: null
   */
  macaddr: any;

  /**
   * Type: macaddr8; UDT: pg_catalog.macaddr8
   * Default value: null
   */
  macaddr8: any;

  /**
   * Type: money; UDT: pg_catalog.money
   * Default value: null
   */
  money: number;

  /**
   * Type: numeric; UDT: pg_catalog.numeric
   * Default value: null
   */
  numeric: number;

  /**
   * Type: path; UDT: pg_catalog.path
   * Default value: null
   */
  path: any;

  /**
   * Type: point; UDT: pg_catalog.point
   * Default value: null
   */
  point: any;

  /**
   * Type: polygon; UDT: pg_catalog.polygon
   * Default value: null
   */
  polygon: any;

  /**
   * Type: real; UDT: pg_catalog.float4
   * Default value: null
   */
  real: number;

  /**
   * Type: integer; UDT: pg_catalog.int4
   * Default value: nextval('options_serial_seq'::regclass)
   */
  serial: number;

  /**
   * Type: smallint; UDT: pg_catalog.int2
   * Default value: null
   */
  smallint: number;

  /**
   * Type: smallint; UDT: pg_catalog.int2
   * Default value: nextval('options_smallserial_seq'::regclass)
   */
  smallserial: number;

  /**
   * Type: text; UDT: pg_catalog.text
   * Default value: null
   */
  text: string;

  /**
   * Type: time without time zone; UDT: pg_catalog.time
   * Default value: null
   */
  time: string;

  /**
   * Type: timestamp without time zone; UDT: pg_catalog.timestamp
   * Default value: null
   */
  timestamp: Date;

  /**
   * Type: timestamp with time zone; UDT: pg_catalog.timestamptz
   * Default value: null
   */
  timestamptz: Date;

  /**
   * Type: time with time zone; UDT: pg_catalog.timetz
   * Default value: null
   */
  timetz: string;

  /**
   * Type: tsquery; UDT: pg_catalog.tsquery
   * Default value: null
   */
  tsquery: any;

  /**
   * Type: tsvector; UDT: pg_catalog.tsvector
   * Default value: null
   */
  tsvector: any;

  /**
   * Type: txid_snapshot; UDT: pg_catalog.txid_snapshot
   * Default value: null
   */
  txidsnapshot: any;

  /**
   * Type: uuid; UDT: pg_catalog.uuid
   * Default value: null
   */
  uuid: string;

  /**
   * Type: bit varying(4); UDT: pg_catalog.varbit
   * Default value: null
   */
  varbit4: any;

  /**
   * Type: character varying(20); UDT: pg_catalog.varchar
   * Default value: 'def20'::character varying
   */
  varchar20: string;

  /**
   * Type: ARRAY; UDT: pg_catalog._varchar
   * Default value: null
   */
  varchar_arr_dim1: string[];

  /**
   * Type: xml; UDT: pg_catalog.xml
   * Default value: null
   */
  xml: any;
}
export type optionsDto = optionsDtoWritable;

/**
 * Model for read & write operations on table users
 * my fancy users table
 */
export interface usersDtoWritable {

  /**
   * Type: integer; UDT: pg_catalog.int4
   * Default value: null
   */
  age: number | null;

  /**
   * unique ID field for each user
   * Type: uuid; UDT: pg_catalog.uuid
   * Default value: null
   */
  id: string;

  /**
   * Type: character varying(100); UDT: pg_catalog.varchar
   * Default value: null
   */
  name: string;

  /**
   * Type: timestamp without time zone; UDT: pg_catalog.timestamp
   * Default value: null
   */
  ts: Date | null;

  /**
   * Type: timestamp with time zone; UDT: pg_catalog.timestamptz
   * Default value: null
   */
  tsz: Date | null;
}
export type usersDto = usersDtoWritable;
