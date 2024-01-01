
import { JsonType } from './_types';

/**
 * Model for read & write operations on table tests
 */
export interface testsDtoWritable {

  /**
   * Type: character varying(50); UDT: pg_catalog.varchar
   * Default value: null
   */
  email: string;

  /**
   * Type: integer; UDT: pg_catalog.int4
   * Default value: null
   */
  id: number;

  /**
   * Type: timestamp with time zone; UDT: pg_catalog.timestamptz
   * Default value: null
   */
  ts: Date;
}
export type testsDto = testsDtoWritable;
