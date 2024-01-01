import { Pool, RowDataPacket } from 'mysql2/promise';

const sqlSelectSchemataAll = `
SELECT
  *
FROM information_schema.schemata
WHERE catalog_name = 'def'
ORDER BY schema_name;
`;

const sqlSelectSchemata = `
SELECT
  *
FROM information_schema.schemata
WHERE catalog_name = 'def'
  AND schema_name NOT IN ('mysql', 'sys', 'information_schema', 'performance_schema')
ORDER BY schema_name;
`;

const sqlSelectTables = `
SELECT
  *
FROM information_schema.tables
WHERE table_catalog = 'def'
  AND table_schema = ?
  AND table_type = 'BASE TABLE'
ORDER BY table_schema, table_name;
`;

const sqlSelectColumns = `
SELECT
  *
FROM information_schema.columns
WHERE table_catalog = 'def'
  AND table_schema = ?
ORDER BY table_name, column_name
`;

const sqlSelectColumnsByTable = `
SELECT
  .*
FROM information_schema.columns
WHERE cols.table_catalog = 'def'
  AND cols.table_schema = ?
  AND cols.table_name = ?
ORDER BY column_name
`;

export const mySqlMsgDbQueryError = 'DB query error';
export const mySqlMsgDbConnError  = 'DB connection error';

export class MySqlInfoService {

  constructor(protected _db: Pool, public readonly dbName: string, protected _logger = console) {
    if (dbName.trim() === '') throw new Error('invalid database name');
  }

  async disconnect() {
    await this._db.end();
  }

  async query<TRow = any>(text: string, values: any[] = []): Promise<TRow[]> {
    let rows: TRow[] = [];
    let dbErr: any = null;
    try {
      const client = await this._db.getConnection();
      try {
        // using prepared + parameterized queries
        const result = await client.query<RowDataPacket[]>(text, values);
        result.forEach(row => rows.push(row as TRow));
      } catch (err) {
        dbErr = err;
        this._logger.error(mySqlMsgDbQueryError, err);
      } finally {
        client.release();
      }
    } catch (err) {
      dbErr = err;
      this._logger.error(mySqlMsgDbConnError, err);
      throw err;
    }
    if (dbErr) throw dbErr;
    return rows;
  }

  async schemataAll(): Promise<MySqlSchema[]> {
    return this.query<MySqlSchema>(sqlSelectSchemataAll, [this.dbName]);
  }

  async schemata(): Promise<MySqlSchema[]> {
    return this.query<MySqlSchema>(sqlSelectSchemata, [this.dbName]);
  }

  schema(schemaName: string): MySqlSchemaService {
    return new MySqlSchemaService(this, schemaName);
  }
}

export class MySqlSchemaService {
  constructor(protected _is: MySqlInfoService, public readonly schemaName: string) {}

  async tables(): Promise<MySqlTable[]> {
    return this._is.query<MySqlTable>(sqlSelectTables, [this._is.dbName, this.schemaName]);
  }

  async columns(): Promise<MySqlColumn[]> {
    const colRecords = await this._is.query<MySqlColumn>(sqlSelectColumns, [this._is.dbName, this.schemaName]);
    return colRecords;
  }

  table(tableName: string) {
    return new MySqlTableService(this._is, this, tableName);
  }
}

export class MySqlTableService {
  constructor(protected _is: MySqlInfoService, protected _dbSchema: MySqlSchemaService, public readonly tableName: string) {}

  async columns(): Promise<MySqlColumn[]> {
    const colRecords = await this._is.query<MySqlColumn>(sqlSelectColumnsByTable, [this._is.dbName, this._dbSchema.schemaName, this.tableName]);
    return colRecords;
  }
}

// @see https://dev.mysql.com/doc/refman/8.0/en/general-information-schema-tables.html
export interface MySqlSchema {
  catalog_name              : string | null;                                 // varchar varchar(64) yes
  default_character_set_name: string;                                        // varchar varchar(64) no
  default_collation_name    : string;                                        // varchar varchar(64) no
  default_encryption        : MySqlYesOrNoEnum | MySqlYesOrNoType | string;  // enum enum('no','yes') no
  schema_name               : string | null;                                 // varchar varchar(64) yes
  sql_path                  : unknown | null;                                // binary binary(0) yes
}

// @see https://dev.mysql.com/doc/refman/8.0/en/information-schema-schemata-table.html
export interface MySqlTable { // data_type, column_type, is_nullable
  auto_increment : number | null;  // bigint bigint unsigned yes
  avg_row_length : number | null;  // bigint bigint unsigned yes
  check_time     : string | null;  // datetime datetime yes
  checksum       : number | null;  // bigint bigint yes
  create_options : string | null;  // varchar varchar(256) yes
  create_time    : string;         // timestamp timestamp no
  data_free      : number | null;  // bigint bigint unsigned yes
  data_length    : number | null;  // bigint bigint unsigned yes
  engine         : string | null;  // varchar varchar(64) yes
  index_length   : number | null;  // bigint bigint unsigned yes
  max_data_length: number | null;  // bigint bigint unsigned yes
  row_format     : string | null;  // enum enum('fixed','dynamic','compressed','redundant','compact','paged') yes
  table_catalog  : string | null;  // varchar varchar(64) yes
  table_collation: string | null;  // varchar varchar(64) yes
  table_comment  : string | null;  // text text yes
  table_name     : string | null;  // varchar varchar(64) yes
  table_rows     : number | null;  // bigint bigint unsigned yes
  table_schema   : string | null;  // varchar varchar(64) yes
  table_type     : string;         // enum enum('base table','view','system view') no
  update_time    : string | null;  // datetime datetime yes
  version        : number | null;  // int int yes
}

// @see https://dev.mysql.com/doc/refman/8.0/en/information-schema-columns-table.html
export interface MySqlColumn {
  character_maximum_length: number | null;  // bigint bigint yes
  character_octet_length  : number | null;  // bigint bigint yes
  character_set_name      : string | null;  // varchar varchar(64) yes
  collation_name          : string | null;  // varchar varchar(64) yes
  column_comment          : string;         // text text no
  column_default          : string | null;  // text text yes
  column_key              : string;         // enum enum('','pri','uni','mul') no
  column_name             : string | null;  // varchar varchar(64) yes
  column_type             : string;         // mediumtext mediumtext no
  data_type               : string | null;  // longtext longtext yes
  datetime_precision      : number | null;  // int int unsigned yes
  extra                   : string | null;  // varchar varchar(256) yes
  generation_expression   : string;         // longtext longtext no
  is_nullable             : string;         // varchar varchar(3) no
  numeric_precision       : number | null;  // bigint bigint unsigned yes
  numeric_scale           : number | null;  // bigint bigint unsigned yes
  ordinal_position        : number;         // int int unsigned no
  privileges              : string | null;  // varchar varchar(154) yes
  srs_id                  : number | null;  // int int unsigned yes
  table_catalog           : string | null;  // varchar varchar(64) yes
  table_name              : string | null;  // varchar varchar(64) yes
  table_schema            : string | null;  // varchar varchar(64) yes
}

export type MySqlTableTypeType = 'BASE TABLE' | 'VIEW' | 'FOREIGN' | 'LOCAL TEMPORARY';
export enum MySqlTableTypeEnum {
  BASE_TABLE      = 'BASE TABLE',
  VIEW            = 'VIEW',
  FOREIGN         = 'FOREIGN',
  LOCAL_TEMPORARY = 'LOCAL TEMPORARY',
}

export type MySqlAlwaysOrNeverType = 'ALWAYS' | 'NEVER';
export enum MySqlAlwaysOrNeverEnum {
  ALWAYS = 'ALWAYS',
  NEVER  = 'NEVER',
}

export type MySqlYesOrNoType = 'YES' | 'NO';
export enum MySqlYesOrNoEnum {
  YES = 'YES',
  NO  = 'NO',
}

export enum MySqlDataTypeEnum {
  'bigint'     = 'bigint',
  'binary'     = 'binary',
  'blob'       = 'blob',
  'char'       = 'char',
  'datetime'   = 'datetime',
  'decimal'    = 'decimal',
  'double'     = 'double',
  'enum'       = 'enum',
  'float'      = 'float',
  'int'        = 'int',
  'json'       = 'json',
  'longblob'   = 'longblob',
  'longtext'   = 'longtext',
  'mediumblob' = 'mediumblob',
  'mediumint'  = 'mediumint',
  'mediumtext' = 'mediumtext',
  'set'        = 'set',
  'smallint'   = 'smallint',
  'text'       = 'text',
  'time'       = 'time',
  'timestamp'  = 'timestamp',
  'tinyint'    = 'tinyint',
  'varbinary'  = 'varbinary',
  'varchar'    = 'varchar',
}

export type MySqlDataTypeType =
  | 'bigint'
  | 'binary'
  | 'blob'
  | 'char'
  | 'datetime'
  | 'decimal'
  | 'double'
  | 'enum'
  | 'float'
  | 'int'
  | 'json'
  | 'longblob'
  | 'longtext'
  | 'mediumblob'
  | 'mediumint'
  | 'mediumtext'
  | 'set'
  | 'smallint'
  | 'text'
  | 'time'
  | 'timestamp'
  | 'tinyint'
  | 'varbinary'
  | 'varchar'
;

export const MySqlColumnTypePatterns: Array<{ ts: string; re: RegExp }> = [
  { ts: 'number', re: /bigint/i },
  { ts: 'number', re: /bigint unsigned/i },
  { ts: 'any', re: /binary\(\d+\)/i },
  { ts: 'any', re: /blob/i },
  { ts: 'string', re: /char\(\d+\)/i },
  { ts: 'string', re: /datetime/i },
  { ts: 'number', re: /decimal\(\d+,\d+\)/i },
  { ts: 'number', re: /decimal\(\d+,\d+\) unsigned/i },
  { ts: 'number', re: /double/i },
  { ts: 'number', re: /double\(\d+,\d+\)/i },
  { ts: 'string', re: /enum\(.+\)/i },
  { ts: 'number', re: /float/i },
  { ts: 'number', re: /float unsigned/i },
  { ts: 'number', re: /float\(\d+,\d+\)/i },
  { ts: 'number', re: /int/i },
  { ts: 'number', re: /int unsigned/i },
  { ts: 'any', re: /json/i },
  { ts: 'any', re: /longblob/i },
  { ts: 'string', re: /longtext/i },
  { ts: 'any', re: /mediumblob/i },
  { ts: 'number', re: /mediumint unsigned/i },
  { ts: 'string', re: /mediumtext/i },
  { ts: 'string', re: /set\(.+\)/i },
  { ts: 'number', re: /smallint/i },
  { ts: 'number', re: /smallint unsigned/i },
  { ts: 'string', re: /text/i },
  { ts: 'string', re: /time/i },
  { ts: 'string', re: /time\(\d+\)/i },
  { ts: 'string', re: /timestamp/i },
  { ts: 'string', re: /timestamp\(\d+\)/i },
  { ts: 'number', re: /tinyint/i },
  { ts: 'number', re: /tinyint unsigned/i },
  { ts: 'number', re: /tinyint\(\d+\)/i },
  { ts: 'any', re: /varbinary\(\d+\)/i },
  { ts: 'string', re: /varchar\(\d+\)/i },
];