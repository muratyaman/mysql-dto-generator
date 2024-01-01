import dotenv from 'dotenv';
import { createPool } from 'mysql2/promise';
import { MySqlInfoService } from './mysql-info';
import { makeCommand } from './cmd';
import { writeSchemaFile } from './files';
import { MySqlDtoGenerator } from './mysql-dto-generator';

dotenv.config();
main();

async function main() {
  let exitCode = 0;
  console.info('begin!');

  try {
    const cmd = makeCommand();

    cmd.parse(process.argv);
    const options = cmd.opts();

    const dbOptions = {
      host: options.host ?? 'localhost',
      port: options.port ?? '5432',
      user: options.user ?? '',
      password: options.password ?? '',
      database: options.database ?? '',
    };

    const outDir = String(options.outdir ?? '').trim();
    if (outDir === '') throw new Error('Output directory is required');

    const dbPool = createPool(dbOptions);
    try {
      console.info('checking connection to your database...');
      const rows = await dbPool.query('SELECT 1 AS success');
      console.info('checking connection to your database... done!', rows);
      const dbInfo = new MySqlInfoService(dbPool, dbOptions.database ?? '');

      const generator = new MySqlDtoGenerator(dbInfo);

      const schemaOutputList = await generator.generate();
      for (const schemaOutput of schemaOutputList) {
        await writeSchemaFile(outDir, schemaOutput.schemaName, schemaOutput.content);
      }
    } catch (err) {
      exitCode = -1;
      console.error(err);
    } finally {
      dbPool.end();
    }

  } catch (err) {
    exitCode = -1;
    console.error(err);
  }

  console.info('the end!');
  process.exit(exitCode);
}
