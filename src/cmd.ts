import { Command, Option } from 'commander';

export function makeCommand() {
  const cmd = new Command();
  const dbHostOption = (new Option('-h, --host <host>', 'enter host or set PGHOST')).env('PGHOST').default('localhost');
  const dbPortOption = (new Option('-p, --port <port>', 'enter port or set PGPORT')).env('PGPORT').default('5432');
  const dbUserOption = (new Option('-u, --user <user>', 'enter username or set PGUSER')).env('PGUSER');
  const dbPassOption = (new Option('-P, --password <password>', 'enter password or set PGPASSWORD')).env('PGPASSWORD');
  const dbNameOption  = (new Option('-d, --database <database>', 'enter database name or set PGDATABASE')).env('PGDATABASE');
  const outDirOption  = (new Option('-o, --outdir <outdir>', 'enter output directory or set PGDTOGEN_OUTDIR')).env('PGDTOGEN_OUTDIR');

  cmd
    .version('1.0.0', '-v, --version', 'output the current version')
    .option('--debug', 'turn on debugging')
    .addOption(dbHostOption)
    .addOption(dbPortOption)
    .addOption(dbUserOption)
    .addOption(dbPassOption)
    .addOption(dbNameOption)
    .addOption(outDirOption)
  ;

  return cmd;
}
