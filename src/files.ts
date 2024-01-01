import fse from 'fs-extra';
import { resolve } from 'path';

export async function writeSchemaFile(outDir: string, schemaName: string, content: string): Promise<boolean> {
  const filePath = resolve(outDir, `${schemaName}.ts`);
  // create if it does not exist
  await fse.ensureFile(filePath);
  await fse.writeFile(filePath, content);
  return true;
}
