// Durable redemption storage - "the proofs ARE the money".
// One redemption writes up to three DISTINCT keys, never overwriting
// any:
//
//   <id>/outputs.json     BEFORE the swap: serialized OutputData (secret, r,
//                         B_) + the exact swap request. What makes the NUT-09
//                         restore ladder unconditional.
//   <id>/signatures.json  AFTER the swap: the mint's blind signatures + the
//                         constructed proofs. This object is the money.
//   <id>/outcome.json     the delivery outcome reported back by the MCP
//                         server - the delivery audit record.
//
// Append-only by construction (three immutable keys), matching the bucket
// policy that denies DeleteObject: nothing here ever needs an overwrite, so an
// overwrite is always a bug.
//
// Two backends: S3 (SSE-KMS, the deployed path - the decisive property is that
// only the service's own credentials can read the bucket, so an account that can
// read the host's filesystem still cannot read the proofs) and a local
// directory (the test suite and the pre-Terraform demo path).

import { mkdir, writeFile, readFile } from 'node:fs/promises';
import { dirname, join, normalize, sep } from 'node:path';
import type { Config } from './config.ts';

export interface RedemptionStore {
  /** Write a NEW object; MUST fail rather than overwrite. */
  put(key: string, body: string): Promise<void>;
  /** Read an object, or null if absent. */
  get(key: string): Promise<string | null>;
}

/** True when a put failed BECAUSE the key already exists - the append-only
 * refusal, which redeem.ts treats as "resume the persisted attempt", not as a
 * storage outage. FileStore surfaces EEXIST (the wx flag); S3 surfaces the
 * IfNoneMatch: '*' precondition as PreconditionFailed / HTTP 412. */
export function isAlreadyExists(e: unknown): boolean {
  const err = e as { code?: string; name?: string; $metadata?: { httpStatusCode?: number } };
  return (
    err.code === 'EEXIST' ||
    err.name === 'PreconditionFailed' ||
    err.$metadata?.httpStatusCode === 412
  );
}

export class FileStore implements RedemptionStore {
  private readonly dir: string;
  constructor(dir: string) {
    this.dir = dir;
  }

  private pathFor(key: string): string {
    const path = normalize(join(this.dir, key));
    // The id component is a hex digest we compute ourselves, but refuse
    // traversal anyway - storage code should not trust its caller's key
    // discipline for a filesystem write.
    if (!path.startsWith(normalize(this.dir) + sep)) {
      throw new Error(`store key escapes the store dir: ${key}`);
    }
    return path;
  }

  async put(key: string, body: string): Promise<void> {
    const path = this.pathFor(key);
    await mkdir(dirname(path), { recursive: true, mode: 0o700 });
    // wx = create, fail if exists - the append-only contract.
    await writeFile(path, body, { flag: 'wx', mode: 0o600 });
  }

  async get(key: string): Promise<string | null> {
    try {
      return await readFile(this.pathFor(key), 'utf8');
    } catch (e) {
      if ((e as NodeJS.ErrnoException).code === 'ENOENT') return null;
      throw e;
    }
  }
}

export class S3Store implements RedemptionStore {
  // Loaded lazily so the file backend (tests, demo) never touches the AWS SDK.
  private client: import('@aws-sdk/client-s3').S3Client | null = null;
  private sdk: typeof import('@aws-sdk/client-s3') | null = null;

  private readonly bucket: string;
  private readonly prefix: string;
  private readonly region: string;

  constructor(bucket: string, prefix: string, region: string) {
    this.bucket = bucket;
    this.prefix = prefix;
    this.region = region;
  }

  private async load() {
    if (!this.sdk) {
      this.sdk = await import('@aws-sdk/client-s3');
      this.client = new this.sdk.S3Client({ region: this.region });
    }
    return { sdk: this.sdk, client: this.client! };
  }

  async put(key: string, body: string): Promise<void> {
    const { sdk, client } = await this.load();
    // IfNoneMatch: '*' = fail with 412 if the key exists - S3's native
    // conditional write, the same append-only contract as FileStore's wx.
    // Encryption is the BUCKET's default (SSE-KMS, set in Terraform); naming
    // the key id here too would be a second copy that can drift.
    await client.send(
      new sdk.PutObjectCommand({
        Bucket: this.bucket,
        Key: this.prefix + key,
        Body: body,
        ContentType: 'application/json',
        IfNoneMatch: '*',
      }),
    );
  }

  async get(key: string): Promise<string | null> {
    const { sdk, client } = await this.load();
    try {
      const res = await client.send(
        new sdk.GetObjectCommand({ Bucket: this.bucket, Key: this.prefix + key }),
      );
      return (await res.Body?.transformToString()) ?? null;
    } catch (e) {
      if ((e as { name?: string }).name === 'NoSuchKey') return null;
      throw e;
    }
  }
}

export function storeFor(config: Config): RedemptionStore {
  if (config.store === 's3') return new S3Store(config.s3Bucket, config.s3Prefix, config.s3Region);
  return new FileStore(config.storeDir);
}
