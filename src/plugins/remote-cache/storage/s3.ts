import { PassThrough, Readable } from 'node:stream'
import {
  GetObjectCommand,
  HeadObjectCommand,
  NotFound,
  S3Client,
  S3ClientConfig,
} from '@aws-sdk/client-s3'
import { Upload } from '@aws-sdk/lib-storage'
import { StorageProvider } from './index.js'

export interface S3Options {
  accessKey?: string
  secretKey?: string
  region?: string
  endpoint?: string
  bucket: string
  s3OptionsPassthrough?: Partial<S3ClientConfig>
}

// AWS_ envs are default for aws-sdk
export function createS3({
  accessKey = process.env.AWS_ACCESS_KEY_ID || process.env.S3_ACCESS_KEY,
  secretKey = process.env.AWS_SECRET_ACCESS_KEY || process.env.S3_SECRET_KEY,
  bucket,
  region = process.env.AWS_REGION || process.env.S3_REGION,
  endpoint,
  s3OptionsPassthrough = {},
}: S3Options): StorageProvider {

  if (!bucket) {
    throw new Error('The STORAGE_PATH environment variable is required for S3')
  }

  const client = new S3Client({
    ...(accessKey && secretKey
      ? {
          credentials: {
            accessKeyId: accessKey,
            secretAccessKey: secretKey,
            sessionToken: process.env.AWS_SESSION_TOKEN,
          },
        }
      : {}),
    ...(region ? { region } : {}),
    ...(endpoint ? { endpoint: endpoint } : {}),
    ...(process.env.NODE_ENV === 'test'
      ? { sslEnabled: false, forcePathStyle: true }
      : {}),
    ...s3OptionsPassthrough,
  })

  return {
    exists: (artifactPath, cb) => {
      client
        .send(
          new HeadObjectCommand({
            Bucket: bucket,
            Key: artifactPath,
          }),
        )
        .then(
          () => {
            cb(null, true)
          },
          (err) => {
            if (err instanceof NotFound) return cb(null, false)
            cb(err, !err)
          },
        )
    },
    createReadStream(artifactPath) {
      const stream = new PassThrough()
      client
        .send(
          new GetObjectCommand({
            Bucket: bucket,
            Key: artifactPath,
          }),
        )
        .then((response) => {
          if (response.Body instanceof Readable) {
            response.Body.pipe(stream)
          }
        })
      return stream
    },
    createWriteStream(artifactPath) {
      const stream = new PassThrough()
      new Upload({
        client,
        params: {
          Bucket: bucket,
          Key: artifactPath,
          Body: stream,
        },
      }).done()
        .then((val) => {
          stream.emit('finish', val);
        })
        .catch((err) => {
          stream.emit('error', err);
        });
      return stream
    },
  }
}
