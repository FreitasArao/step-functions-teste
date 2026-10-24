import { SFNClient } from '@aws-sdk/client-sfn';
import { SQSClient } from '@aws-sdk/client-sqs';

export const SFN_CLIENT = Symbol('SFN_CLIENT');
export const SQS_CLIENT = Symbol('SQS_CLIENT');

export function createSfnClient(): SFNClient {
  const endpoint = process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566';
  const region = process.env.AWS_REGION ?? 'us-east-1';
  return new SFNClient({
    endpoint,
    region,
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test',
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test',
    },
  });
}

export function createSqsClient(): SQSClient {
  const endpoint = process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566';
  const region = process.env.AWS_REGION ?? 'us-east-1';
  return new SQSClient({
    endpoint,
    region,
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test',
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test',
    },
  });
}
