import { cleanBuckets, cleanTopics, cleanQueues, getTopicMessages, createTable, cleanTables } from './mock';
import { beforeAll, beforeEach, describe, expect, test } from 'vitest';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { PutCommand, GetCommand, BatchGetCommand } from '@aws-sdk/lib-dynamodb';
import { GetObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { GetPublicKeyCommand, KMSClient, SignCommand } from '@aws-sdk/client-kms';
import { SNSClient, PublishCommand } from '@aws-sdk/client-sns';
import { SQSClient, SendMessageCommand, ReceiveMessageCommand } from '@aws-sdk/client-sqs';
import jwt, { JwtPayload } from 'jsonwebtoken';
import base64url from 'base64url';

const client = new DynamoDBClient();
const s3Client = new S3Client();
const snsClient = new SNSClient();
const sqsClient = new SQSClient();
const kmsClient = new KMSClient();

describe('DynamoDB', () => {
	beforeEach(async () => {
		cleanTables();
		await createTable('TableName', { primaryIndex: { hashKey: 'pk', rangeKey: 'sk' } });
	});

	test('PutCommand && GetCommand', async () => {
		await client.send(
			new PutCommand({
				TableName: 'TableName',
				Item: { pk: 'item1', sk: 'sk' },
			}),
		);

		const shouldItemExist = await client.send(
			new GetCommand({
				TableName: 'TableName',
				Key: { pk: 'item1', sk: 'sk' },
			}),
		);

		expect(shouldItemExist.Item).toStrictEqual({ pk: 'item1', sk: 'sk' });

		await client.send(
			new PutCommand({
				TableName: 'TableName',
				Item: { pk: 'item1', sk: 'sk', foo: 'bar' },
			}),
		);

		const shouldItemOverride = await client.send(
			new GetCommand({
				TableName: 'TableName',
				Key: { pk: 'item1', sk: 'sk' },
			}),
		);

		expect(shouldItemOverride.Item).toStrictEqual({
			pk: 'item1',
			sk: 'sk',
			foo: 'bar',
		});
	});

	test('GetBatchCommand', async () => {
		await client.send(
			new PutCommand({
				TableName: 'TableName',
				Item: { pk: 'item1', sk: 'sk' },
			}),
		);

		const oneItem = await client.send(
			new BatchGetCommand({
				RequestItems: {
					['TableName']: {
						Keys: [{ pk: 'item1', sk: 'sk' }],
					},
				},
			}),
		);

		expect(oneItem.Responses).toStrictEqual({
			['TableName']: [{ pk: 'item1', sk: 'sk' }],
		});

		await client.send(
			new PutCommand({
				TableName: 'TableName',
				Item: { pk: 'item1', sk: 'sk', foo: 'bar' },
			}),
		);

		const stillOneItem = await client.send(
			new BatchGetCommand({
				RequestItems: {
					['TableName']: {
						Keys: [{ pk: 'item1', sk: 'sk' }],
					},
				},
			}),
		);

		expect(stillOneItem.Responses).toStrictEqual({
			['TableName']: [{ pk: 'item1', sk: 'sk', foo: 'bar' }],
		});
	});
});

describe('S3', () => {
	beforeEach(() => {
		cleanBuckets();
	});

	test('PutObjectCommand && GetObjectCommand', async () => {
		await s3Client.send(
			new PutObjectCommand({
				Bucket: 'BucketName',
				Key: 'file.txt',
				Body: 'some text\n',
			}),
		);

		const file = await s3Client.send(new GetObjectCommand({ Bucket: 'BucketName', Key: 'file.txt' }));
		expect(await file.Body?.transformToString()).toStrictEqual('some text\n');

		await s3Client.send(
			new PutObjectCommand({
				Bucket: 'BucketName',
				Key: 'file.txt',
				Body: 'overrided text!\n',
			}),
		);

		const overridedFile = await s3Client.send(new GetObjectCommand({ Bucket: 'BucketName', Key: 'file.txt' }));
		expect(await overridedFile.Body?.transformToString()).toStrictEqual('overrided text!\n');
	});
});

describe('SNS', () => {
	beforeEach(() => {
		cleanTopics();
	});

	test('PublishCommand & check if message gets to topic', async () => {
		const message = { foo: 'bar' };

		const publishResult = await snsClient.send(
			new PublishCommand({
				TopicArn: 'TopicArn',
				Message: JSON.stringify(message),
			}),
		);

		expect(publishResult.MessageId).toBeDefined();

		const topicMessages = getTopicMessages('TopicArn');
		expect(topicMessages).toStrictEqual([
			{
				TopicArn: 'TopicArn',
				Message: JSON.stringify(message),
			},
		]);
	});
});

describe('SQS', () => {
	beforeAll(() => {
		cleanQueues();
	});

	test('SendMessageCommand && ReceiveMessageCommand', async () => {
		const messageBody = { foo: 'bar' };

		const sendMessageResult = await sqsClient.send(
			new SendMessageCommand({
				QueueUrl: 'QueueUrl',
				MessageBody: JSON.stringify(messageBody),
			}),
		);

		expect(sendMessageResult.MessageId).toBeDefined();

		const receiveMessageResult = await sqsClient.send(
			new ReceiveMessageCommand({
				QueueUrl: 'QueueUrl',
				MaxNumberOfMessages: 1,
			}),
		);

		expect(receiveMessageResult.Messages).toStrictEqual([
			{
				QueueUrl: 'QueueUrl',
				MessageBody: JSON.stringify(messageBody),
			},
		]);
	});
});

describe('KMS', () => {
	test('SignCommand && GetPublicKeyCommand', async () => {
		const payload = {
			foo: 'bar',
			iss: 'company/provider/api',
			sub: 'company/provider/012345678/',
			iat: 1734288132,
			aud: 'company/provider/api',
		};

		const signature = await sign(payload);
		expect(signature).toBeDefined();

		const result = await verify(signature);
		expect(result).toStrictEqual(payload);
	});
});

async function sign(payload: JwtPayload) {
	const headers: jwt.JwtHeader = {
		alg: 'RS256',
		typ: 'JWT',
	};

	const headersHash = base64url(JSON.stringify(headers));
	const payloadHash = base64url(JSON.stringify(payload));

	return `${headersHash}.${payloadHash}.${await buildSignature(`${headersHash}.${payloadHash}`)}`;
}

async function buildSignature(message: string) {
	const res = await kmsClient.send(
		new SignCommand({
			Message: Buffer.from(message),
			KeyId: 'KmsKeyIdValue',
			SigningAlgorithm: 'RSASSA_PKCS1_V1_5_SHA_256',
			MessageType: 'RAW',
		}),
	);
	return Buffer.from(res.Signature!).toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
}

async function verify(token: string) {
	const getPublicKeyResponse = await kmsClient.send(
		new GetPublicKeyCommand({
			KeyId: 'KmsKeyIdValue',
		}),
	);
	const publicKey = Buffer.from(getPublicKeyResponse.PublicKey!).toString('base64');
	const pemPublicKey = convertToPem(publicKey);

	return jwt.verify(token, pemPublicKey);
}

function convertToPem(base64Key: string) {
	return `-----BEGIN PUBLIC KEY-----\n${base64Key.match(/.{1,64}/g)!.join('\n')}\n-----END PUBLIC KEY-----`;
}
