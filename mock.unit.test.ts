import { cleanResources, getTopicMessages, createTable } from './mock';
import { beforeEach, describe, expect, test } from 'vitest';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { PutCommand, GetCommand, BatchGetCommand, QueryCommand } from '@aws-sdk/lib-dynamodb';
import { CopyObjectCommand, GetObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
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
	beforeEach(() => {
		cleanResources();
		createTable('TableName', { primaryIndex: { hashKey: 'pk', rangeKey: 'sk' } });
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

	describe('QueryCommand', () => {
		beforeEach(() => {
			cleanResources();
			createTable('TableName', { primaryIndex: { hashKey: 'pk', rangeKey: 'sk' } });

			client.send(
				new PutCommand({
					TableName: 'TableName',
					Item: { pk: 'item1', sk: 'sk1', merchantId: 'merchant1', confirmedAt: 1620000000 },
				}),
			);

			client.send(
				new PutCommand({
					TableName: 'TableName',
					Item: { pk: 'item1', sk: 'sk2', merchantId: 'merchant1', confirmedAt: 1625000000 },
				}),
			);
		});

		test('QueryCommand: simple case', async () => {
			const multipleResults = await client.send(
				new QueryCommand({
					TableName: 'TableName',
					KeyConditionExpression: 'pk = :pk',
					ExpressionAttributeValues: {
						':pk': 'item1',
					},
				}),
			);

			expect(multipleResults.Items).toStrictEqual([
				{ pk: 'item1', sk: 'sk1', merchantId: 'merchant1', confirmedAt: 1620000000 },
				{ pk: 'item1', sk: 'sk2', merchantId: 'merchant1', confirmedAt: 1625000000 },
			]);

			const singleFilteredResult = await client.send(
				new QueryCommand({
					TableName: 'TableName',
					KeyConditionExpression: 'sk = :sk',
					ExpressionAttributeValues: {
						':sk': 'sk2',
					},
				}),
			);

			expect(singleFilteredResult.Items).toStrictEqual([
				{ pk: 'item1', sk: 'sk2', merchantId: 'merchant1', confirmedAt: 1625000000 },
			]);
		});

		test('QueryCommand: and / between cases', async () => {
			const queryResultWithCondition = await client.send(
				new QueryCommand({
					TableName: 'TableName',
					KeyConditionExpression: '#merchantId = :merchantId and #confirmedAt between :from and :to',
					ExpressionAttributeNames: {
						'#merchantId': 'merchantId',
						'#confirmedAt': 'confirmedAt',
					},
					ExpressionAttributeValues: {
						':merchantId': 'merchant1',
						':from': 1610000000,
						':to': 1620000000,
					},
				}),
			);

			expect(queryResultWithCondition.Items).toStrictEqual([
				{ pk: 'item1', sk: 'sk1', merchantId: 'merchant1', confirmedAt: 1620000000 },
			]);
		});
	});
});

describe('S3', () => {
	beforeEach(() => {
		cleanResources();
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

	test('CopyObjectCommand', async () => {
		await s3Client.send(
			new PutObjectCommand({
				Bucket: 'SourceBucket',
				Key: 'sourceFile.txt',
				Body: 'source text\n',
			}),
		);

		await s3Client.send(
			new CopyObjectCommand({
				Bucket: 'DestinationBucket',
				CopySource: 'SourceBucket/sourceFile.txt',
				Key: 'destinationFile.txt',
			}),
		);

		const copiedFile = await s3Client.send(
			new GetObjectCommand({ Bucket: 'DestinationBucket', Key: 'destinationFile.txt' }),
		);
		expect(await copiedFile.Body?.transformToString()).toStrictEqual('source text\n');
	});
});

describe('SNS', () => {
	beforeEach(() => {
		cleanResources();
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
		expect(topicMessages).toStrictEqual([message]);
	});
});

describe('SQS', () => {
	beforeEach(() => {
		cleanResources();
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
