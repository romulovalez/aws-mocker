import { vi } from 'vitest';
import {
	BatchGetCommandInput,
	BatchGetCommandOutput,
	GetCommandInput,
	GetCommandOutput,
	PutCommandInput,
	PutCommandOutput,
} from '@aws-sdk/lib-dynamodb';
import {
	GetObjectCommandInput,
	GetObjectCommandOutput,
	PutObjectCommandInput,
	PutObjectCommandOutput,
} from '@aws-sdk/client-s3';
import {
	CreateTableCommand,
	CreateTableCommandInput,
	CreateTableCommandOutput,
	DynamoDBClient,
	KeyType,
} from '@aws-sdk/client-dynamodb';
import { StreamingBlobPayloadOutputTypes } from '@smithy/types';
import { DocumentClient } from 'aws-sdk/clients/dynamodb';
import { PublishCommandInput, PublishCommandOutput } from '@aws-sdk/client-sns';
import {
	ReceiveMessageCommandInput,
	ReceiveMessageCommandOutput,
	SendMessageBatchCommandInput,
	SendMessageBatchCommandOutput,
	SendMessageBatchRequestEntry,
	SendMessageCommandInput,
	SendMessageCommandOutput,
} from '@aws-sdk/client-sqs';
import {
	GetPublicKeyCommandInput,
	GetPublicKeyCommandOutput,
	SignCommandInput,
	SignCommandOutput,
} from '@aws-sdk/client-kms';
import { createHash, generateKeyPairSync, randomUUID, sign } from 'crypto';

/**
 * MARK: memory storage
 */
let tables: Record<string, Array<DocumentClient.AttributeMap>> = {};
let tablesDefinitions: Record<string, { KeySchema: Record<string, KeyType> }> = {};
let buckets: Record<string, Array<{ Key: string; Body: string }>> = {};
let topics: Record<string, Array<PublishCommandInput>> = {};
let queues: Record<string, Array<SendMessageBatchRequestEntry | SendMessageCommandInput>> = {};

export function internal_DynamoDBPutCommand({ TableName, Item }: PutCommandInput): PutCommandOutput {
	if (!TableName) throw new Error('TableName is required');
	if (!Item) throw new Error('Item is required');
	if (!tables[TableName]) throw new Error(`Table '${TableName}' not found, create it first with createTable()`);

	const KeySchema = tablesDefinitions[TableName]?.KeySchema;
	if (!KeySchema) throw new Error(`Table '${TableName}' not found, create it first with createTable()`);

	const keys = Object.keys(KeySchema);
	const indexFound = tables[TableName].findIndex((item) => keys.every((key) => item[key] === Item[key]));
	if (indexFound === -1) {
		tables[TableName].push(Item);
	} else {
		tables[TableName][indexFound] = Item;
	}

	return { $metadata: {} };
}

function validateTableKeySchema(TableName: string, Key: Record<string, any>) {
	const KeySchema = tablesDefinitions[TableName]?.KeySchema;
	if (!KeySchema) throw new Error(`Table '${TableName}' not found, create it first with createTable()`);

	const keys = Object.keys(Key);
	for (const key of keys) {
		if (!KeySchema[key]) throw new Error(`Key '${key}' is not defined in table '${TableName}'`);
	}
}

/**
 * MARK: Helpers
 */
export function cleanTables() {
	tables = {};
	tablesDefinitions = {};
}

export function cleanBuckets() {
	buckets = {};
}

export function cleanTopics() {
	topics = {};
}

export function cleanQueues() {
	queues = {};
}

export type Queues = typeof queues;

export function subscribeToQueue(cb: (queuesRef: Queues) => void) {
	cb(queues);
}

export function getTopicMessages(name: string) {
	return topics[name];
}

export async function createTable(name: string, options: { primaryIndex: { hashKey: string; rangeKey?: string } }) {
	const primaryIndex = Object.entries(options.primaryIndex);
	await new DynamoDBClient().send(
		new CreateTableCommand({
			TableName: name,
			AttributeDefinitions: [], // For now we don't need to define the attributes for mocking
			KeySchema: primaryIndex.map(([type, name]) => ({
				AttributeName: name,
				KeyType: type === 'hashKey' ? 'HASH' : 'RANGE',
			})),
		}),
	);
}

/**
 * MARK: DynamoDB mock
 */
vi.mock('@aws-sdk/client-dynamodb', async () => ({
	DynamoDBClient: class DynamoDBClient {
		send: <T>(command: { value: T }) => Promise<T> = async (command) => {
			return command.value;
		};
	},

	/**
	 * Need to create tables in mock to know the indexes to properly query the tables
	 */
	CreateTableCommand: class CreateTableCommand {
		value: CreateTableCommandOutput;
		constructor(command: CreateTableCommandInput) {
			if (!command.TableName) throw new Error('TableName is required');
			if (!command.KeySchema) throw new Error('KeySchema is required');

			tables[command.TableName] = [];
			tablesDefinitions[command.TableName] = { KeySchema: {} };

			for (const key of command.KeySchema) {
				if (key.KeyType == null) throw new Error('KeyType is required');
				if (key.AttributeName == null) throw new Error('AttributeName is required');

				tablesDefinitions[command.TableName]!.KeySchema[key.AttributeName] = key.KeyType;
			}

			this.value = { $metadata: {} };
		}
	},
}));

vi.mock('@aws-sdk/lib-dynamodb', async () => ({
	DynamoDBDocumentClient: {
		from(client: DynamoDBClient) {
			return client;
		},
	},

	PutCommand: class PutCommand {
		value: PutCommandOutput;
		constructor(command: PutCommandInput) {
			this.value = internal_DynamoDBPutCommand(command);
		}
	},

	GetCommand: class GetCommand {
		value: GetCommandOutput;
		constructor({ TableName, Key }: GetCommandInput) {
			if (!TableName) throw new Error('TableName is required');
			if (!Key) throw new Error('Key is required');

			validateTableKeySchema(TableName, Key);

			const keys = Object.keys(Key);
			const result = tables[TableName]?.find((item) => keys.every((key) => item[key] === Key[key]));

			this.value = { $metadata: {}, Item: result };
		}
	},

	BatchGetCommand: class BatchGetCommand {
		value: BatchGetCommandOutput;
		constructor({ RequestItems }: BatchGetCommandInput) {
			if (!RequestItems) throw new Error('RequestItems is required');

			const TableName = Object.keys(RequestItems)[0]!;
			const Keys = RequestItems[TableName]?.Keys;
			if (!Keys) throw new Error('RequestItems Keys is required');

			const result =
				tables[TableName]?.filter((item) =>
					Keys.some((Key) => {
						validateTableKeySchema(TableName, Key);

						const keys = Object.keys(Key);
						return keys.every((subkey) => item[subkey] === Key[subkey]);
					}),
				) ?? [];

			this.value = { $metadata: {}, Responses: { [TableName]: result } };
		}
	},
}));

/**
 * MARK: S3 mock
 */
vi.mock('@aws-sdk/client-s3', async () => ({
	S3Client: class S3Client {
		send: <T>(command: { value: T }) => Promise<T> = async (command) => {
			return command.value;
		};
	},

	PutObjectCommand: class PutObjectCommand {
		value: PutObjectCommandOutput;
		constructor({ Bucket, Key, Body }: PutObjectCommandInput) {
			if (!Bucket) throw new Error('Bucket is required');
			if (!Key) throw new Error('Key is required');
			if (!Body) throw new Error('Body is required');

			const file = { Key, Body: Body as string };

			if (!buckets[Bucket]) {
				buckets[Bucket] = [file];
			} else {
				const indexFound = buckets[Bucket].findIndex((item) => item.Key === Key);
				if (indexFound === -1) {
					buckets[Bucket].push(file);
				} else {
					buckets[Bucket][indexFound] = file;
				}
			}

			this.value = { $metadata: {} };
		}
	},

	GetObjectCommand: class GetObjectCommand {
		value: GetObjectCommandOutput;
		constructor({ Bucket, Key }: GetObjectCommandInput) {
			if (!Bucket) throw new Error('Bucket is required');
			if (!Key) throw new Error('Key is required');

			const result = buckets[Bucket]?.find((item) => item.Key === Key);

			this.value = {
				$metadata: {},
				Body: !result?.Body
					? undefined
					: ({
							transformToByteArray: async (): Promise<Uint8Array> => Buffer.from(result.Body),
							transformToString: async (): Promise<string> => result.Body,
							transformToWebStream: (): ReadableStream => new Blob([result.Body]).stream(),
							// TODO: there are some inconsistencies in the transformToWebStream method, check it
						} as StreamingBlobPayloadOutputTypes),
			};
		}
	},
}));

/**
 * MARK: SNS mock
 */
vi.mock('@aws-sdk/client-sns', async () => ({
	SNSClient: class SNSClient {
		send: <T>(command: { value: T }) => Promise<T> = async (command) => {
			return command.value;
		};
	},

	PublishCommand: class PublishCommand {
		value: PublishCommandOutput;
		constructor(input: PublishCommandInput) {
			if (!input.Message) throw new Error('Message is required');
			if (!input.TopicArn) throw new Error('TopicArn is required');

			if (topics[input.TopicArn]) {
				topics[input.TopicArn]!.push(input);
			} else {
				topics[input.TopicArn] = [input];
			}

			this.value = {
				$metadata: {},
				MessageId: randomUUID(),
			};
		}
	},
}));

/**
 * MARK: SQS mock
 */
vi.mock('@aws-sdk/client-sqs', async () => ({
	SQSClient: class SQSClient {
		send: <T>(command: { value: T }) => Promise<T> = async (command) => {
			return command.value;
		};
	},

	SendMessageBatchCommand: class SendMessageBatchCommand {
		value: SendMessageBatchCommandOutput;
		constructor(input: SendMessageBatchCommandInput) {
			if (!input.QueueUrl) throw new Error('QueueUrl is required');
			if (!input.Entries) throw new Error('Entries is required');

			if (queues[input.QueueUrl]) {
				queues[input.QueueUrl]!.push(...input.Entries);
			} else {
				queues[input.QueueUrl] = input.Entries;
			}

			this.value = {
				$metadata: {},
				Failed: [],
				Successful: input.Entries.map((entry) => ({
					Id: entry.Id,
					MessageId: entry.Id,
					MD5OfMessageBody: createHash('md5').update(Buffer.from(entry.MessageBody!)).digest('hex'),
				})),
			};
		}
	},

	SendMessageCommand: class SendMessageCommand {
		value: SendMessageCommandOutput;
		constructor(input: SendMessageCommandInput) {
			if (!input.QueueUrl) throw new Error('QueueUrl is required');
			if (!input.MessageBody) throw new Error('MessageBody is required');

			if (queues[input.QueueUrl]) {
				queues[input.QueueUrl]!.push(input);
			} else {
				queues[input.QueueUrl] = [input];
			}

			this.value = {
				$metadata: {},
				MessageId: randomUUID(),
			};
		}
	},

	ReceiveMessageCommand: class ReceiveMessageCommand {
		value: ReceiveMessageCommandOutput;
		constructor(input: ReceiveMessageCommandInput) {
			if (!input.QueueUrl) throw new Error('QueueUrl is required');

			this.value = {
				$metadata: {},
				Messages: queues[input.QueueUrl],
			};
		}
	},
}));

/**
 * MARK: KMS mock
 */
const { privateKey, publicKey } = generateKeyPairSync('rsa', {
	modulusLength: 2048,
	publicKeyEncoding: {
		type: 'spki',
		format: 'pem',
	},
	privateKeyEncoding: {
		type: 'pkcs1',
		format: 'pem',
	},
});

vi.mock('@aws-sdk/client-kms', async () => ({
	KMSClient: class KMSClient {
		send: <T>(command: { value: T }) => Promise<T> = async (command) => {
			return command.value;
		};
	},

	SignCommand: class SignCommand {
		value: SignCommandOutput;
		constructor({ KeyId, Message }: SignCommandInput) {
			if (!KeyId) throw new Error('KeyId is required');
			if (!Message) throw new Error('Message is required');

			this.value = {
				$metadata: {},
				Signature: sign('sha256', Message, { key: privateKey }),
			};
		}
	},

	GetPublicKeyCommand: class GetPublicKeyCommand {
		value: GetPublicKeyCommandOutput;
		constructor({ KeyId }: GetPublicKeyCommandInput) {
			if (!KeyId) throw new Error('KeyId is required');

			this.value = {
				$metadata: {},
				PublicKey: Buffer.from(
					publicKey
						.replace('-----BEGIN PUBLIC KEY-----', '')
						.replace('-----END PUBLIC KEY-----', '')
						.replaceAll('\n', ''),
					'base64',
				),
			};
		}
	},
}));
