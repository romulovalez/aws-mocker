import { vi } from 'vitest';
import {
	DynamoDBClient,
	CreateTableCommand,
	type CreateTableCommandInput,
	type CreateTableCommandOutput,
	type KeyType,
} from '@aws-sdk/client-dynamodb';
import type {
	BatchGetCommandInput,
	BatchGetCommandOutput,
	BatchWriteCommandInput,
	BatchWriteCommandOutput,
	DeleteCommandInput,
	DeleteCommandOutput,
	GetCommandInput,
	GetCommandOutput,
	NativeAttributeValue,
	PutCommandInput,
	PutCommandOutput,
	QueryCommandInput,
	QueryCommandOutput,
	ScanCommandInput,
	ScanCommandOutput,
} from '@aws-sdk/lib-dynamodb';
import type {
	S3Client,
	CopyObjectCommandInput,
	CopyObjectCommandOutput,
	GetObjectCommandInput,
	GetObjectCommandOutput,
	PutObjectCommandInput,
	PutObjectCommandOutput,
} from '@aws-sdk/client-s3';
import type {
	SNSClient,
	PublishBatchCommandInput,
	PublishBatchCommandOutput,
	PublishCommandInput,
	PublishCommandOutput,
} from '@aws-sdk/client-sns';
import type {
	SQSClient,
	ReceiveMessageCommandInput,
	ReceiveMessageCommandOutput,
	SendMessageBatchCommandInput,
	SendMessageBatchCommandOutput,
	SendMessageCommandInput,
	SendMessageCommandOutput,
} from '@aws-sdk/client-sqs';
import type {
	KMSClient,
	GetPublicKeyCommandInput,
	GetPublicKeyCommandOutput,
	SignCommandInput,
	SignCommandOutput,
} from '@aws-sdk/client-kms';
import { createHash, generateKeyPairSync, randomUUID, sign } from 'node:crypto';
import { Readable } from 'node:stream';
import type { DynamoDBStreamEvent, SQSEvent } from 'aws-lambda';
import { marshall } from '@aws-sdk/util-dynamodb';

/**
 * MARK: Memory store
 */
const tables: Record<string, NativeAttributeValue[]> = {};
const tablesKeys: Record<string, Record<string, KeyType>> = {};
const buckets: Record<string, { Key: string; Body: string }[]> = {};
const topics: Record<string, string[]> = {};
const queues: Record<string, string[]> = {};

function validateTableKeySchema(TableName: string, Key: NativeAttributeValue) {
	if (!tablesKeys[TableName]) throw new Error(`Table '${TableName}' not found, create it first with createTable()`);
	if (!Key) throw new Error('Key is required');

	for (const key of Object.keys(Key)) {
		if (!tablesKeys[TableName][key]) throw new Error(`Key '${key}' is not defined in table '${TableName}'`);
	}
}

function findTableItemIndex(tableName: string, Item: NativeAttributeValue) {
	if (!tables[tableName]) throw new Error(`Table '${tableName}' not found, create it first with createTable()`);
	if (!tablesKeys[tableName]) throw new Error(`Table '${tableName}' not found, create it first with createTable()`);

	const keys = Object.keys(tablesKeys[tableName]);
	return tables[tableName]?.findIndex((item) => keys.every((key) => item[key] === Item[key]));
}

function putTableItem({ TableName, Item }: PutCommandInput) {
	if (!TableName) throw new Error('TableName is required');
	if (!Item) throw new Error('Item is required');
	if (!tables[TableName]) throw new Error(`Table '${TableName}' not found, create it first with createTable()`);

	const indexFound = findTableItemIndex(TableName, Item);
	if (indexFound === -1) {
		tables[TableName].push(Item);
	} else {
		tables[TableName][indexFound] = Item;
	}
}

/**
 * MARK: Helpers
 */
export async function createTable(name: string, options: { primaryIndex: { hashKey: string; rangeKey?: string } }) {
	const primaryIndex = Object.entries(options.primaryIndex);
	await new DynamoDBClient().send(
		new CreateTableCommand({
			TableName: name,
			AttributeDefinitions: [], // For mocking we don't need to define the attributes
			KeySchema: primaryIndex.map(([type, name]) => ({
				AttributeName: name,
				KeyType: type === 'hashKey' ? 'HASH' : 'RANGE',
			})),
		}),
	);
}

export function getTopicMessages<T>(topicArn: string): T[] {
	return topics[topicArn]?.map((message) => JSON.parse(message!)) ?? [];
}

export function getQueueMessages<T>(queueUrl: string): T[] {
	return queues[queueUrl]?.map((message) => JSON.parse(message!)) ?? [];
}

export function subscribeToQueue(queueUrl: string, subscriber: (event: SQSEvent) => void) {
	queues[queueUrl] = new Proxy(queues[queueUrl] ?? [], {
		get(target, prop, receiver) {
			if (prop === 'push') {
				return (...messages: string[]) => {
					for (const message of messages) {
						if (!message) throw new Error(`MessageBody is required, queue: '${queueUrl}`);
						subscriber({ Records: [{ body: message }] } as SQSEvent);
					}

					target.push(...messages);
				};
			}

			return Reflect.get(target, prop, receiver);
		},
	});
}

export function subscribeToTable(tableName: string, subscriber: (event: DynamoDBStreamEvent) => void) {
	tables[tableName] = new Proxy(tables[tableName] ?? [], {
		get(target, prop, receiver) {
			if (prop === 'push') {
				return (...items: NativeAttributeValue[]) => {
					subscriber({
						Records: items.map((item) => ({
							eventID: randomUUID(),
							eventName: 'INSERT',
							dynamodb: {
								NewImage: marshall(item, { removeUndefinedValues: true }),
								StreamViewType: 'NEW_IMAGE',
							},
						})),
					});

					target.push(...items);
				};
			}

			if (prop === 'splice') {
				return (start: number, deleteCount?: number) => {
					subscriber({
						Records: [
							{
								eventID: randomUUID(),
								eventName: 'REMOVE',
								dynamodb: {
									OldImage: marshall(tables[tableName]![start], { removeUndefinedValues: true }),
									StreamViewType: 'OLD_IMAGE',
								},
							},
						],
					});

					target.splice(start, deleteCount);
				};
			}

			return Reflect.get(target, prop, receiver);
		},
		set(target, prop, value, receiver) {
			if (!isNaN(Number(prop))) {
				subscriber({
					Records: [
						{
							eventID: randomUUID(),
							eventName: 'MODIFY',
							dynamodb: {
								OldImage: marshall(target[Number(prop)], { removeUndefinedValues: true }),
								NewImage: marshall(value, { removeUndefinedValues: true }),
								StreamViewType: 'NEW_AND_OLD_IMAGES',
							},
						},
					],
				});
			}

			return Reflect.set(target, prop, value, receiver);
		},
	});
}

// Don't override any arrays with an empty one or we can lose the possible proxy attached,
// instead pop every element without mutating the array
export function clearResources() {
	for (const table in tables) while (tables[table]!.length > 0) tables[table]!.pop();
	for (const bucket in buckets) while (buckets[bucket]!.length > 0) buckets[bucket]!.pop();
	for (const topic in topics) while (topics[topic]!.length > 0) topics[topic]!.pop();
	for (const queue in queues) while (queues[queue]!.length > 0) queues[queue]!.pop();
}

/**
 * MARK: DynamoDB mock
 */
vi.mock('@aws-sdk/client-dynamodb', () => ({
	DynamoDBClient: class {
		send: DynamoDBClient['send'] = async (command) => command.input;
	},

	/**
	 * We need to create tables to know the indexes to properly update the items
	 */
	CreateTableCommand: class {
		input: CreateTableCommandOutput;
		constructor({ TableName, KeySchema }: CreateTableCommandInput) {
			if (!TableName) throw new Error('TableName is required');
			if (!KeySchema) throw new Error('KeySchema is required');

			tables[TableName] = [];
			tablesKeys[TableName] = {};

			for (const { KeyType, AttributeName } of KeySchema) {
				if (KeyType == null) throw new Error('KeyType is required');
				if (AttributeName == null) throw new Error('AttributeName is required');

				tablesKeys[TableName]![AttributeName] = KeyType;
			}

			this.input = { $metadata: {} };
		}
	},
}));

vi.mock('@aws-sdk/lib-dynamodb', () => ({
	DynamoDBDocumentClient: {
		from: (client: DynamoDBClient) => client,
	},

	PutCommand: class {
		input: PutCommandOutput;
		constructor(command: PutCommandInput) {
			putTableItem(command);

			this.input = { $metadata: {} };
		}
	},

	GetCommand: class {
		input: GetCommandOutput;
		constructor({ TableName, Key }: GetCommandInput) {
			if (!TableName) throw new Error('TableName is required');
			if (!Key) throw new Error('Key is required');

			validateTableKeySchema(TableName, Key);

			this.input = { $metadata: {}, Item: tables[TableName]?.[findTableItemIndex(TableName, Key)] };
		}
	},

	BatchGetCommand: class {
		input: BatchGetCommandOutput;
		constructor({ RequestItems }: BatchGetCommandInput) {
			if (!RequestItems) throw new Error('RequestItems is required');

			const TableName = Object.keys(RequestItems)[0]!;
			const Keys = RequestItems[TableName]?.Keys;
			if (!Keys) throw new Error('RequestItems Keys is required');

			this.input = {
				$metadata: {},
				Responses: {
					[TableName]:
						tables[TableName]?.filter((item) =>
							Keys.some((Key) => {
								validateTableKeySchema(TableName, Key);

								return Object.keys(Key).every((key) => item[key] === Key[key]);
							}),
						) ?? [],
				},
			};
		}
	},

	BatchWriteCommand: class {
		input: BatchWriteCommandOutput;
		constructor({ RequestItems }: BatchWriteCommandInput) {
			if (!RequestItems) throw new Error('RequestItems is required');

			const TableName = Object.keys(RequestItems)[0]!;
			const Items = RequestItems[TableName]?.map(({ PutRequest }) => PutRequest?.Item);
			if (!Items) throw new Error('RequestItems Items is required');

			Items.forEach((Item) => putTableItem({ TableName, Item }));

			this.input = { $metadata: {} };
		}
	},

	QueryCommand: class {
		input: QueryCommandOutput;
		constructor(input: QueryCommandInput) {
			if (!input.TableName) throw new Error('TableName is required');
			if (!input.KeyConditionExpression) throw new Error('KeyConditionExpression is required');
			if (input.FilterExpression) throw new Error('TEST MOCK ERROR: FilterExpression is not supported yet');

			/**
			 * This are the possible values for KeyConditionExpression:
			 * a) #pk = :pk
			 * c) #pk = :pk and #sk = :sk
			 * d) #pk = :pk and #sk <= :sk
			 * e) #pk = :pk and #sk >= :sk
			 * f) #pk = :pk and #sk between :from and :to
			 */
			const [firstCondition, ...secondConditionSplitted] = input.KeyConditionExpression.split(' and ');
			const secondCondition = secondConditionSplitted.join(' and ');

			const result = tables[input.TableName]?.filter((item) =>
				[firstCondition, secondCondition].every((condition) => {
					if (!condition) return true;

					const [nameKey, operator, firstValueKey, _and, secondValueKey] = condition.split(' ');

					const name = nameKey!.startsWith('#') ? input.ExpressionAttributeNames?.[nameKey!] : nameKey;
					if (!name) throw new Error(`Attribute '${nameKey}' not found in ExpressionAttributeNames`);

					if (operator !== '=' && operator !== '<=' && operator !== '>=' && operator !== 'between') {
						throw new Error(`Unsupported operator: ${operator}`);
					}

					const firstValue = input.ExpressionAttributeValues?.[firstValueKey!];
					if (firstValueKey && !firstValue) {
						throw new Error(`Attribute '${firstValueKey}' not found in ExpressionAttributeValues`);
					}

					const secondValue = input.ExpressionAttributeValues?.[secondValueKey!];
					if (secondValueKey && !secondValue) {
						throw new Error(`Attribute '${secondValueKey}' not found in ExpressionAttributeValues`);
					}

					if (operator === '=') return item[name!] === firstValue;
					if (operator === '<=') return item[name!] <= firstValue;
					if (operator === '>=') return item[name!] >= firstValue;
					if (operator === 'between') return item[name!] >= firstValue && item[name!] <= secondValue;
				}),
			);

			this.input = {
				$metadata: {},
				Items: result ?? [],
				Count: input.Select === 'COUNT' ? (result?.length ?? 0) : undefined,
			};
		}
	},

	ScanCommand: class {
		input: ScanCommandOutput;
		constructor({ TableName }: ScanCommandInput) {
			if (!TableName) throw new Error('TableName is required');

			this.input = { $metadata: {}, Items: tables[TableName] ?? [] };
		}
	},

	DeleteCommand: class {
		input: DeleteCommandOutput;
		constructor({ TableName, Key }: DeleteCommandInput) {
			if (!TableName) throw new Error('TableName is required');
			if (!Key) throw new Error('Key is required');

			validateTableKeySchema(TableName, Key);

			const indexFound = findTableItemIndex(TableName, Key);
			if (indexFound !== -1) {
				tables[TableName]!.splice(indexFound!, 1);
			}

			this.input = { $metadata: {} };
		}
	},
}));

/**
 * MARK: S3 mock
 */
vi.mock('@aws-sdk/client-s3', () => ({
	S3Client: class {
		send: S3Client['send'] = async (command) => command.input;
	},

	PutObjectCommand: class {
		input: PutObjectCommandOutput;
		// TODO: We assert Body as string for simplicity, but it can be a lot of things
		constructor({ Bucket, Key, Body }: PutObjectCommandInput & { Body: string }) {
			if (!Bucket) throw new Error('Bucket is required');
			if (!Key) throw new Error('Key is required');
			if (!Body) throw new Error('Body is required');

			if (!buckets[Bucket]) {
				buckets[Bucket] = [{ Key, Body }];
			} else {
				const indexFound = buckets[Bucket].findIndex((item) => item.Key === Key);
				if (indexFound === -1) {
					buckets[Bucket].push({ Key, Body });
				} else {
					buckets[Bucket][indexFound] = { Key, Body };
				}
			}

			this.input = { $metadata: {} };
		}
	},

	GetObjectCommand: class {
		input: GetObjectCommandOutput;
		constructor({ Bucket, Key }: GetObjectCommandInput) {
			if (!Bucket) throw new Error('Bucket is required');
			if (!Key) throw new Error('Key is required');

			const file = buckets[Bucket]?.find((item) => item.Key === Key);
			if (!file) throw new Error(`UNEXPECTED_FILE_FORMAT`);

			const Body: Readable & {
				transformToByteArray?: () => Promise<Uint8Array>;
				transformToString?: () => Promise<string>;
				transformToWebStream?: () => ReadableStream;
			} = Readable.from([file.Body]);

			Body.transformToByteArray = async () => Buffer.from(file.Body);
			Body.transformToString = async () => file.Body;
			Body.transformToWebStream = () => new Blob([file.Body]).stream();

			this.input = { $metadata: {}, Body: Body as GetObjectCommandOutput['Body'] };
		}
	},

	CopyObjectCommand: class {
		input: CopyObjectCommandOutput;
		constructor({ Bucket, Key, CopySource }: CopyObjectCommandInput) {
			if (!Bucket) throw new Error('Bucket is required');
			if (!Key) throw new Error('Key is required');
			if (!CopySource) throw new Error('CopySource is required');

			const [sourceBucket, ...sourceKeySplittedBySlashes] = CopySource.split('/');
			const sourceKey = sourceKeySplittedBySlashes.join('/');

			const file = buckets[sourceBucket!]?.find((item) => item.Key === sourceKey);
			if (!file) throw new Error(`UNEXPECTED_FILE_FORMAT`);

			if (!buckets[Bucket]) {
				buckets[Bucket] = [{ Key, Body: file.Body }];
			} else {
				const indexFound = buckets[Bucket].findIndex((item) => item.Key === Key);
				if (indexFound === -1) {
					buckets[Bucket].push({ Key, Body: file.Body });
				} else {
					buckets[Bucket][indexFound] = { Key, Body: file.Body };
				}
			}

			this.input = { $metadata: {} };
		}
	},
}));

/**
 * MARK: SNS mock
 */
vi.mock('@aws-sdk/client-sns', () => ({
	SNSClient: class {
		send: SNSClient['send'] = async (command) => command.input;
	},

	PublishCommand: class {
		input: PublishCommandOutput;
		constructor({ TopicArn, Message }: PublishCommandInput) {
			if (!Message) throw new Error('Message is required');
			if (!TopicArn) throw new Error('TopicArn is required');

			if (topics[TopicArn]) {
				topics[TopicArn]!.push(Message);
			} else {
				topics[TopicArn] = [Message];
			}

			this.input = { $metadata: {}, MessageId: randomUUID() };
		}
	},

	PublishBatchCommand: class {
		input: PublishBatchCommandOutput;
		constructor({ TopicArn, PublishBatchRequestEntries }: PublishBatchCommandInput) {
			if (!TopicArn) throw new Error('TopicArn is required');
			if (!PublishBatchRequestEntries) throw new Error('PublishBatchRequestEntries is required');

			if (topics[TopicArn]) {
				topics[TopicArn].push(...PublishBatchRequestEntries.map(({ Message }) => Message!));
			} else {
				topics[TopicArn] = PublishBatchRequestEntries.map(({ Message }) => Message!);
			}

			this.input = {
				$metadata: {},
				Successful: PublishBatchRequestEntries.map(({ Id }) => ({ Id, MessageId: randomUUID() })),
			};
		}
	},
}));

/**
 * MARK: SQS mock
 */
vi.mock('@aws-sdk/client-sqs', () => ({
	SQSClient: class {
		send: SQSClient['send'] = async (command) => command.input;
	},

	SendMessageBatchCommand: class {
		input: SendMessageBatchCommandOutput;
		constructor({ QueueUrl, Entries }: SendMessageBatchCommandInput) {
			if (!QueueUrl) throw new Error('QueueUrl is required');
			if (!Entries) throw new Error('Entries is required');

			if (queues[QueueUrl]) {
				queues[QueueUrl]!.push(...Entries.map(({ MessageBody }) => MessageBody!));
			} else {
				queues[QueueUrl] = Entries.map(({ MessageBody }) => MessageBody!);
			}

			this.input = {
				$metadata: {},
				Failed: [],
				Successful: Entries.map(({ Id, MessageBody }) => ({
					Id,
					MessageId: Id,
					MD5OfMessageBody: createHash('md5').update(Buffer.from(MessageBody!)).digest('hex'),
				})),
			};
		}
	},

	SendMessageCommand: class {
		input: SendMessageCommandOutput;
		constructor({ QueueUrl, MessageBody }: SendMessageCommandInput) {
			if (!QueueUrl) throw new Error('QueueUrl is required');
			if (!MessageBody) throw new Error('MessageBody is required');

			if (queues[QueueUrl]) {
				queues[QueueUrl]!.push(MessageBody);
			} else {
				queues[QueueUrl] = [MessageBody];
			}

			this.input = { $metadata: {}, MessageId: randomUUID() };
		}
	},

	ReceiveMessageCommand: class {
		input: ReceiveMessageCommandOutput;
		constructor({ QueueUrl }: ReceiveMessageCommandInput) {
			if (!QueueUrl) throw new Error('QueueUrl is required');

			this.input = { $metadata: {}, Messages: queues[QueueUrl]?.map((Body) => ({ Body })) ?? [] };
		}
	},
}));

/**
 * MARK: KMS mock
 */
const { privateKey, publicKey } = generateKeyPairSync('rsa', {
	modulusLength: 2048,
	publicKeyEncoding: { type: 'spki', format: 'pem' },
	privateKeyEncoding: { type: 'pkcs1', format: 'pem' },
});

vi.mock('@aws-sdk/client-kms', () => ({
	KMSClient: class {
		send: KMSClient['send'] = async (command) => command.input;
	},

	SignCommand: class {
		input: SignCommandOutput;
		constructor({ KeyId, Message }: SignCommandInput) {
			if (!KeyId) throw new Error('KeyId is required');
			if (!Message) throw new Error('Message is required');

			this.input = { $metadata: {}, Signature: sign('sha256', Message, { key: privateKey }) };
		}
	},

	GetPublicKeyCommand: class {
		input: GetPublicKeyCommandOutput;
		constructor({ KeyId }: GetPublicKeyCommandInput) {
			if (!KeyId) throw new Error('KeyId is required');

			this.input = {
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
