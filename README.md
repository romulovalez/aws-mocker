# AWS Mocker

AWS mock library that creates aws resources in-memory for lightning fast unit testing.

## Example

```typescript
import 'aws-mocker';
import { test, expect } from 'vitest';
import { DynamoDBClient, CreateTableCommand } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand, QueryCommand } from '@aws-sdk/lib-dynamodb';

test('DynamoDB', async () => {
	const client = new DynamoDBClient();
	const ddbClient = DynamoDBDocumentClient.from(client);

	await client.send(
		new CreateTableCommand({
			TableName: 'TableName',
			AttributeDefinitions: [],
			KeySchema: [
				{ AttributeName: 'pk', KeyType: 'HASH' },
				{ AttributeName: 'sk', KeyType: 'RANGE' },
			],
		}),
	);

	await ddbClient.send(
		new PutCommand({
			TableName: 'TableName',
			Item: { pk: 'pk', sk: 'CREATED:2024-12-30T23:00:00' },
		}),
	);

	await ddbClient.send(
		new PutCommand({
			TableName: 'TableName',
			Item: { pk: 'pk', sk: 'CREATED:2024-12-31T00:00:00' },
		}),
	);

	expect(
		(
			await ddbClient.send(
				new QueryCommand({
					TableName: 'TableName',
					KeyConditionExpression: 'pk = :pk',
					ExpressionAttributeValues: { ':pk': 'pk' },
				}),
			)
		).Items,
	).toStrictEqual([
		{ pk: 'pk', sk: 'CREATED:2024-12-30T23:00:00' },
		{ pk: 'pk', sk: 'CREATED:2024-12-31T00:00:00' },
	]);

	expect(
		(
			await ddbClient.send(
				new QueryCommand({
					TableName: 'TableName',
					KeyConditionExpression: '#pk = :pk and #sk between :from and :to',
					ExpressionAttributeNames: {
						'#pk': 'pk',
						'#sk': 'sk',
					},
					ExpressionAttributeValues: {
						':pk': 'pk',
						':from': 'CREATED:2024-12-30T23:00:00',
						':to': 'CREATED:2024-12-30T23:30:00',
					},
				}),
			)
		).Items,
	).toStrictEqual([{ pk: 'pk', sk: 'CREATED:2024-12-30T23:00:00' }]);
});
```
