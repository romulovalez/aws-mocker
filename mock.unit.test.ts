import {
  cleanTables,
  cleanBuckets,
  cleanTopics,
  cleanQueues,
  internal_getTopic,
} from "./mock";
import { beforeAll, beforeEach, describe, expect, test } from "vitest";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { PutCommand, GetCommand, BatchGetCommand } from "@aws-sdk/lib-dynamodb";
import {
  GetObjectCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import {
  GetPublicKeyCommand,
  KMSClient,
  SignCommand,
} from "@aws-sdk/client-kms";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import {
  SQSClient,
  SendMessageCommand,
  ReceiveMessageCommand,
} from "@aws-sdk/client-sqs";
import jwt, { JwtPayload } from "jsonwebtoken";
import base64url from "base64url";

const client = new DynamoDBClient();
const s3Client = new S3Client();
const snsClient = new SNSClient();
const sqsClient = new SQSClient();
const kmsClient = new KMSClient();

describe("DynamoDB", () => {
  beforeEach(() => {
    cleanTables();
  });

  test("PutCommand && GetCommand", async () => {
    await client.send(
      new PutCommand({
        TableName: "TableName",
        Item: { pk: "item1", sk: "sk" },
      })
    );

    const shouldItemBeUndefined = await client.send(
      new GetCommand({
        TableName: "TableName",
        Key: { pk: "item2", sk: "sk" },
      })
    );

    expect(shouldItemBeUndefined.Item).toStrictEqual(undefined);

    const shouldItemExist = await client.send(
      new GetCommand({
        TableName: "TableName",
        Key: { pk: "item1", sk: "sk" },
      })
    );

    expect(shouldItemExist.Item).toStrictEqual({ pk: "item1", sk: "sk" });
  });

  test("GetBatchCommand", async () => {
    await client.send(
      new PutCommand({
        TableName: "TableName",
        Item: { pk: "item1", sk: "sk" },
      })
    );

    const items = await client.send(
      new BatchGetCommand({
        RequestItems: {
          ["TableName"]: {
            Keys: [{ pk: "item1", sk: "sk" }],
          },
        },
      })
    );

    expect(items.Responses).toStrictEqual({
      ["TableName"]: [{ pk: "item1", sk: "sk" }],
    });
  });
});

describe("S3", () => {
  beforeEach(() => {
    cleanBuckets();
  });

  test("PutObjectCommand && GetObjectCommand", async () => {
    await s3Client.send(
      new PutObjectCommand({
        Bucket: "BucketName",
        Key: "file.txt",
        Body: "some text\n",
      })
    );

    const response = await s3Client.send(
      new GetObjectCommand({ Bucket: "BucketName", Key: "file.txt" })
    );

    const file = await response.Body?.transformToString();

    expect(file).toStrictEqual("some text\n");
  });
});

describe("SNS", () => {
  beforeEach(() => {
    cleanTopics();
  });

  test("PublishCommand & check if message gets to topic", async () => {
    const message = { foo: "bar" };

    const publishResult = await snsClient.send(
      new PublishCommand({
        TopicArn: "TopicArn",
        Message: JSON.stringify(message),
      })
    );

    expect(publishResult.MessageId).toBeDefined();

    const topicMessages = internal_getTopic("TopicArn");
    expect(topicMessages).toStrictEqual([
      {
        TopicArn: "TopicArn",
        Message: JSON.stringify(message),
      },
    ]);
  });
});

describe("SQS", () => {
  beforeAll(() => {
    cleanQueues();
  });

  test("SendMessageCommand && ReceiveMessageCommand", async () => {
    const messageBody = { foo: "bar" };

    const sendMessageResult = await sqsClient.send(
      new SendMessageCommand({
        QueueUrl: "Queueurl",
        MessageBody: JSON.stringify(messageBody),
      })
    );

    expect(sendMessageResult.MessageId).toBeDefined();

    const receiveMessageResult = await sqsClient.send(
      new ReceiveMessageCommand({
        QueueUrl: "Queueurl",
        MaxNumberOfMessages: 1,
      })
    );

    expect(receiveMessageResult.Messages).toStrictEqual([
      {
        QueueUrl: "Queueurl",
        MessageBody: JSON.stringify(messageBody),
      },
    ]);
  });
});

describe("KMS", () => {
  test("SignCommand && GetPublicKeyCommand", async () => {
    const jwt = {
      fuc: "012345678",
      serialNumber: "",
      iss: "worldcoo.com/provider/bancSabadell/tpvAppApi",
      sub: "worldcoo.com/provider/bancSabadell/centers/012345678/terminals/",
      iat: 1734288132,
      aud: "worldcoo.com/provider/bancSabadell/tpvAppApi",
    };

    const signature = await sign(jwt);
    expect(signature).toBeDefined();

    const result = await verify(signature);
    expect(result).toStrictEqual(jwt);
  });
});

const headers: jwt.JwtHeader = {
  alg: "RS256",
  typ: "JWT",
};

const sign = async (payload: JwtPayload) => {
  const headersHash = base64url(JSON.stringify(headers));
  const payloadHash = base64url(JSON.stringify(payload));

  return `${headersHash}.${payloadHash}.${await buildSignature(
    `${headersHash}.${payloadHash}`
  )}`;
};

async function buildSignature(message: string) {
  const res = await kmsClient.send(
    new SignCommand({
      Message: Buffer.from(message),
      KeyId: "KmsKeyIdValue",
      SigningAlgorithm: "RSASSA_PKCS1_V1_5_SHA_256",
      MessageType: "RAW",
    })
  );
  return Buffer.from(res.Signature!)
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=/g, "");
}

const verify = async (token: string) => {
  const getPublicKeyResponse = await kmsClient.send(
    new GetPublicKeyCommand({
      KeyId: "KmsKeyIdValue",
    })
  );
  const publicKey = Buffer.from(getPublicKeyResponse.PublicKey!).toString(
    "base64"
  );
  const pemPublicKey = convertToPem(publicKey);

  return jwt.verify(token, pemPublicKey);
};

function convertToPem(base64Key: string) {
  return `-----BEGIN PUBLIC KEY-----\n${base64Key
    .match(/.{1,64}/g)!
    .join("\n")}\n-----END PUBLIC KEY-----`;
}
