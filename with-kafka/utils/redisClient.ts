import { Redis } from "ioredis";

const redis = new Redis({
  host: "localhost",
  port: 6379,
  db: 0,
  connectTimeout: 5000,
});

redis.on("connect", () => console.log("Connected to Redis!"));

redis.on("error", (error) => {
  console.log("Redis error: ", error);
});

export async function getBalance(accountId: string): Promise<string | null> {
  const balance: string | null = await redis.get(`balance:${accountId}`);
  return balance;
}

export async function setBalance(
  accountId: string,
  balance: number,
): Promise<void> {
  await redis.set(`balance:${accountId}`, balance);
}

export async function updateBalance(
  accountId: string,
  amount: number,
): Promise<void> {
  await redis.incrby(`balance:${accountId}`, amount);
}
