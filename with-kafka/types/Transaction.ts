export class Transaction {
  transactionId: string;
  senderAccountId: string;
  receiverAccountId: string;
  amount: number;
  currency: string;
  timestamp: Date;

  constructor(senderId: string, recieverId: string, amount: number) {
    this.transactionId = this.generateTransactionId();
    this.senderAccountId = senderId;
    this.receiverAccountId = recieverId;
    this.amount = amount;
    this.currency = "USD";
    this.timestamp = new Date();
  }

  private generateTransactionId(): string {
    return Math.random().toString(36).substr(2, 9);
  }
}
