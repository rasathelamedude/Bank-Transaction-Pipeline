export class KafkaMessage {
  transactionId: string;
  from: string;
  to: string;
  amount: number;
  currency: string;
  timestamp: Date;

  constructor(from: string, to: string, amount: number) {
    this.transactionId = this.generateTransactionId();
    this.from = from;
    this.to = to;
    this.amount = amount;
    this.currency = "USD";
    this.timestamp = new Date();
  }

  private generateTransactionId(): string {
    return Math.random().toString(36).substr(2, 9);
  }
}
