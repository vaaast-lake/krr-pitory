import { z } from "zod";

const ItemSchema = z.object({
  productId: z.string(),
  sellerId: z.string(),
});

const PaymentTypeSchema = z.enum([
  "credit_card",
  "boleto",
  "voucher",
  "debit_card",
  "not_defined",
]);

export const OrderCreatedSchema = z.object({
  eventId: z.uuid(),
  orderId: z.string(),
  items: z
    .array(
      ItemSchema.extend({
        price: z.number(),
        quantity: z.number().int().positive(),
      }),
    )
    .min(1),
  payments: z
    .array(
      z.object({
        sequential: z.number().int().positive(),
        type: PaymentTypeSchema,
        value: z.number().nonnegative(),
        installments: z.number().int().nonnegative(),
      }),
    )
    .min(1),
  occurredAt: z.iso.datetime(),
  type: z.literal("order.created"),
});
export type OrderCreatedEvent = z.infer<typeof OrderCreatedSchema>;

export const OrderCanceledSchema = z.object({
  eventId: z.uuid(),
  orderId: z.string(),
  reason: z.enum(["customer_request", "stock_unavailable"]),
  items: z
    .array(
      ItemSchema.extend({
        quantity: z.number().int().positive(),
      }),
    )
    .min(1),
  occurredAt: z.iso.datetime(),
  type: z.literal("order.canceled"),
});
export type OrderCanceledEvent = z.infer<typeof OrderCanceledSchema>;

export const OrderProcessedSchema = z.object({
  eventId: z.uuid(),
  orderId: z.string(),
  items: z.array(ItemSchema).min(1),
  shippedAt: z.iso.datetime(),
  deliveredAt: z.iso.datetime(),
  estimatedDeliveryAt: z.iso.datetime(),
  occurredAt: z.iso.datetime(),
  type: z.literal("order.processed"),
});
export type OrderProcessedEvent = z.infer<typeof OrderProcessedSchema>;

export const ReviewCreatedSchema = z.object({
  eventId: z.uuid(),
  orderId: z.string(),
  reviewId: z.string(),
  reviewScore: z.number().int().min(1).max(5),
  sellerIds: z.array(z.string()).min(1),
  occurredAt: z.iso.datetime(),
  type: z.literal("review.created"),
});
export type ReviewCreatedEvent = z.infer<typeof ReviewCreatedSchema>;
