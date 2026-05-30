# AFO Live Test Bot

Fresh single-folder Render project for Telegram AFO daily live tests.

## Render Start Command

```bash
uvicorn app:app --host 0.0.0.0 --port $PORT
```

## Render Environment

Add these keys in Render:

```env
TELEGRAM_BOT_TOKEN=
TELEGRAM_PUBLIC_GROUP_ID=@agriquizworld
TELEGRAM_PAID_GROUP_ID=-1003687531473
TELEGRAM_ADMIN_USER_IDS=1138783169
MONGODB_URI=
MONGODB_DB_NAME=afo_daily_test
RAZORPAY_KEY_ID=
RAZORPAY_KEY_SECRET=
RAZORPAY_WEBHOOK_SECRET=
GOOGLE_SHEET_ID=1cPPxwPTgDHfKAwLc_7ZG9WsAMUhYsiZrbJhfV0gN6W4
GOOGLE_SHEET_NAME=Sheet1
PUBLIC_BASE_URL=https://your-render-service.onrender.com
TELEGRAM_WEBHOOK_SECRET=afo_telegram_webhook_1996
```

After Render gives the live URL, update `PUBLIC_BASE_URL`.

## Telegram Admin Commands

Run these in bot DM from admin account:

```text
/import_sheet
/build_sets
/run_test
```

Use `/pay` to test Razorpay payment link creation.

## Razorpay Webhook

Set webhook URL:

```text
https://your-render-service.onrender.com/razorpay/webhook
```

Enable events:

```text
payment.captured
payment_link.paid
```
