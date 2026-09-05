# Azure always-on shadow deployment

This deployment runs market-data ingestion and reconciliation in **shadow mode**. It
cannot submit orders. The notifier can deliver already-enqueued paper round-trip emails.

## Prerequisites

- Azure CLI with Bicep
- an Azure Container Registry
- a migrated Azure Database for PostgreSQL
- Alpaca paper credentials
- Resend API key plus a verified sender

## Build and push

```powershell
$registry = "<registry-name>"
$sha = git rev-parse --short HEAD
az acr build --registry $registry --image "tradeagent:$sha" .
```

Apply the database migration from a trusted shell:

```powershell
$env:TRADEAGENT_DATABASE_URL = "<postgresql+psycopg-url>"
.\.venv\Scripts\alembic.exe upgrade head
```

## Deploy

Keep secrets in local secure variables or a secret manager; never commit a parameter
file containing values.

```powershell
az deployment group create `
  --resource-group "<resource-group>" `
  --template-file .\infra\azure\main.bicep `
  --parameters `
    containerImage="<registry>.azurecr.io/tradeagent:$sha" `
    databaseUrl="$env:TRADEAGENT_DATABASE_URL" `
    alpacaKeyId="$env:ALPACA_KEY_ID" `
    alpacaSecretKey="$env:ALPACA_SECRET_KEY" `
    emailApiKey="$env:EMAIL_API_KEY" `
    emailSender="$env:EMAIL_SENDER" `
    emailRecipient="$env:EMAIL_RECIPIENT"
```

Both services use `minReplicas=1` and `maxReplicas=1`, so they remain running when the
laptop is off and cannot scale into duplicate workers. The database worker lock is an
additional guard.

## Verify

```powershell
az containerapp logs show `
  --resource-group "<resource-group>" `
  --name tradeagent-shadow-worker `
  --follow
```

Verify recurring `tradeagent-worker` and `tradeagent-reconciler` heartbeats in
PostgreSQL. Keep the durable kill switch active until the shadow soak is healthy.

The template stores secrets in Container Apps secret storage. A production hardening
follow-up should replace direct values with Key Vault references and private PostgreSQL
networking.

