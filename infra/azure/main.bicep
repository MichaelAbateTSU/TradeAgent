@description('Azure region for the always-on shadow runtime.')
param location string = resourceGroup().location

@description('Immutable TradeAgent container image, preferably tagged by Git SHA.')
param containerImage string

@secure()
@description('SQLAlchemy PostgreSQL URL for a migrated managed database.')
param databaseUrl string

@secure()
param alpacaKeyId string

@secure()
param alpacaSecretKey string

@secure()
param emailApiKey string

param emailSender string
param emailRecipient string
param symbols string = 'SPY,QQQ,IWM,TLT,GLD'

resource logs 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: 'tradeagent-logs'
  location: location
  properties: {
    retentionInDays: 30
    sku: {
      name: 'PerGB2018'
    }
  }
}

resource environment 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: 'tradeagent-environment'
  location: location
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: logs.properties.customerId
        sharedKey: logs.listKeys().primarySharedKey
      }
    }
  }
}

resource worker 'Microsoft.App/containerApps@2024-03-01' = {
  name: 'tradeagent-shadow-worker'
  location: location
  properties: {
    managedEnvironmentId: environment.id
    configuration: {
      activeRevisionsMode: 'Single'
      secrets: [
        {
          name: 'database-url'
          value: databaseUrl
        }
        {
          name: 'alpaca-key-id'
          value: alpacaKeyId
        }
        {
          name: 'alpaca-secret-key'
          value: alpacaSecretKey
        }
      ]
    }
    template: {
      containers: [
        {
          name: 'worker'
          image: containerImage
          command: [
            'tradeagent'
            'worker-shadow'
            '--symbols'
            symbols
          ]
          env: [
            {
              name: 'TRADEAGENT_MODE'
              value: 'paper'
            }
            {
              name: 'TRADEAGENT_DATABASE_URL'
              secretRef: 'database-url'
            }
            {
              name: 'ALPACA_KEY_ID'
              secretRef: 'alpaca-key-id'
            }
            {
              name: 'ALPACA_SECRET_KEY'
              secretRef: 'alpaca-secret-key'
            }
            {
              name: 'ALPACA_DATA_STREAM_URL'
              value: 'wss://stream.data.alpaca.markets/v2/iex'
            }
          ]
          resources: {
            cpu: json('0.5')
            memory: '1Gi'
          }
        }
      ]
      scale: {
        minReplicas: 1
        maxReplicas: 1
      }
    }
  }
}

resource notifier 'Microsoft.App/containerApps@2024-03-01' = {
  name: 'tradeagent-notifier'
  location: location
  properties: {
    managedEnvironmentId: environment.id
    configuration: {
      activeRevisionsMode: 'Single'
      secrets: [
        {
          name: 'database-url'
          value: databaseUrl
        }
        {
          name: 'email-api-key'
          value: emailApiKey
        }
      ]
    }
    template: {
      containers: [
        {
          name: 'notifier'
          image: containerImage
          command: [
            'tradeagent'
            'notifier'
          ]
          env: [
            {
              name: 'TRADEAGENT_MODE'
              value: 'paper'
            }
            {
              name: 'TRADEAGENT_DATABASE_URL'
              secretRef: 'database-url'
            }
            {
              name: 'EMAIL_PROVIDER'
              value: 'resend'
            }
            {
              name: 'EMAIL_API_KEY'
              secretRef: 'email-api-key'
            }
            {
              name: 'EMAIL_SENDER'
              value: emailSender
            }
            {
              name: 'EMAIL_RECIPIENT'
              value: emailRecipient
            }
          ]
          resources: {
            cpu: json('0.25')
            memory: '0.5Gi'
          }
        }
      ]
      scale: {
        minReplicas: 1
        maxReplicas: 1
      }
    }
  }
}

output environmentName string = environment.name
output workerName string = worker.name
output notifierName string = notifier.name
