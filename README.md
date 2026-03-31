# FX Trading Signal Bot

An automated FX trading signal system built with AWS Lambda and Python.

## Overview

A serverless trading signal bot that monitors 5 currency pairs in real-time, 
detects entry signals based on technical indicators, and sends notifications 
via Discord webhook.

## Features

- Monitors 5 currency pairs: EURUSD, GBPUSD, USDJPY, GBPJPY, AUDUSD
- Runs every 5 minutes via AWS EventBridge
- Entry signal detection based on MA crossover and trailing stop logic
- State management with DynamoDB
- Real-time Discord webhook notifications
- Backtested against historical 1-minute Dukascopy data

## Tech Stack

- Python 3.x
- AWS Lambda
- AWS DynamoDB
- AWS EventBridge
- Twelvedata API (market data)
- Discord Webhook

## Parameters (Backtested)

| Pair   | Stop Loss | Take Profit | Decay |
|--------|-----------|-------------|-------|
| EURUSD | 40 pips   | 30%         | 20    |
| GBPUSD | 50 pips   | 25%         | 20    |
| USDJPY | 30 pips   | 25%         | 20    |
| GBPJPY | 60 pips   | 40%         | 20    |
| AUDUSD | 50 pips   | 30%         | 20    |

## Architecture

EventBridge (5min) → Lambda → Twelvedata API → Signal Detection → DynamoDB → Discord
