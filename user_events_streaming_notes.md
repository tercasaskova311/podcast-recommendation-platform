Kafka (raw events)
    ↓
Structured Streaming
    ↓ parse + dedupe + weight + decay
    ↓
Bronze Delta (raw events, immutable)
    ↓
Silver Delta (daily engagement: user_id × episode_id × day)
    ↓
ALS Training (aggregates across days)
    ↓
Recommendations
