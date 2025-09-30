# ARIA Analytics Overview

## Quick Start

ARIA captures comprehensive analytics across the entire RFI (Request for Information) process with **batch-level tracking** and **accurate business KPIs**.

### 🎯 Key Metrics Available

| Metric Type | Key Tables | Primary Use Case |
|-------------|------------|------------------|
| **Business KPIs** | `rfi_generation_batches` | Total questions answered, batch success rates, completion metrics |
| **Performance** | `rfi_generation_batches` | Model performance, processing times, batch analysis |  
| **User Experience** | Session analysis | Regeneration patterns, batch retry rates, user journey |
| **Chat Analytics** | `chat_questions` | Chat usage, response quality, model performance |

### ⚡ Quick Queries

**Business KPI - Total Questions Answered:**
```sql
SELECT SUM(SIZE(answers_in_batch)) as total_questions_answered
FROM users.rafi_kurlansik.rfi_generation_batches 
WHERE batch_status = 'success';
```

**Success Rate by Document:**
```sql
SELECT document_name, 
       AVG(SIZE(answers_in_batch)::FLOAT / SIZE(questions_in_batch)) as success_rate
FROM users.rafi_kurlansik.rfi_generation_batches
GROUP BY document_name;
```

**Model Performance:**
```sql
SELECT model_used,
       AVG(batch_processing_time_seconds) as avg_time,
       SUM(CASE WHEN batch_status = 'success' THEN 1 ELSE 0 END)::FLOAT / COUNT(*) as success_rate
FROM users.rafi_kurlansik.rfi_generation_batches
GROUP BY model_used;
```

## 📊 Data Structure Benefits

### Before: JSON Blobs 😞
```
questions_json: "[{\"question\":\"1\",\"sub_topics\":[...]"
```

### After: Navigable STRUCT/ARRAY 😍
```
questions[0].sub_topics[0].sub_questions[0].text
questions_in_batch[0].question_id, questions_in_batch[0].text
answers_in_batch[0].answer, answers_in_batch[0].references
```

## 🏗️ Architecture Highlights

- **Batch-Level Tracking**: Each topic/batch of questions captured as a unit
- **Complete Context**: All questions and answers preserved per batch
- **Session Versioning**: Explicit regeneration handling through user actions
- **STRUCT/ARRAY Format**: No more JSON parsing - direct SQL navigation
- **Auto Schema Evolution**: Missing columns/tables created automatically

## 📈 Analytics Tables

| Table | Purpose | Key Fields |
|-------|---------|------------|
| `rfi_extractions` | Question extraction events | `questions` (STRUCT/ARRAY), `document_name` |
| `rfi_generation_batches` | Batch processing & answers | `questions_in_batch`, `answers_in_batch`, `batch_status` |
| `chat_questions` | Chat interactions | `question`, `response` (both STRUCT) |
| `rfi_uploads` | Document uploads | `document_name`, `file_size_bytes` |
| `rfi_exports` | Export events | `export_format`, `total_questions` |

## 🚀 Getting Started

1. **Restart Backend** to apply schema changes:
   ```bash
   python app.py
   ```

2. **Verify Tables Created**:
   ```sql
   SHOW TABLES IN users.rafi_kurlansik LIKE '*rfi*';
   ```

3. **Test Data Navigation**:
   ```sql
   SELECT questions_in_batch[0].text, answers_in_batch[0].answer
   FROM users.rafi_kurlansik.rfi_generation_batches 
   WHERE questions_in_batch IS NOT NULL LIMIT 1;
   ```

## 📖 Full Documentation

For complete documentation including:
- Detailed table schemas
- Advanced query examples  
- Entity relationship diagrams
- Performance analysis queries
- Data exploration patterns

**See: [analytics.md](./analytics.md)**

## 💡 Common Use Cases

### Business Reporting
- Daily/weekly question volume and success rates
- Document performance comparison
- Topic/batch success analysis

### Product Optimization  
- Topic difficulty analysis
- Model performance comparison
- Batch processing optimization

### Technical Monitoring
- Processing time distributions
- Batch success/failure analysis
- Session versioning patterns

### Data Science
- Feature engineering for ML models
- Topic classification insights
- User behavior analytics

---

**Key Advantage**: This analytics system provides **complete batch-level data** that can answer business KPIs, UX metrics, and performance questions efficiently with full context preservation. 🎯