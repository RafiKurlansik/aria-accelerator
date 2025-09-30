# Evaluating ARIA-Generated vs. Human-Reviewed Responses

This document demonstrates a technique for evaluating the differences between AI-generated responses (from ARIA) and final human-reviewed submissions. This approach can be applied to any dataset where you have:

1. **Original AI-generated responses** (e.g., from ARIA)
2. **Final human-reviewed responses** (e.g., edited by subject matter experts)

The evaluation uses Databricks' `ai_classify()` function to automatically categorize the types and extent of changes made during the human review process.

## Overview of the Evaluation Approach

### The Process

1. **Data Preparation**: Collect pairs of original AI responses and final human-edited versions
2. **Prompt Engineering**: Design a classification prompt that can reliably categorize edit types
3. **Automated Classification**: Use `ai_classify()` to label each response pair
4. **Analysis**: Aggregate results to understand editing patterns and AI quality

### Why This Matters

- **Quality Assessment**: Understand how often AI responses need significant editing
- **Process Improvement**: Identify common types of edits to improve AI prompts
- **Trust Building**: Quantify the reliability of AI-generated content
- **Workflow Optimization**: Determine which responses might need less human review

## Data Structure Requirements

Your evaluation dataset should have the following columns:

```sql
CREATE TABLE your_evaluation_dataset (
  question_id STRING,           -- Unique identifier for each question
  question_text STRING,         -- The original question or prompt
  original_response STRING,     -- AI-generated response from ARIA
  final_response STRING,        -- Human-reviewed and edited response
  reviewer STRING,              -- Optional: who reviewed it
  review_date DATE,             -- Optional: when it was reviewed
  category STRING               -- Optional: type/category of question
);
```

### Sample Data

Here's an example of what your data might look like:

| question_id | question_text | original_response | final_response | reviewer |
|-------------|---------------|-------------------|----------------|----------|
| Q001 | "Does your platform support real-time analytics?" | "Yes, our platform provides real-time analytics capabilities through our streaming engine." | "Yes, Databricks provides real-time analytics capabilities through Delta Live Tables and Structured Streaming, enabling sub-second latency for critical business insights." | SME_Analytics |
| Q002 | "What security certifications do you have?" | "We have SOC 2 and ISO 27001 certifications." | "Databricks maintains SOC 2 Type II, ISO 27001, HIPAA, and FedRAMP certifications, with additional regional compliance including GDPR and data residency options." | SME_Security |

## Classification Framework

### Edit Categories

The evaluation framework classifies edits into these categories:

1. **No Changes** - AI response was used as-is
2. **Minor Edits** - Small wording changes, grammar fixes, formatting
3. **Factual Corrections** - Correcting inaccurate information or adding missing details
4. **Messaging Alignment** - Adjusting tone, brand voice, or positioning
5. **Significant Rewrites** - Major structural or content changes
6. **Complete Replacement** - Entirely new response written

### Quality Scoring

Each response pair receives a quality score:

- **Excellent (90-100%)** - No changes or minor edits only
- **Good (70-89%)** - Minor factual corrections or messaging adjustments
- **Fair (50-69%)** - Significant edits but core content retained
- **Poor (0-49%)** - Major rewrites or complete replacements needed

## Implementation Example

### Step 1: Data Preparation

```sql
-- Create your evaluation dataset
CREATE OR REPLACE TABLE evaluation.aria_response_comparison AS
SELECT 
  question_id,
  question_text,
  aria_response as original_response,
  final_submission as final_response,
  reviewer_name as reviewer,
  review_date,
  question_category as category
FROM your_rfp_data
WHERE aria_response IS NOT NULL 
  AND final_submission IS NOT NULL;
```

### Step 2: Classification Prompt

```sql
-- Use ai_classify to categorize the edits
SELECT 
  question_id,
  question_text,
  original_response,
  final_response,
  ai_classify(
    CONCAT(
      'Compare these two responses to the same question and classify the type of edits made:\n\n',
      'Question: ', question_text, '\n\n',
      'Original AI Response: ', original_response, '\n\n',
      'Final Human-Edited Response: ', final_response, '\n\n',
      'Classification Options:\n',
      '- no_changes: Responses are identical or nearly identical\n',
      '- minor_edits: Small wording changes, grammar, or formatting\n',
      '- factual_corrections: Correcting inaccurate information\n',
      '- messaging_alignment: Adjusting tone, brand voice, or positioning\n',
      '- significant_rewrites: Major structural or content changes\n',
      '- complete_replacement: Entirely new response\n\n',
      'Respond with only the classification category.'
    ),
    ARRAY('no_changes', 'minor_edits', 'factual_corrections', 'messaging_alignment', 'significant_rewrites', 'complete_replacement')
  ) as edit_classification
FROM evaluation.aria_response_comparison;
```

### Step 3: Quality Scoring

```sql
-- Add quality scores based on edit types
SELECT 
  *,
  CASE 
    WHEN edit_classification = 'no_changes' THEN 100
    WHEN edit_classification = 'minor_edits' THEN 90
    WHEN edit_classification = 'factual_corrections' THEN 75
    WHEN edit_classification = 'messaging_alignment' THEN 80
    WHEN edit_classification = 'significant_rewrites' THEN 50
    WHEN edit_classification = 'complete_replacement' THEN 25
    ELSE 0
  END as quality_score,
  CASE 
    WHEN quality_score >= 90 THEN 'Excellent'
    WHEN quality_score >= 70 THEN 'Good'
    WHEN quality_score >= 50 THEN 'Fair'
    ELSE 'Poor'
  END as quality_grade
FROM evaluation_with_classification;
```

## Analysis and Insights

### Key Metrics to Track

1. **Overall Quality Distribution**
   ```sql
   SELECT 
     quality_grade,
     COUNT(*) as response_count,
     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
   FROM evaluation_results
   GROUP BY quality_grade
   ORDER BY quality_score DESC;
   ```

2. **Edit Type Frequency**
   ```sql
   SELECT 
     edit_classification,
     COUNT(*) as frequency,
     AVG(quality_score) as avg_quality_score
   FROM evaluation_results
   GROUP BY edit_classification
   ORDER BY frequency DESC;
   ```

3. **Quality by Question Category**
   ```sql
   SELECT 
     category,
     AVG(quality_score) as avg_quality,
     COUNT(*) as total_questions
   FROM evaluation_results
   GROUP BY category
   ORDER BY avg_quality DESC;
   ```

### Improvement Opportunities

Based on your evaluation results, you can:

1. **Identify Weak Areas**: Categories with low quality scores need prompt improvements
2. **Update Training Data**: Add examples from high-quality human edits to your knowledge base
3. **Refine Prompts**: Address common factual corrections or messaging issues
4. **Optimize Workflows**: Route certain question types directly to human review

## Advanced Analysis

### Temporal Trends

Track how AI quality improves over time:

```sql
SELECT 
  DATE_TRUNC('month', review_date) as month,
  AVG(quality_score) as avg_quality,
  COUNT(*) as total_responses
FROM evaluation_results
GROUP BY month
ORDER BY month;
```

### Reviewer Consistency

Analyze if different reviewers have different editing patterns:

```sql
SELECT 
  reviewer,
  AVG(quality_score) as avg_quality_assigned,
  COUNT(*) as reviews_completed,
  MODE(edit_classification) as most_common_edit_type
FROM evaluation_results
GROUP BY reviewer;
```

### Question Complexity Analysis

Identify which types of questions are hardest for AI to handle:

```sql
SELECT 
  category,
  AVG(LENGTH(original_response)) as avg_response_length,
  AVG(quality_score) as avg_quality,
  COUNT(*) as question_count
FROM evaluation_results
GROUP BY category
ORDER BY avg_quality ASC;
```

## Best Practices

### Data Collection

1. **Consistent Timing**: Collect evaluation data regularly, not just when problems occur
2. **Representative Sampling**: Include questions from all categories and difficulty levels
3. **Multiple Reviewers**: Have different experts review the same responses to check consistency
4. **Version Control**: Track which version of ARIA generated each response

### Evaluation Process

1. **Blind Review**: Don't tell reviewers which responses are AI-generated
2. **Clear Guidelines**: Provide reviewers with consistent editing standards
3. **Regular Calibration**: Periodically check that reviewers are applying standards consistently
4. **Feedback Loop**: Share evaluation results with the team improving ARIA

### Continuous Improvement

1. **Monthly Reviews**: Run this evaluation monthly to track progress
2. **Prompt Updates**: Use insights to refine AI prompts and instructions
3. **Training Data**: Add high-quality human responses to your knowledge base
4. **Model Tuning**: Consider fine-tuning models on your specific domain

## Conclusion

This evaluation framework provides a systematic approach to measuring and improving AI response quality. By regularly comparing AI-generated content with human-reviewed versions, you can:

- Quantify AI performance objectively
- Identify specific areas for improvement
- Build confidence in AI-generated content
- Optimize human review workflows

The key is to make this evaluation process routine and use the insights to continuously improve your AI system's performance.

## Implementation Checklist

- [ ] Set up evaluation dataset with required columns
- [ ] Implement classification using `ai_classify()`
- [ ] Create quality scoring framework
- [ ] Build analysis dashboards
- [ ] Establish regular evaluation schedule
- [ ] Define improvement process based on results
- [ ] Train reviewers on consistent standards
- [ ] Set up automated reporting

For questions about implementing this evaluation framework, consult with your Databricks solution architect or data science team.
