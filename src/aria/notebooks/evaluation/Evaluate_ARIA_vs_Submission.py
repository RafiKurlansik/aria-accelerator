# Databricks notebook source
# MAGIC %md
# MAGIC # Evaluating ARIA-Generated vs. Human-Reviewed Responses
# MAGIC 
# MAGIC This notebook demonstrates a technique for evaluating the differences between AI-generated responses (from ARIA) and final human-reviewed submissions. This approach can be applied to any dataset where you have:
# MAGIC 
# MAGIC 1. **Original AI-generated responses** (e.g., from ARIA)
# MAGIC 2. **Final human-reviewed responses** (e.g., edited by subject matter experts)
# MAGIC 
# MAGIC The evaluation uses Databricks' `ai_classify()` function to automatically categorize the types and extent of changes made during the human review process.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview of the Evaluation Approach
# MAGIC 
# MAGIC ### The Process
# MAGIC 
# MAGIC 1. **Data Preparation**: Collect pairs of original AI responses and final human-edited versions
# MAGIC 2. **Prompt Engineering**: Design a classification prompt that can reliably categorize edit types
# MAGIC 3. **Automated Classification**: Use `ai_classify()` to label each response pair
# MAGIC 4. **Analysis**: Aggregate results to understand editing patterns and AI quality
# MAGIC 
# MAGIC ### Why This Matters
# MAGIC 
# MAGIC - **Quality Assessment**: Understand how often AI responses need significant editing
# MAGIC - **Process Improvement**: Identify common types of edits to improve AI prompts
# MAGIC - **Trust Building**: Quantify the reliability of AI-generated content
# MAGIC - **Workflow Optimization**: Determine which responses might need less human review

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Structure Requirements
# MAGIC 
# MAGIC Your evaluation dataset should have the following columns:
# MAGIC 
# MAGIC ```sql
# MAGIC CREATE TABLE your_evaluation_dataset (
# MAGIC   question_id STRING,           -- Unique identifier for each question
# MAGIC   question_text STRING,         -- The original question or prompt
# MAGIC   original_response STRING,     -- AI-generated response from ARIA
# MAGIC   final_response STRING,        -- Human-reviewed and edited response
# MAGIC   reviewer STRING,              -- Optional: who reviewed it
# MAGIC   review_date DATE,             -- Optional: when it was reviewed
# MAGIC   category STRING               -- Optional: type/category of question
# MAGIC );
# MAGIC ```
# MAGIC 
# MAGIC ### Sample Data
# MAGIC 
# MAGIC | question_id | original_response | final_response |
# MAGIC |-------------|-------------------|----------------|
# MAGIC | Q001 | "Our platform supports real-time data processing..." | "Our platform provides comprehensive real-time data processing capabilities with sub-second latency..." |
# MAGIC | Q002 | "We use machine learning for recommendations..." | "Our recommendation engine leverages advanced machine learning algorithms including collaborative filtering..." |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classification Framework
# MAGIC 
# MAGIC We use a 5-level classification system to categorize the extent of changes:
# MAGIC 
# MAGIC ### Edit Type Categories
# MAGIC 
# MAGIC 1. **Unchanged** - No meaningful changes; content is essentially identical
# MAGIC 2. **Rephrasing / Grammar / Style Changes** - Improved readability, grammar, or style without factual changes
# MAGIC 3. **Minor to Moderate Enhancements** - Small factual updates, clarifications, or examples added
# MAGIC 4. **Substantial Enhancements** - Extensive additions, modifications, or restructuring
# MAGIC 5. **Complete Rewrite** - Entirely new content; original largely discarded
# MAGIC 
# MAGIC ### Decision Rules
# MAGIC 
# MAGIC - Count only **substantive edits** (facts, claims, numbers, sources)
# MAGIC - Ignore purely **cosmetic changes** (capitalization, formatting, typography)
# MAGIC - Re-ordering without new information = Rephrasing/Style
# MAGIC - Added examples, caveats, or technical details count as substantive
# MAGIC - When uncertain between categories, choose the higher (more-change) label

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Classification Prompt
# MAGIC 
# MAGIC Here's the refined prompt that works well with `ai_classify()`:

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC WITH evaluation_prompt AS (
# MAGIC   SELECT CONCAT(
# MAGIC     'Original: "', original_response, '"\n',
# MAGIC     'Revised: "', final_response, '"\n',
# MAGIC     'You are a senior editor with expertise in the subject domain.
# MAGIC
# MAGIC ──────────────────────── TASK ────────────────────────
# MAGIC Given an ORIGINAL answer (O) and a REVISED answer (R), decide how much the meaning has changed and return only one label from the list in "LABELS."
# MAGIC
# MAGIC ─────────────────────── LABELS ───────────────────────
# MAGIC 	1.	Unchanged
# MAGIC – No substantive change; meaning identical.
# MAGIC 	2.	Rephrasing / Grammar / Style Changes  
# MAGIC – Improved readability, grammar, punctuation, capitalization, spelling, tone, or style; no factual changes.
# MAGIC 	3.	Minor to Moderate Enhancements
# MAGIC – Small to moderate factual updates, clarifications, examples, or adjustments; core meaning intact, improved completeness or accuracy.
# MAGIC 	4.	Substantial Enhancements
# MAGIC – Extensive additions, modifications, or restructuring; significantly clearer or more informative.
# MAGIC 	5.	Complete Rewrite
# MAGIC – Entirely new text; original content largely discarded or replaced.
# MAGIC
# MAGIC ────────────────────── DECISION RULES ─────────────────
# MAGIC • Count only substantive edits (facts, claims, numbers, sources).
# MAGIC • Ignore purely cosmetic changes (capitalization, list re-formatting, typographic quotes).
# MAGIC • Re-ordering sentences without new or lost information = Rephrasing / Grammar / Style Changes.
# MAGIC • Added / deleted examples, caveats, or code snippets count as substantive edits.
# MAGIC • If you cannot decide between two adjacent labels, choose the higher (more-change) label.
# MAGIC
# MAGIC ──────────────────────── EXAMPLES ─────────────────────
# MAGIC 	1.	Unchanged
# MAGIC O: "Data pipelines process information in batches."
# MAGIC R: "Data pipelines process information in batches."
# MAGIC 	2.	Rephrasing / Grammar / Style Changes
# MAGIC O: "We dont support real-time processing currently."
# MAGIC R: "We do not currently support real-time processing."
# MAGIC 	3.	Rephrasing / Grammar / Style Changes  
# MAGIC O: "The results were surprising to many users."
# MAGIC R: "Many users found the results surprising."
# MAGIC 	4.	Minor to Moderate Enhancements
# MAGIC O: "Our product launches in Q3."
# MAGIC R: "Our product launches in Q3 2024 with beta access starting in July."
# MAGIC 	5.	Minor to Moderate Enhancements
# MAGIC O: "The study covered 100 users in 2023."
# MAGIC R: "The 6-month study enrolled 100 enterprise users and 50 individual users between January-June 2023."
# MAGIC 	6.	Substantial Enhancements
# MAGIC O: "We handle data processing using standard ETL tools."
# MAGIC R: "We orchestrate complex data processing workflows using Apache Airflow for scheduling, dbt for transformations, and Spark for distributed processing, with monitoring via custom dashboards."
# MAGIC → Core claims retained, extensive technical details added.
# MAGIC 	7.	Complete Rewrite
# MAGIC O: "We handle data processing using standard ETL tools."
# MAGIC R: "Our real-time streaming architecture leverages Kafka for event ingestion, Flink for stream processing, and ClickHouse for analytics, enabling sub-second query responses across petabyte datasets."
# MAGIC → Original approach completely replaced with new architecture.
# MAGIC
# MAGIC ──────────────────────── OUTPUT ───────────────────────
# MAGIC Return one bare label token and nothing else, e.g.:
# MAGIC
# MAGIC Minor to Moderate Enhancements'
# MAGIC   ) AS classification_prompt
# MAGIC   FROM your_evaluation_dataset
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Implementation Example
# MAGIC 
# MAGIC Here's how to apply this classification to your dataset:

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- Step 1: Create the classification prompt for each response pair
# MAGIC WITH classified_responses AS (
# MAGIC   SELECT
# MAGIC     question_id,
# MAGIC     question_text,
# MAGIC     original_response,
# MAGIC     final_response,
# MAGIC     category,
# MAGIC     ai_classify(
# MAGIC       CONCAT(
# MAGIC         'Original: "', original_response, '"\n',
# MAGIC         'Revised: "', final_response, '"\n',
# MAGIC         -- [Insert the full prompt from above here]
# MAGIC       ),
# MAGIC       ARRAY(
# MAGIC         'Unchanged',
# MAGIC         'Rephrasing / Grammar / Style Changes',
# MAGIC         'Minor to Moderate Enhancements', 
# MAGIC         'Substantial Enhancements',
# MAGIC         'Complete Rewrite'
# MAGIC       )
# MAGIC     ) AS edit_classification
# MAGIC   FROM your_evaluation_dataset
# MAGIC   WHERE original_response IS NOT NULL 
# MAGIC     AND final_response IS NOT NULL
# MAGIC )
# MAGIC 
# MAGIC -- Step 2: Analyze the results
# MAGIC SELECT 
# MAGIC   edit_classification,
# MAGIC   COUNT(*) as response_count,
# MAGIC   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
# MAGIC FROM classified_responses
# MAGIC GROUP BY edit_classification
# MAGIC ORDER BY 
# MAGIC   CASE edit_classification
# MAGIC     WHEN 'Unchanged' THEN 1
# MAGIC     WHEN 'Rephrasing / Grammar / Style Changes' THEN 2
# MAGIC     WHEN 'Minor to Moderate Enhancements' THEN 3
# MAGIC     WHEN 'Substantial Enhancements' THEN 4
# MAGIC     WHEN 'Complete Rewrite' THEN 5
# MAGIC   END;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analysis and Insights
# MAGIC 
# MAGIC ### Key Metrics to Track
# MAGIC 
# MAGIC 1. **AI Accuracy Rate**: Percentage of "Unchanged" + "Rephrasing/Grammar/Style" responses
# MAGIC 2. **Enhancement Rate**: Percentage needing "Minor to Moderate" or "Substantial" enhancements
# MAGIC 3. **Rewrite Rate**: Percentage requiring "Complete Rewrite"
# MAGIC 
# MAGIC ### Sample Analysis Queries
# MAGIC 
# MAGIC ```sql
# MAGIC -- Overall AI performance summary
# MAGIC SELECT 
# MAGIC   CASE 
# MAGIC     WHEN edit_classification IN ('Unchanged', 'Rephrasing / Grammar / Style Changes') 
# MAGIC     THEN 'Minimal Changes'
# MAGIC     WHEN edit_classification IN ('Minor to Moderate Enhancements', 'Substantial Enhancements')
# MAGIC     THEN 'Needs Enhancement'
# MAGIC     ELSE 'Major Revision Required'
# MAGIC   END as change_category,
# MAGIC   COUNT(*) as count,
# MAGIC   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) as percentage
# MAGIC FROM classified_responses
# MAGIC GROUP BY change_category;
# MAGIC 
# MAGIC -- Performance by question category
# MAGIC SELECT 
# MAGIC   category,
# MAGIC   edit_classification,
# MAGIC   COUNT(*) as count
# MAGIC FROM classified_responses
# MAGIC GROUP BY category, edit_classification
# MAGIC ORDER BY category, count DESC;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interpreting Results
# MAGIC 
# MAGIC ### What Good Results Look Like
# MAGIC 
# MAGIC - **60-80% Minimal Changes**: AI responses are largely accurate and well-written
# MAGIC - **15-30% Needs Enhancement**: Normal improvement and refinement
# MAGIC - **5-15% Major Revision**: Acceptable rate for complex or novel questions
# MAGIC 
# MAGIC ### Red Flags
# MAGIC 
# MAGIC - **>30% Major Revision**: May indicate AI prompt issues or training data gaps
# MAGIC - **Consistent patterns by category**: Specific types of questions that need improvement
# MAGIC - **High variance between reviewers**: May indicate inconsistent review standards
# MAGIC 
# MAGIC ### Improvement Actions
# MAGIC 
# MAGIC 1. **Analyze Complete Rewrites**: Understand what the AI got wrong
# MAGIC 2. **Update AI Prompts**: Incorporate lessons from substantial enhancements
# MAGIC 3. **Enhance Training Data**: Add examples for poorly performing categories
# MAGIC 4. **Reviewer Training**: Ensure consistent evaluation standards

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced Variations
# MAGIC 
# MAGIC ### Multi-Dimensional Classification
# MAGIC 
# MAGIC You can extend this approach to classify multiple aspects:
# MAGIC 
# MAGIC ```sql
# MAGIC -- Classify both content changes and factual accuracy
# MAGIC SELECT
# MAGIC   question_id,
# MAGIC   ai_classify(prompt, content_change_labels) as content_change_type,
# MAGIC   ai_classify(accuracy_prompt, accuracy_labels) as accuracy_rating,
# MAGIC   ai_classify(style_prompt, style_labels) as style_improvement
# MAGIC FROM your_evaluation_dataset;
# MAGIC ```
# MAGIC 
# MAGIC ### Time-Series Analysis
# MAGIC 
# MAGIC Track improvement over time:
# MAGIC 
# MAGIC ```sql
# MAGIC -- See how AI performance improves over time
# MAGIC SELECT 
# MAGIC   DATE_TRUNC('month', review_date) as month,
# MAGIC   AVG(CASE WHEN edit_classification = 'Unchanged' THEN 1.0 ELSE 0.0 END) as unchanged_rate,
# MAGIC   AVG(CASE WHEN edit_classification = 'Complete Rewrite' THEN 1.0 ELSE 0.0 END) as rewrite_rate
# MAGIC FROM classified_responses
# MAGIC GROUP BY month
# MAGIC ORDER BY month;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices
# MAGIC 
# MAGIC ### Prompt Design
# MAGIC 
# MAGIC 1. **Be Specific**: Clearly define each classification category
# MAGIC 2. **Provide Examples**: Include representative examples for each category
# MAGIC 3. **Set Decision Rules**: Help the AI handle edge cases consistently
# MAGIC 4. **Domain Context**: Adapt the prompt to your specific domain expertise
# MAGIC 
# MAGIC ### Data Quality
# MAGIC 
# MAGIC 1. **Clean Input**: Ensure original and final responses are properly formatted
# MAGIC 2. **Handle Nulls**: Filter out incomplete response pairs
# MAGIC 3. **Consistent Reviews**: Use standardized review processes
# MAGIC 4. **Sufficient Sample Size**: Aim for statistically significant sample sizes
# MAGIC 
# MAGIC ### Validation
# MAGIC 
# MAGIC 1. **Human Validation**: Manually verify a sample of AI classifications
# MAGIC 2. **Inter-rater Reliability**: Have multiple people classify the same examples
# MAGIC 3. **Iterative Refinement**: Improve the prompt based on misclassifications
# MAGIC 4. **Regular Monitoring**: Track classification quality over time

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC 
# MAGIC This evaluation framework provides a scalable way to assess the quality and reliability of AI-generated responses. By systematically categorizing the types of edits made during human review, you can:
# MAGIC 
# MAGIC - **Quantify AI accuracy** across different types of content
# MAGIC - **Identify improvement opportunities** for AI prompts and training
# MAGIC - **Optimize review workflows** by focusing human effort where it's most needed
# MAGIC - **Build confidence** in AI-generated content through data-driven insights
# MAGIC 
# MAGIC The approach is flexible and can be adapted to any domain where you have paired AI-generated and human-reviewed content, making it valuable for continuous improvement of AI-assisted content creation workflows.
