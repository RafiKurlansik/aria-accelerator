# Databricks notebook source
# MAGIC %md
# MAGIC ## Comparing documents

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH diffs AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     CONCAT(
# MAGIC       'Original: "',
# MAGIC       original_answer,
# MAGIC       '"\n',
# MAGIC       'Revised: "',
# MAGIC       final_answer,
# MAGIC       '"\n',
# MAGIC       'You are a senior editor.  
# MAGIC Task: Compare ORIGINAL vs. REVISED and return **one** label from this list:  
# MAGIC   • Unchanged  
# MAGIC   • Grammar & Mechanics  
# MAGIC   • Rephrase / Stylistic  
# MAGIC   • Minor Content Change  
# MAGIC   • Content Change  
# MAGIC   • Rewrite  
# MAGIC
# MAGIC DEFINITIONS  
# MAGIC • Unchanged – 0-1 word-level content changes; text is effectively identical.  
# MAGIC • Grammar & Mechanics – Surface fixes only (spelling, punctuation, capitalization, agreement, style-guide mechanics). Meaning unchanged.  
# MAGIC • Rephrase / Stylistic – Wording or structure improved for clarity, concision, or tone. Information unchanged.  
# MAGIC • Minor Content Change – 1-3 substantive content tweaks (adding, deleting, or replacing facts, claims, data, or examples).  
# MAGIC • Content Change – 4-10 substantive content changes that add, delete, or reorganize information.  
# MAGIC • Rewrite – 10+ substantive content changes OR the answer is essentially rewritten with a different structure or focus.  
# MAGIC
# MAGIC EXAMPLES  
# MAGIC 1. **Unchanged**  
# MAGIC    - O: “Cats are mammals.”  
# MAGIC      R: “Cats are mammals.”  
# MAGIC
# MAGIC 2. **Grammar**  
# MAGIC    - O: “He dont like coffee.”  
# MAGIC      R: “He doesn’t like coffee.”  
# MAGIC
# MAGIC 3. **Rephrase / Stylistic**  
# MAGIC    - O: “The results were surprising to many people.”  
# MAGIC      R: “Many people found the results surprising.”  
# MAGIC
# MAGIC 4. **Minor Content Change** (1 change)  
# MAGIC    - O: “Our product ships in July.”  
# MAGIC      R: “Our product ships in July and August.”  
# MAGIC
# MAGIC 5. **Content Change** (5 changes)  
# MAGIC    - O: “The study covered 100 patients in 2022.”  
# MAGIC      R: “The 18-month study enrolled 150 adult patients and 50 teens between 2022-23.”  
# MAGIC
# MAGIC 6. **Rewrite**  
# MAGIC    - O: “Describe your leadership style.”  
# MAGIC      R: “As a servant-leader, I empower cross-functional teams, set OKRs, mentor juniors, and drive continuous improvement through data-driven retrospectives.”  
# MAGIC
# MAGIC RETURN FORMAT  
# MAGIC Just one of: Unchanged | Grammar | Rephrase / Stylistic | Minor Content Change | Content Change | Rewrite'
# MAGIC     ) AS prompt
# MAGIC   FROM
# MAGIC     users.rafi_kurlansik.dbms_mq_2025_eval
# MAGIC )
# MAGIC SELECT
# MAGIC   original_answer,
# MAGIC   final_answer,
# MAGIC   ai_classify(
# MAGIC     prompt,
# MAGIC     ARRAY(
# MAGIC       'Unchanged',
# MAGIC       'Grammar',
# MAGIC       'Rephrase / Stylistic',
# MAGIC       'Minor Content Change',
# MAGIC       'Content Change',
# MAGIC       'Rewrite'
# MAGIC     )
# MAGIC   ) AS edit_type
# MAGIC FROM
# MAGIC   diffs;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH diffs AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     CONCAT(
# MAGIC       'Original: "',
# MAGIC       original_answer,
# MAGIC       '"\n',
# MAGIC       'Revised: "',
# MAGIC       final_answer,
# MAGIC       '"\n',
# MAGIC       'You are a senior editor with deep expertise in Databricks, data analytics, data engineering, machine learning and artificial intelligence.
# MAGIC
# MAGIC Task: Compare the ORIGINAL vs REVISED answer and return **exactly one** label from this list:
# MAGIC • Unchanged  
# MAGIC • Grammar & Mechanics  
# MAGIC • Rephrase / Stylistic  
# MAGIC • Minor Content Change  
# MAGIC • Content Change  
# MAGIC • Rewrite: Heavy Edit  
# MAGIC • Rewrite: Complete Restart
# MAGIC
# MAGIC DEFINITIONS:
# MAGIC • Unchanged: 0-1 substantive or word-level edits; meaning and wording are essentially identical.  
# MAGIC • Grammar & Mechanics: corrections to spelling, punctuation, capitalization, grammatical agreement, or style-guide conventions only; meaning is unchanged.  
# MAGIC • Rephrase / Stylistic: wording or structure improved for clarity, tone, or concision without adding or removing substantive content.  
# MAGIC • Minor Content Change: 1-3 substantive edits (fact additions, deletions, or small clarifications). Core claims remain unchanged.  
# MAGIC • Content Change: 4-10 substantive edits; information is added, removed, or reorganized significantly.  
# MAGIC • Rewrite - Heavy Edit: many substantive edits (5-20+) but core claims or references from the original remain in revised form.  
# MAGIC • Rewrite - Complete Restart: original is largely discarded; revised text is mostly new content, retaining less than ~10% of original wording or structure.
# MAGIC
# MAGIC EXAMPLES:
# MAGIC 1. **Unchanged**  
# MAGIC    O: “Cats are mammals.”  
# MAGIC    R: “Cats are mammals.”
# MAGIC
# MAGIC 2. **Grammar & Mechanics**  
# MAGIC    O: “He dont like coffee.”  
# MAGIC    R: “He does not like coffee.”
# MAGIC
# MAGIC 3. **Rephrase / Stylistic**  
# MAGIC    O: “The results were surprising to many people.”  
# MAGIC    R: “Many people found the results surprising.”
# MAGIC
# MAGIC 4. **Minor Content Change**  
# MAGIC    O: “Our product ships in July.”  
# MAGIC    R: “Our product ships in July and August.”
# MAGIC
# MAGIC 5. **Content Change** (5 tweaks)  
# MAGIC    O: “The study covered 100 patients in 2022.”  
# MAGIC    R: “The 18-month study enrolled 150 adults and 50 teens between 2022–23.”
# MAGIC
# MAGIC 6. **Rewrite - Heavy Edit**  
# MAGIC    O: “I manage data pipelines using Airflow in Python and SQL.”  
# MAGIC    R: “I orchestrate data pipelines via Airflow, using SQL for transformations on staging tables before loading to downstream analytics datasets.”  
# MAGIC    → Core claims retained, language heavily rewritten.
# MAGIC
# MAGIC 7. **Rewrite - Complete Restart**  
# MAGIC    O: “I manage data pipelines using Airflow in Python and SQL.”  
# MAGIC    R: “My process revolves around Scala-based ETL jobs scheduled in Airflow, with monitoring dashboards in Grafana and automated alerts via Kafka.”  
# MAGIC    → Original largely discarded, rewritten from scratch.
# MAGIC
# MAGIC RETURN FORMAT: exactly one and only one of these for each row:
# MAGIC Unchanged | Grammar & Mechanics | Rephrase / Stylistic | Minor Content Change | Content Change | Rewrite - Heavy Edit | Rewrite - Complete Restart'
# MAGIC     ) AS prompt
# MAGIC   FROM
# MAGIC     users.rafi_kurlansik.2025_dbms_mq_eval
# MAGIC )
# MAGIC SELECT
# MAGIC   original_answer,
# MAGIC   final_answer,
# MAGIC   ai_query(
# MAGIC     'databricks-claude-3-7-sonnet',
# MAGIC     prompt
# MAGIC   ) AS edit_type
# MAGIC FROM
# MAGIC   diffs;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH diffs AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     CONCAT(
# MAGIC       'Original: "',
# MAGIC       original_answer,
# MAGIC       '"\n',
# MAGIC       'Revised: "',
# MAGIC       final_answer,
# MAGIC       '"\n',
# MAGIC       'You are a senior editor with deep expertise in Databricks, data analytics, data engineering, machine learning and artificial intelligence.
# MAGIC
# MAGIC Task: Compare the ORIGINAL vs REVISED answer and return **exactly one** label from this list:
# MAGIC • Unchanged  
# MAGIC • Grammar & Mechanics  
# MAGIC • Rephrase / Stylistic  
# MAGIC • Minor Content Change  
# MAGIC • Content Change  
# MAGIC • Rewrite: Heavy Edit  
# MAGIC • Rewrite: Complete Restart
# MAGIC
# MAGIC DEFINITIONS:
# MAGIC • Unchanged: 0-1 substantive or word-level edits; meaning and wording are essentially identical.  
# MAGIC • Grammar & Mechanics: corrections to spelling, punctuation, capitalization, grammatical agreement, or style-guide conventions only; meaning is unchanged.  
# MAGIC • Rephrase / Stylistic: wording or structure improved for clarity, tone, or concision without adding or removing substantive content.  
# MAGIC • Minor Content Change: 1-3 substantive edits (fact additions, deletions, or small clarifications). Core claims remain unchanged.  
# MAGIC • Content Change: 4-10 substantive edits; information is added, removed, or reorganized significantly.  
# MAGIC • Rewrite - Heavy Edit: many substantive edits (5-20+) but core claims or references from the original remain in revised form.  
# MAGIC • Rewrite - Complete Restart: original is largely discarded; revised text is mostly new content, retaining less than ~10% of original wording or structure.
# MAGIC
# MAGIC EXAMPLES:
# MAGIC 1. **Unchanged**  
# MAGIC    O: “Cats are mammals.”  
# MAGIC    R: “Cats are mammals.”
# MAGIC
# MAGIC 2. **Grammar & Mechanics**  
# MAGIC    O: “He dont like coffee.”  
# MAGIC    R: “He does not like coffee.”
# MAGIC
# MAGIC 3. **Rephrase / Stylistic**  
# MAGIC    O: “The results were surprising to many people.”  
# MAGIC    R: “Many people found the results surprising.”
# MAGIC
# MAGIC 4. **Minor Content Change**  
# MAGIC    O: “Our product ships in July.”  
# MAGIC    R: “Our product ships in July and August.”
# MAGIC
# MAGIC 5. **Content Change** (5 tweaks)  
# MAGIC    O: “The study covered 100 patients in 2022.”  
# MAGIC    R: “The 18-month study enrolled 150 adults and 50 teens between 2022–23.”
# MAGIC
# MAGIC 6. **Rewrite - Heavy Edit**  
# MAGIC    O: “I manage data pipelines using Airflow in Python and SQL.”  
# MAGIC    R: “I orchestrate data pipelines via Airflow, using SQL for transformations on staging tables before loading to downstream analytics datasets.”  
# MAGIC    → Core claims retained, language heavily rewritten.
# MAGIC
# MAGIC 7. **Rewrite - Complete Restart**  
# MAGIC    O: “I manage data pipelines using Airflow in Python and SQL.”  
# MAGIC    R: “My process revolves around Scala-based ETL jobs scheduled in Airflow, with monitoring dashboards in Grafana and automated alerts via Kafka.”  
# MAGIC    → Original largely discarded, rewritten from scratch.
# MAGIC
# MAGIC RETURN FORMAT: exactly one and only one of these for each row:
# MAGIC Unchanged | Grammar & Mechanics | Rephrase / Stylistic | Minor Content Change | Content Change | Rewrite - Heavy Edit | Rewrite - Complete Restart'
# MAGIC     ) AS prompt
# MAGIC   FROM
# MAGIC     users.rafi_kurlansik.2025_dsml_mq_eval
# MAGIC )
# MAGIC SELECT
# MAGIC   original_answer,
# MAGIC   final_answer,
# MAGIC   ai_query(
# MAGIC     'databricks-claude-3-7-sonnet',
# MAGIC     prompt
# MAGIC   ) AS edit_type
# MAGIC FROM
# MAGIC   diffs;

# COMMAND ----------

# MAGIC %pip install plotnine
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH diffs AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     CONCAT(
# MAGIC       'Original: "',
# MAGIC       original_answer,
# MAGIC       '"\n',
# MAGIC       'Revised: "',
# MAGIC       final_answer,
# MAGIC       '"\n',
# MAGIC       'You are a senior editor with deep expertise in Databricks, data analytics, data engineering, machine learning, and artificial intelligence.
# MAGIC
# MAGIC ──────────────────────── TASK ────────────────────────
# MAGIC Given an ORIGINAL answer (O) and a REVISED answer (R), decide how much the meaning has changed and return only one label from the list in “LABELS.”
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
# MAGIC O: “Cats are mammals.”
# MAGIC R: “Cats are mammals.”
# MAGIC 	2.	Rephrasing / Grammar / Style Changes
# MAGIC O: “He dont like coffee.”
# MAGIC R: “He does not like coffee.”
# MAGIC 	3.	Rephrasing / Grammar / Style Changes
# MAGIC O: “The results were surprising to many people.”
# MAGIC R: “Many people found the results surprising.”
# MAGIC 	4.	Minor to Moderate Enhancements
# MAGIC O: “Our product ships in July.”
# MAGIC R: “Our product ships in July and August.”
# MAGIC 	5.	Minor to Moderate Enhancements
# MAGIC O: “The study covered 100 patients in 2022.”
# MAGIC R: “The 18-month study enrolled 150 adults and 50 teens between 2022–23.”
# MAGIC 	6.	Substantial Enhancements
# MAGIC O: “I manage data pipelines using Airflow in Python and SQL.”
# MAGIC R: “I orchestrate data pipelines via Airflow, using SQL for transformations on staging tables before loading to downstream analytics datasets.”
# MAGIC → Core claims retained, language heavily rewritten.
# MAGIC 	7.	Complete Rewrite
# MAGIC O: “I manage data pipelines using Airflow in Python and SQL.”
# MAGIC R: “My process revolves around Scala-based ETL jobs scheduled in Airflow, with monitoring dashboards in Grafana and automated alerts via Kafka.”
# MAGIC → Original largely discarded, rewritten from scratch.
# MAGIC
# MAGIC ──────────────────────── OUTPUT ───────────────────────
# MAGIC Return one bare label token and nothing else, e.g.:
# MAGIC
# MAGIC Minor to Moderate Enhancements'
# MAGIC     ) AS prompt
# MAGIC   FROM
# MAGIC     (
# MAGIC       SELECT
# MAGIC         'DSML MQ' AS Report,
# MAGIC         original_answer,
# MAGIC         final_answer
# MAGIC       FROM
# MAGIC         users.rafi_kurlansik.2025_dsml_mq_eval
# MAGIC       UNION ALL
# MAGIC       SELECT
# MAGIC         'DBMS MQ' AS Report,
# MAGIC         original_answer,
# MAGIC         final_answer
# MAGIC       FROM
# MAGIC         users.rafi_kurlansik.dbms_mq_2025_eval
# MAGIC       UNION ALL
# MAGIC       SELECT
# MAGIC         'IDC DP MarketScape' AS Report,
# MAGIC         original_answer,
# MAGIC         final_answer
# MAGIC       FROM
# MAGIC         users.rafi_kurlansik.2025_idc_data_platforms_eval
# MAGIC     )
# MAGIC )
# MAGIC SELECT
# MAGIC   Report,
# MAGIC   original_answer,
# MAGIC   final_answer,
# MAGIC   ai_classify(
# MAGIC     prompt,
# MAGIC     ARRAY(
# MAGIC       'Unchanged',
# MAGIC       'Rephrasing / Grammar / Style Changes',
# MAGIC       'Minor to Moderate Enhancements',
# MAGIC       'Substantial Enhancements',
# MAGIC       'Complete Rewrite'
# MAGIC     )
# MAGIC   ) AS `Difference from Initial to Final Draft`
# MAGIC FROM
# MAGIC   diffs
# MAGIC WHERE
# MAGIC   original_answer IS NOT NULL
# MAGIC   AND final_answer IS NOT NULL;

# COMMAND ----------

reports_df = _sqldf.toPandas()

# COMMAND ----------

order = [
    'Unchanged',
    'Rephrasing / Grammar / Style Changes',
    'Minor to Moderate Enhancements',
    'Substantial Enhancements',
    'Complete Rewrite'
]

summary_stats_df = (
    reports_df
    .groupby('Difference from Initial to Final Draft')
    .agg(
        count=('Difference from Initial to Final Draft', 'size'),
        percentage=('Difference from Initial to Final Draft', lambda x: 100 * len(x) / len(reports_df))
    )
    .reset_index()
)

summary_stats_df['Difference from Initial to Final Draft'] = pd.Categorical(
    summary_stats_df['Difference from Initial to Final Draft'],
    categories=order,
    ordered=True
)

summary_stats_df = summary_stats_df.sort_values('Difference from Initial to Final Draft').reset_index(drop=True)

# Add total row
total_row = pd.DataFrame({
    'Difference from Initial to Final Draft': ['Total'],
    'count': [summary_stats_df['count'].sum()],
    'percentage': [summary_stats_df['percentage'].sum()]
})
summary_stats_df = pd.concat([summary_stats_df, total_row], ignore_index=True)

display(summary_stats_df)

# COMMAND ----------

summary_stats_df

# COMMAND ----------

from plotnine import *
import pandas as pd

# Capitalize the MQ column values

reports_df['Report'] = reports_df['Report'].str.upper()

# Calculate group-wise percentages so each 'mq' sums to 100%
grouped_reports_df = reports_df.groupby(['Report', 'edit_type']).size().reset_index(name='count')
grouped_reports_df['pct_total'] = grouped_reports_df.groupby('mq')['count'].transform(lambda x: x / x.sum() * 100)

# Flip the order so 'Unchanged' is on the bottom
order = [
    'Rewrite - Complete Restart',
    'Major Content Change',
    'Content Change',
    'Minor Content Change',
    'Rephrase / Stylistic',
    'Grammar & Mechanics',
    'Unchanged'
]

grouped_reports_df['edit_type'] = pd.Categorical(grouped_reports_df['edit_type'],
                                  categories=order,
                                  ordered=True)

# Use a crisp, colorblind-friendly palette
crisp_colors = [
    "#b30000",  # red
    "#e34a33",  # red-orange
    "#e08214",  # dark orange
    "#fdb863",  # orange
    "#7fcdbb",  # light blue-green
    "#4e9eda",  # blue
    "#1b9e77"   # green
]

p = (ggplot(grouped_reports_df) 
     + aes(x='mq', y='pct_total', fill='edit_type', label='pct_total')
     + geom_col(width=0.55, position='stack')
     + geom_text(position=position_stack(vjust=0.5), size=7, color='white', format_string='{:.1f}%')
     + scale_y_continuous(labels=lambda l: [f'{v:.0f}%' for v in l])
     + scale_fill_manual(values=crisp_colors)
     + labs(x='', y='% of Answers', title='Evaluation of AI-generated vs. Submitted Answers for 2025 MQs')
     + theme_minimal()
     + theme(
         figure_size=(9,6),
         legend_title=element_blank(),
         axis_text_x=element_text(size=11, weight='bold'),
         axis_title_y=element_text(size=11)
     ))

print(p)
p.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 0️⃣  Parameters -------------------------------------------------------------
# MAGIC -- Define the label set once so it can be reused.
# MAGIC WITH params AS (
# MAGIC   SELECT array(
# MAGIC            'Unchanged',
# MAGIC            'Grammar & Mechanics',
# MAGIC            'Rephrase / Stylistic',
# MAGIC            'Minor Content Change',
# MAGIC            'Content Change',
# MAGIC            'Rewrite - Heavy Edit',
# MAGIC            'Rewrite - Complete Restart'
# MAGIC          ) AS labels
# MAGIC ),
# MAGIC
# MAGIC -- 1️⃣  Collect rows from both evaluation tables ------------------------------
# MAGIC eval AS (
# MAGIC   SELECT 'dsml' AS mq, original_answer, final_answer
# MAGIC   FROM   users.rafi_kurlansik.2025_dsml_mq_eval
# MAGIC   UNION ALL
# MAGIC   SELECT 'dbms' AS mq, orignal_answer AS original_answer, final_answer
# MAGIC   FROM   users.rafi_kurlansik.dbms_mq_2025_eval
# MAGIC ),
# MAGIC
# MAGIC -- 2️⃣  Classify each pair -----------------------------------------------------
# MAGIC classified AS (
# MAGIC   SELECT
# MAGIC       e.mq,
# MAGIC       ai_classify(
# MAGIC         CONCAT(
# MAGIC           'Original: "', e.original_answer, '"\n',
# MAGIC           'Revised: "',  e.final_answer  , '"\n',
# MAGIC           'You are a senior editor with deep expertise in Databricks, data analytics, data engineering, machine learning and artificial intelligence.
# MAGIC
# MAGIC           Task: Compare the ORIGINAL vs REVISED answer and return **exactly one** label from this list:
# MAGIC           • Unchanged  
# MAGIC           • Grammar & Mechanics  
# MAGIC           • Rephrase / Stylistic  
# MAGIC           • Minor Content Change  
# MAGIC           • Content Change  
# MAGIC           • Rewrite - Heavy Edit  
# MAGIC           • Rewrite - Complete Restart
# MAGIC
# MAGIC           DEFINITIONS:
# MAGIC           • Unchanged: 0-1 substantive or word-level edits; meaning and wording are essentially identical.  
# MAGIC           • Grammar & Mechanics: corrections to spelling, punctuation, capitalization, grammatical agreement, or style-guide conventions only; meaning is unchanged.  
# MAGIC           • Rephrase / Stylistic: wording or structure improved for clarity, tone, or concision without adding or removing substantive content.  
# MAGIC           • Minor Content Change: 1-3 substantive edits (fact additions, deletions, or small clarifications). Core claims remain unchanged.  
# MAGIC           • Content Change: 4-10 substantive edits; information is added, removed, or reorganized significantly.  
# MAGIC           • Rewrite - Heavy Edit: many substantive edits (5-20+) but core claims or references from the original remain in revised form.  
# MAGIC           • Rewrite - Complete Restart: original is largely discarded; revised text is mostly new content, retaining less than ~10% of original wording or structure.
# MAGIC
# MAGIC           EXAMPLES:
# MAGIC           1. **Unchanged**  
# MAGIC             O: “Cats are mammals.”  
# MAGIC             R: “Cats are mammals.”
# MAGIC
# MAGIC           2. **Grammar & Mechanics**  
# MAGIC             O: “He dont like coffee.”  
# MAGIC             R: “He does not like coffee.”
# MAGIC
# MAGIC           3. **Rephrase / Stylistic**  
# MAGIC             O: “The results were surprising to many people.”  
# MAGIC             R: “Many people found the results surprising.”
# MAGIC
# MAGIC           4. **Minor Content Change**  
# MAGIC             O: “Our product ships in July.”  
# MAGIC             R: “Our product ships in July and August.”
# MAGIC
# MAGIC           5. **Content Change** (5 tweaks)  
# MAGIC             O: “The study covered 100 patients in 2022.”  
# MAGIC             R: “The 18-month study enrolled 150 adults and 50 teens between 2022–23.”
# MAGIC
# MAGIC           6. **Rewrite - Heavy Edit**  
# MAGIC             O: “I manage data pipelines using Airflow in Python and SQL.”  
# MAGIC             R: “I orchestrate data pipelines via Airflow, using SQL for transformations on staging tables before loading to downstream analytics datasets.”  
# MAGIC             → Core claims retained, language heavily rewritten.
# MAGIC
# MAGIC           7. **Rewrite - Complete Restart**  
# MAGIC             O: “I manage data pipelines using Airflow in Python and SQL.”  
# MAGIC             R: “My process revolves around Scala-based ETL jobs scheduled in Airflow, with monitoring dashboards in Grafana and automated alerts via Kafka.”  
# MAGIC             → Original largely discarded, rewritten from scratch.
# MAGIC
# MAGIC           RETURN FORMAT: exactly one and only one of these for each row:
# MAGIC           Unchanged | Grammar & Mechanics | Rephrase / Stylistic | Minor Content Change | Content Change | Rewrite - Heavy Edit | Rewrite - Complete Restart'
# MAGIC         ),
# MAGIC         array(
# MAGIC           'Unchanged',
# MAGIC           'Grammar & Mechanics',
# MAGIC           'Rephrase / Stylistic',
# MAGIC           'Minor Content Change',
# MAGIC           'Content Change',
# MAGIC           'Rewrite - Heavy Edit',
# MAGIC           'Rewrite - Complete Restart'
# MAGIC         )
# MAGIC       ) AS edit_type
# MAGIC   FROM eval e
# MAGIC ),
# MAGIC
# MAGIC -- 3️⃣  Aggregate counts & percentages ----------------------------------------
# MAGIC agg AS (
# MAGIC   SELECT
# MAGIC       mq,
# MAGIC       edit_type,
# MAGIC       COUNT(*)                               AS answer_cnt,
# MAGIC       ROUND(100.0 * COUNT(*) 
# MAGIC             / SUM(COUNT(*)) OVER (PARTITION BY mq), 2) AS pct_total
# MAGIC   FROM classified
# MAGIC   GROUP BY mq, edit_type
# MAGIC )
# MAGIC
# MAGIC -- 4️⃣  Pivot into wide form ---------------------------------------------------
# MAGIC SELECT *
# MAGIC FROM agg
# MAGIC PIVOT (
# MAGIC   MAX(pct_total)            -- swap to MAX(answer_cnt) for absolute bars
# MAGIC   FOR edit_type IN (
# MAGIC     'Unchanged',
# MAGIC     'Grammar & Mechanics',
# MAGIC     'Rephrase / Stylistic',
# MAGIC     'Minor Content Change',
# MAGIC     'Content Change',
# MAGIC     'Rewrite - Heavy Edit',
# MAGIC     'Rewrite - Complete Restart'
# MAGIC   )
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1️⃣ Gather & label answers from both eval tables
# MAGIC WITH eval AS (
# MAGIC   SELECT 'dsml' AS mq, original_answer, final_answer FROM users.rafi_kurlansik.2025_dsml_mq_eval
# MAGIC   UNION ALL
# MAGIC   SELECT 'dbms' AS mq, original_answer, final_answer FROM users.rafi_kurlansik.2025_dbms_mq_eval
# MAGIC ),
# MAGIC classified AS (
# MAGIC   SELECT
# MAGIC     mq,
# MAGIC     ai_classify(
# MAGIC       CONCAT(
# MAGIC         'Original: "', original_answer, '"\n',
# MAGIC         'Revised: "',  final_answer , '"\n',
# MAGIC         /* …prompt text unchanged… */
# MAGIC         ''),
# MAGIC       ARRAY(
# MAGIC         'Unchanged','Grammar & Mechanics','Rephrase / Stylistic',
# MAGIC         'Minor Content Change','Content Change',
# MAGIC         'Rewrite - Heavy Edit','Rewrite - Complete Restart'
# MAGIC       )
# MAGIC     ) AS edit_type
# MAGIC   FROM eval
# MAGIC ),
# MAGIC
# MAGIC -- 2️⃣ Counts
# MAGIC counts AS (
# MAGIC   SELECT mq, edit_type, COUNT(*) AS answer_cnt
# MAGIC   FROM   classified
# MAGIC   GROUP  BY mq, edit_type
# MAGIC ),
# MAGIC
# MAGIC -- 3️⃣ Percent-of-total for 100 % bars
# MAGIC dist AS (
# MAGIC   SELECT
# MAGIC     c.mq,
# MAGIC     c.edit_type,
# MAGIC     c.answer_cnt,
# MAGIC     ROUND(100.0 * c.answer_cnt / SUM(c.answer_cnt) OVER (PARTITION BY c.mq), 2) AS pct_total
# MAGIC   FROM counts c
# MAGIC )
# MAGIC
# MAGIC -- 4️⃣ Wide (pivot) form if you need columns per label
# MAGIC SELECT *
# MAGIC FROM dist
# MAGIC PIVOT (
# MAGIC   SUM(answer_cnt)
# MAGIC   FOR edit_type IN (
# MAGIC     'Unchanged','Grammar & Mechanics','Rephrase / Stylistic',
# MAGIC     'Minor Content Change','Content Change',
# MAGIC     'Rewrite - Heavy Edit','Rewrite - Complete Restart'
# MAGIC   )
# MAGIC );