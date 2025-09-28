# 📖 Understanding the Dictionary in Silver Enrichment

## What the Dictionary Is
The **dictionary** (`auto.landing.pbp_dictionary`) is a lookup table that provides metadata context for raw PBP fields.  
For each file + field, it can hold:

- **`file`** → PBP file the field came from  
- **`name`** → raw field/column name (e.g., `pbp_b10b_ded_amt`)  
- **`title`** → human-friendly title (e.g., *Ambulance Deductible*)  
- **`field_title`** → alternate label  
- **`json_question`** → CMS question text  
- **`codes`, `code_values`** → enumerations (e.g., `01 = Yes`, `02 = No`)  

---

## How It Shows Up in Silver
When Bronze is enriched → Silver, the dictionary populates additional `bendict_*` columns:

- `bendict_title` ← dictionary.title  
- `bendict_field_title` ← dictionary.field_title  
- `bendict_json_question` ← dictionary.json_question  
- `bendict_code` ← dictionary.codes  
- `bendict_code_values` ← dictionary.code_values  
- `bendict_value_canon` / `bendict_value_decoded` ← mapping raw values to dictionary codes  

So in **Silver**, each row now has both:
- The **raw field/value** (from Bronze)  
- The **dictionary metadata** (if present)  

---

## What We Found
- **All `bendict_*` fields = `novalue`.**  
  - Profiling showed only **1 distinct value** per dictionary column → effectively placeholders.  

- The **real semantics** come from:  
  - **`col_name`** → systematic naming conventions (`_ded_amt`, `_copay_max`, `_limit_unit_per_d`, etc.)  
  - **`raw_value`** → actual benefit data:  
    - Numeric amounts (e.g., `$50`, `20%`)  
    - Free-text descriptions (e.g., *“Car, wheelchair access vehicle”*)  

---

## Why It Matters
- The dictionary join is technically working (100% coverage ✅).  
- But because dictionary content is sparse, **we must rely on parsing `col_name` + `raw_value`** to build Gold and support agents.  
- Long-term, enhancing the dictionary would:  
  - Reduce parsing complexity  
  - Improve readability  
  - Help non-technical users interpret results  

---

## Next Steps (Team Discussion)

### 🗂 Gold Table Strategy
We should normalize Silver into four Gold tables:

1. **`plan_master`**  
   - Source: Section A  
   - Fields: plan type, SNP, hospice, network flags, metadata  

2. **`plan_costsharing`**  
   - Sources: Sections C, D, inpatient (B1A/B1B), outpatient, SNF, emergency, etc.  
   - Schema:  
     ```sql
     plan_id,
     service_category,
     coverage_type,   -- copay | coins | ded | max_plan | limit | other
     metric,          -- min | max | value | yn
     unit,            -- amt | pct | per | per_day | flag | other
     raw_value
     ```

3. **`plan_limits`**  
   - Frequency caps (e.g., # of visits, screenings per year)  
   - Sources: Preventive, Other Services, Home Health  

4. **`plan_services_long`**  
   - Narrative/unstructured services  
   - Sources: Other Services, Ambulance/Transport, Hearing, Dental free-text  
   - Fields kept for RAG embeddings: `col_name`, `raw_value`, `bendict_json_question`  

---

### 🤖 RAG Strategy
We propose a **hybrid strategy**:

- **Direct DB retrieval** for structured Gold tables  
  - Deductibles  
  - Copays  
  - Coinsurance  
  - Max plan amounts  
  - Plan metadata  

- **RAG embeddings** for unstructured/narrative values  
  - Ambulance descriptions  
  - “Other” services  
  - Special eligibility rules  

**Embedding text template:**
[bendict_json_question] + [cleaned col_name] + [raw_value]


Example:  
> *"Does plan H0028 cover ambulance? Car, wheelchair access vehicle."*  

This allows natural-language Q&A for agents while preserving structured queries for standard reporting.

---

## 🚦 Action Items
1. **Team Alignment** → confirm dictionary (`bendict_*`) isn’t usable today.  
2. **Developer Work** → design Gold schemas using `col_name` parsing.  
3. **RAG Work** → build embeddings for `plan_services_long`.  
4. **Future Work** → explore enhancing `pbp_dictionary` so `bendict_*` fields add meaningful metadata.  

---

👉 Next deliverables (once the team aligns):  
- **Gold schema DDL** (`CREATE TABLE ...`) + example queries  
- **RAG architecture doc** (retrieval flow, metadata to embed, usage examples)  
