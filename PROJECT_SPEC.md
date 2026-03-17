
# Project Document 1 — Pan-cCRE: Pangenome-Aware Regulatory Element Registry

## Plain-English overview

We are building a public registry of human regulatory elements that **change across human haplotypes**. Start with ENCODE candidate cis-regulatory elements (cCREs). For each cCRE, ask: is it conserved, sequence-diverged, fractured by structural variation, absent, duplicated, or replaced by nearby inserted sequence on real human haplotypes? Then rank the altered loci for likely functional importance using **cheap sequence features first** and **expensive genome-model scoring only on a shortlist**.

This is worth doing because the reference genome is incomplete for regulatory biology. ENCODE now catalogs **2.37 million human cCREs**. The human pangenome adds **119 million base pairs** beyond GRCh38, much of it structural variation. Public noncoding CRISPRi and MPRA resources are now large enough to evaluate prioritization rigorously. AlphaGenome and open DNA foundation models can score sequence effects, but they should be treated as **scarce oracles**, not bulk annotators. The durable output is not a tuned model. It is a registry, a benchmark, and a prioritized queue for experimental follow-up.

## Why this is important, novel, and interesting

1. **ENCODE expanded the human cCRE registry to 2.37 million elements** and reports functional characterization data for >90% of human cCREs. That gives us a strong reference catalog and many public validation hooks.  
   - Moore et al., *Nature* (2026), “An expanded registry of candidate cis-regulatory elements”  
   - https://www.nature.com/articles/s41586-025-09909-9

2. **The draft human pangenome adds 119 million base pairs of euchromatic polymorphic sequence** beyond GRCh38, much of it from structural variation. That means reference-only regulatory catalogs are incomplete by construction.  
   - Liao et al., *Nature* (2023), “A draft human pangenome reference”  
   - https://www.nature.com/articles/s41586-023-05896-x

3. **Large-scale non-reference sequence studies already show functional signal**. One human nonreference-sequence analysis found **565 NRS eQTLs, including 426 novel findings**.  
   - Wu et al., *Nucleic Acids Research* (2024), “Human pangenome analysis of sequences missing from the reference genome”  
   - https://academic.oup.com/nar/article/52/5/2212/7607875

4. **Public assay resources are strong enough to benchmark prioritization**.  
   - ENCODE4 noncoding CRISPRi analysis: **108 screens**, >540,000 perturbations, and screening guidance for cCRE–gene mapping  
   - Yao et al., *Nature Methods* (2024)  
   - https://www.nature.com/articles/s41592-024-02216-7
   - MPRAbase: **17,718,677 tested elements**, **130 experiments**, **35 cell types**  
   - Zhao et al., *Database* / PubMed record (2025)  
   - https://pubmed.ncbi.nlm.nih.gov/40262894/

5. **Foundation models are useful, but not uniformly reliable**. AlphaGenome reports state-of-the-art multimodal variant-effect prediction, while 2025 DNA foundation model benchmarks show strong task-to-task variability. That makes a disagreement-aware, model-agnostic benchmark much more valuable than another adapter paper.  
   - Avsec et al., *Nature* (2026), “Advancing regulatory variant effect prediction with AlphaGenome”  
   - https://www.nature.com/articles/s41586-025-10014-0  
   - Feng et al., *Nature Communications* (2025), “Benchmarking DNA foundation models for genomic and genetic tasks”  
   - https://www.nature.com/articles/s41467-025-65823-8  
   - AlphaGenome API documentation / GitHub: suitable for **thousands** of predictions, not >1 million bulk calls  
   - https://github.com/google-deepmind/alphagenome

---

## 1. Project thesis

### Core question
Which ENCODE cCREs are **polymorphic across human haplotypes**, and which altered loci are most likely to have **measurable functional consequences**?

### Main hypothesis
Reference cCREs are not a fixed catalog. Across haplotypes, a nontrivial subset of cCREs will be absent, split, duplicated, or sequence-diverged, and those altered states will be enriched for functional effects in public assays and QTL resources.

### Secondary hypothesis
**Cross-scorer disagreement** is useful. If AlphaGenome, open DNA models, and cheap heuristics disagree strongly at a locus, that locus is more likely to sit near poorly modeled biology or functionally important non-reference sequence.

### What we are explicitly not doing
- No training of a new foundation model.
- No genome-wide exhaustive AlphaGenome scan.
- No whole-pangenome de novo regulatory discovery in phase 1.
- No clinical variant classifier.
- No attempt to prove causal disease mechanisms in the first paper.

---

## 2. First paper target

### Paper title shape
**A pangenome-aware registry of polymorphic candidate cis-regulatory elements in human immune and hematopoietic loci**

### Minimal publishable claim
1. A reference-anchored registry can classify cCREs across haplotypes into biologically interpretable state classes.
2. Altered cCRE states are enriched for functional signal in held-out public assays.
3. Shortlist ranking beats simple heuristics.
4. Cross-scorer disagreement adds lift or at least diagnoses failure modes.

### Sharp biological result to pair with the resource
Choose **one** of:
- Immune/hematopoietic loci show a high burden of polymorphic cCRE states with assay enrichment.
- A subclass of “absent but locally replaced” cCREs is more assay-active than simple absence would predict.
- TE-enriched replacement candidates contribute disproportionally to high-confidence hits.

The project is not done when the registry exists. It is done when one of these claims is supported cleanly.

---

## 3. Phase-1 scope

### Biological scope
- Human only
- GRCh38 reference anchor
- **Immune / hematopoietic context family only** for phase 1
- Autosomal euchromatic regions only
- ENCODE cCREs only as starting anchors
- Non-reference candidate discovery allowed only in local windows around altered anchors

### Data scope
Required:
- ENCODE cCRE registry (SCREEN / ENCODE4)
- Human pangenome haplotype assemblies or assembly-alignment resources
- Public structural variant context
- ENCODE noncoding CRISPRi screen resource
- MPRAbase
- Non-reference sequence resource with anchored NRSs if available

Optional in phase 1:
- GTEx / eQTL overlays
- GWAS fine-mapping overlays
- TE subclass annotation as a deeper analysis layer

### Compute scope
- CPU-first pipeline
- DuckDB + Parquet storage
- Cheap sequence features on all candidates
- Expensive model calls only on a shortlist
- Total expensive model calls in phase 1 should stay on the order of **10^3 to 10^4**, not 10^6

---

## 4. Success and failure criteria

### Must-have success criteria
- Top-k assay hit rate is at least **2x** the best cheap baseline on **study-held-out** evaluation.
- Registry state calls are auditable and reproducible from raw files.
- At least one state class shows clear functional enrichment.
- The pipeline produces a public artifact others can query: files + schema + API + reproducible build.

### Nice-to-have success criteria
- Cross-scorer disagreement adds lift.
- Replacement candidates outperform simple “nearest inserted sequence” rules.
- TE subclasses explain part of the hit enrichment.

### Stop conditions
Stop or narrow scope if:
- Projection ambiguity remains >25% among shortlisted loci.
- Best model-assisted ranking does not beat motif/conservation/distance baselines.
- The project becomes mostly graph engineering with weak biological output.
- Assay labels are too sparse or too inconsistent for a clean benchmark.

---

## 5. Biological framing and interpretation rules

### What counts as a “polymorphic cCRE”
A reference cCRE is polymorphic if, on at least one haplotype:
- it is absent,
- partially missing / fractured,
- duplicated,
- mapped with substantial sequence divergence,
- or replaced by a nearby inserted candidate sequence.

### Why reference-anchored first
This avoids a combinatorial search of all non-reference sequence. It lets us measure a concrete quantity:
> among known human regulatory elements, which are not stable across haplotypes?

That yields a cleaner paper than “we scanned everything and found many candidates.”

### Why immune / hematopoietic first
- ENCODE and CRISPR resources are strong.
- Regulatory activity is rich and disease-relevant.
- The context is broad enough for impact but narrow enough for tractable benchmarking.

---

## 6. High-level system architecture

```text
raw_sources
  -> manifest + checksums
  -> normalized reference tables
  -> haplotype projection
  -> altered-state caller
  -> local replacement-candidate discovery
  -> cheap feature extraction
  -> expensive scorer fanout
  -> ranker / calibrator
  -> evaluation / benchmark
  -> registry build + API + reports
```

### Core design principles
- Every derived row carries source provenance.
- All transformations are deterministic and versioned.
- Sequence models are plug-ins, not hard-coded assumptions.
- Evaluation is study-held-out and leakage-audited.
- Registry rows are first-class products, not just intermediate files.

---

## 7. Repository layout

```text
pan-ccre/
  README.md
  pyproject.toml
  Makefile
  configs/
    project.yaml
    contexts/
      immune_hematopoietic.yaml
    scorers/
      cheap_baseline.yaml
      alphagenome.yaml
      ntv2_embedding.yaml
  data/
    raw/               # immutable downloads by release/checksum
    interim/
    processed/
    registry/
  docs/
    schemas/
    assay_inventory.md
    design_decisions.md
  src/
    panccre/
      cli/
      manifests/
      ingest/
      normalize/
      projection/
      state_calling/
      candidate_discovery/
      features/
      scorers/
      ranking/
      evaluation/
      registry/
      api/
      reports/
      utils/
  notebooks/
    00_smoke_tests.ipynb
    01_projection_qc.ipynb
    02_assay_enrichment.ipynb
  tests/
    unit/
    integration/
    golden/
  scripts/
    build_manifest.py
    run_phase1.py
```

---

## 8. Source manifest spec

Every external source must have a manifest row.

```yaml
source_id: encode_ccre_v4
name: ENCODE cCRE registry
version: "2026-01"
license: "public / follow upstream"
download_urls:
  - "<upstream-url>"
checksum: "<sha256>"
format: "bed/tsv/json"
genome_build: "GRCh38"
notes: "Reference anchor catalog"
```

### Required manifest fields
- `source_id`
- `version`
- `download_url`
- `download_date`
- `checksum`
- `license`
- `genome_build`
- `parser_version`

No downstream build should run without a frozen manifest.

---

## 9. Canonical data contracts

Use Parquet as the canonical intermediate format. Use DuckDB for joins and evaluation.

### 9.1 `ccre_ref`
One row per reference cCRE.

```sql
CREATE TABLE ccre_ref (
  ccre_id TEXT PRIMARY KEY,
  chr TEXT,
  start INT,
  "end" INT,
  strand TEXT,
  ccre_class TEXT,
  biosample_count INT,
  context_group TEXT,
  anchor_width INT,
  source_release TEXT
);
```

### 9.2 `hap_projection`
One row per `(ccre_id, haplotype_id)`.

```sql
CREATE TABLE hap_projection (
  ccre_id TEXT,
  haplotype_id TEXT,
  ref_chr TEXT,
  ref_start INT,
  ref_end INT,
  alt_contig TEXT,
  alt_start INT,
  alt_end INT,
  orientation TEXT,
  map_status TEXT,                -- exact, diverged, fractured, absent, duplicated, ambiguous
  coverage_frac DOUBLE,
  seq_identity DOUBLE,
  split_count INT,
  copy_count INT,
  flank_synteny_confidence DOUBLE,
  mapping_method TEXT,
  PRIMARY KEY (ccre_id, haplotype_id)
);
```

### 9.3 `ccre_state`
One row per `(ccre_id, haplotype_id)` after state calling.

```sql
CREATE TABLE ccre_state (
  ccre_id TEXT,
  haplotype_id TEXT,
  state_class TEXT,               -- conserved/diverged/fractured/absent/duplicated/replaced/ambiguous
  state_reason JSON,
  local_sv_class TEXT,
  replacement_candidate_id TEXT,
  qc_flag TEXT,
  PRIMARY KEY (ccre_id, haplotype_id)
);
```

### 9.4 `replacement_candidate`
One row per local non-reference candidate.

```sql
CREATE TABLE replacement_candidate (
  candidate_id TEXT PRIMARY KEY,
  parent_ccre_id TEXT,
  haplotype_id TEXT,
  window_class TEXT,              -- absent_window / fracture_gap / local_insertion / duplicate_neighbor
  alt_contig TEXT,
  alt_start INT,
  alt_end INT,
  seq_len INT,
  repeat_class TEXT,
  te_family TEXT,
  motif_count INT,
  gc_content DOUBLE,
  nearest_gene TEXT,
  nearest_gene_distance INT
);
```

### 9.5 `feature_matrix`
Wide or tall feature table; tall form is easier for versioning.

```sql
CREATE TABLE feature_matrix (
  entity_id TEXT,                 -- ccre_id or candidate_id
  entity_type TEXT,               -- ref_state or replacement_candidate
  feature_name TEXT,
  feature_value DOUBLE,
  feature_version TEXT,
  PRIMARY KEY (entity_id, entity_type, feature_name, feature_version)
);
```

### 9.6 `scorer_output`
One row per `(entity_id, scorer_name, assay_proxy, context_group)`.

```sql
CREATE TABLE scorer_output (
  entity_id TEXT,
  entity_type TEXT,
  scorer_name TEXT,
  assay_proxy TEXT,               -- accessibility/expression/contact/splicing/mpra-like
  context_group TEXT,
  ref_score DOUBLE,
  alt_score DOUBLE,
  delta_score DOUBLE,
  uncertainty DOUBLE,
  run_id TEXT,
  PRIMARY KEY (entity_id, scorer_name, assay_proxy, context_group, run_id)
);
```

### 9.7 `validation_link`
One row per external functional label.

```sql
CREATE TABLE validation_link (
  entity_id TEXT,
  entity_type TEXT,
  study_id TEXT,
  assay_type TEXT,                -- MPRA/CRISPRi/STARR/eQTL/etc
  label TEXT,                     -- hit/non-hit/effect_direction if available
  effect_size DOUBLE,
  cell_context TEXT,
  publication_year INT,
  holdout_group TEXT
);
```

---

## 10. Projection layer spec

### Goal
Map each reference cCRE to each haplotype and determine whether the locus is preserved, altered, or absent.

### Inputs
- `ccre_ref`
- haplotype assemblies or upstream assembly alignments
- structural variant context if available

### Preferred implementation order
1. Use **upstream assembly alignments / lift resources** if available.
2. Fall back to flank-based local alignment only for unresolved loci.

### Flank-based rescue method
For a reference cCRE:
- extract left flank: 1 kb
- extract element
- extract right flank: 1 kb
- align left and right flanks independently to haplotype contig
- require:
  - both flanks align with high identity,
  - same contig,
  - expected orientation,
  - distance compatible with the element length plus local indel tolerance

### State-calling thresholds (initial defaults)
These are defaults; keep them in config.

```yaml
projection:
  min_flank_aln_len: 500
  min_flank_identity: 0.95
  min_coverage_frac_conserved: 0.90
  min_identity_conserved: 0.97
  min_coverage_frac_diverged: 0.75
  max_split_count_conserved: 1
  duplicate_copy_threshold: 2
  ambiguous_if_multiple_good_maps: true
```

### Projection output classes
Internal `map_status`:
- `exact`
- `diverged`
- `fractured`
- `absent`
- `duplicated`
- `ambiguous`

Final biological `state_class` can combine `map_status` with local replacement logic.

### Required QC plots
- coverage fraction histogram
- sequence identity histogram
- state class by chromosome
- ambiguous rate by cCRE class
- state class by cCRE class

---

## 11. Local replacement-candidate discovery spec

### Trigger
Run only for loci with `fractured` or `absent` status, and optionally for `duplicated`.

### Search window
Configurable. Start with:
- `±25 kb` reference neighborhood around anchor
- or local haplotype insertion neighborhood if explicit insertion edges are available

### Candidate generation
Within each search window:
1. enumerate inserted or non-reference intervals
2. filter out obvious junk:
   - >80% simple repeats / low complexity
   - extremely short fragments
3. retain TE-derived sequence
4. break long inserted segments into candidate windows only if necessary

### Cheap candidate features
- sequence length
- GC content
- CpG density
- motif counts from a fixed TF library
- motif grammar summaries
- repeat class
- TE family
- nearest-gene distance
- local cCRE density
- local SV class
- overlap with known NRS/eQTL resources if available

### Candidate labels
Do **not** claim these are true cCREs in phase 1. Call them:
- `replacement candidates`
- `inserted candidates`
- `local non-reference candidates`

Only assay enrichment justifies stronger language.

---

## 12. Feature engineering spec

### Cheap features (required)
- interval length
- GC content
- CpG observed/expected
- motif density
- motif diversity
- max motif score
- repeat fraction
- TE one-hot family
- distance to nearest gene
- distance to nearest cCRE
- local cCRE density in ±100 kb
- local SV type
- state class one-hot

### Optional but useful features
- sequence embeddings from an open DNA model
- conservation where defined on reference anchors
- reference class metadata from ENCODE

### Explicit rule
Cheap features must be enough to run the entire pipeline without any expensive scorer.

---

## 13. Expensive scorer layer

### Philosophy
Treat foundation models as high-value instruments. They are not the backbone of the system.

### Required scorers in phase 1
1. **Heuristic baseline scorer**
   - motif/conservation/distance model
2. **Open-model scorer**
   - DNA embedding model with a small linear or gradient-boosted head trained on public assay labels
3. **AlphaGenome scorer**
   - only on shortlisted entities

### AlphaGenome usage rules
- Batch only shortlisted loci.
- Keep each run fully logged with sequence window and output modality.
- Use model outputs for **delta scoring** between reference and alt sequence windows.
- Do not use AlphaGenome for an exhaustive whole-catalog sweep.

### AlphaGenome budget rule
Keep phase-1 calls within a budget that is compatible with the public API’s intended use for “thousands” of predictions. If larger runs are needed, move to a local research release only after the cheap baseline has already proven value.

### Scorer interface
Every scorer must implement:

```python
class BaseScorer(Protocol):
    name: str
    version: str

    def score_batch(
        self,
        entities: list[ScoringEntity],
        context_group: str,
        assay_proxies: list[str],
    ) -> list[ScorerResult]:
        ...
```

### Required `ScorerResult`
```python
@dataclass
class ScorerResult:
    entity_id: str
    entity_type: str
    scorer_name: str
    assay_proxy: str
    context_group: str
    ref_score: float | None
    alt_score: float | None
    delta_score: float | None
    uncertainty: float | None
    metadata: dict
```

---

## 14. Ranking and calibration

### Goal
Produce a ranked queue of altered loci likely to have functional consequences.

### Phase-1 model family
Allowed:
- logistic regression
- LightGBM / XGBoost
- isotonic or Platt calibration

Not allowed:
- transformer fine-tuning
- end-to-end deep sequence model training
- graph neural net vanity work

### Ranker inputs
- cheap features
- state class
- open-model scores
- AlphaGenome deltas if present
- disagreement statistics

### Required disagreement features
- score variance across scorers
- sign disagreement count
- rank disagreement count
- max-min delta
- missingness pattern across scorers

### Output queues
Produce at least three queues:
1. **assay-priority queue**
2. **replacement-candidate queue**
3. **high-disagreement queue**

High-disagreement is a discovery queue, not necessarily a hit-maximization queue.

---

## 15. Evaluation design

### Primary evaluation task
Given altered cCRE states and replacement candidates, rank entities so that the top-k are enriched for **held-out functional positives**.

### Primary metric
- `hit_rate_at_k`

Report for:
- k = 50, 100, 500, 1000
- MPRA-only
- CRISPRi-only
- combined assay label set

### Secondary metrics
- precision-recall AUC
- enrichment over matched negatives
- odds ratio by state class
- calibration
- lift from disagreement features
- performance by cCRE class
- performance by assay family

### Leakage rules
Never random-split rows from the same assay study into train and test as the headline result.

Required holdouts:
- **publication holdout**
- **locus neighborhood holdout**
- optionally chromosome holdout for sensitivity analysis

### Matched negatives
For each positive assay entity, sample negatives matched on:
- length bucket
- GC bucket
- distance-to-gene bucket
- context group

This prevents the ranker from winning on trivial confounds.

---

## 16. Registry product spec

### Deliverables
1. `polymorphic_ccre_registry.parquet`
2. `replacement_candidates.parquet`
3. `scorer_outputs.parquet`
4. `validation_links.parquet`
5. `README + schema docs`
6. query API
7. static report with figures
8. frozen benchmark split files

### Registry row minimum fields
- entity ID
- source anchor cCRE
- state class
- coordinates
- provenance
- top-level evidence summary
- ranking scores
- QC flags

### Public-facing API endpoints
Minimal FastAPI service:

- `GET /health`
- `GET /ccre/{ccre_id}`
- `GET /candidate/{candidate_id}`
- `GET /search?gene=...`
- `GET /search?state_class=absent`
- `GET /top_hits?context=immune_hematopoietic&k=100`
- `GET /downloads`

No authentication in phase 1 unless deployment requires it.

---

## 17. Reports and figures

Required first-report figures:
1. state class distribution across haplotypes
2. assay enrichment by state class
3. top-k hit rate vs baselines
4. disagreement vs hit probability
5. 2–3 case-study loci with sequence and evidence panels

Required tables:
- top 100 ranked loci
- assay inventory
- scorer ablation summary
- failure-mode taxonomy

---

## 18. Agent roles

Use agents only where they save real time.

### Agent 1 — Manifest agent
- fetch sources
- compute checksums
- update source manifests
- fail on drift

### Agent 2 — Coordinate-resolution agent
- reconcile contig naming
- detect failed lifts
- route ambiguous loci to rescue alignment
- emit QC summaries

### Agent 3 — Supplement miner
- scrape supplementary tables from relevant papers
- extract assay intervals and labels
- normalize into `validation_link`

### Agent 4 — Scoring orchestrator
- run cheap scorers on all entities
- decide shortlist for expensive scorers
- enforce AlphaGenome budget

### Agent 5 — Leakage auditor
- verify holdout groups
- check for study contamination
- block benchmark runs if contamination is detected

### Agent 6 — Case-study compiler
- generate per-locus evidence packets
- collect nearby genes, known eQTLs, assay hits, and scorer outputs

---

## 19. CLI spec

```bash
panccre build-manifest
panccre ingest --source encode_ccre_v4
panccre project --context immune_hematopoietic
panccre call-states
panccre discover-candidates
panccre featurize
panccre score --scorer cheap_baseline
panccre shortlist --top 10000
panccre score --scorer alphagenome --from-shortlist
panccre train-ranker
panccre evaluate
panccre build-registry
panccre serve-api
```

Each command must:
- write a run manifest
- log input versions
- write outputs to a deterministic path
- exit nonzero on schema failure

---

## 20. Testing spec

### Unit tests
- parser correctness
- interval conversion
- state-class threshold logic
- feature calculations
- scorer adapters

### Integration tests
- one chromosome smoke build
- one assay source end-to-end
- registry assembly
- benchmark split integrity

### Golden tests
Freeze a small test fixture:
- 100 cCREs
- 3 haplotypes
- 1 assay study
- 1 scorer output file

All pipeline changes must preserve expected outputs or intentionally update the goldens with rationale.

---

## 21. Compute and infra

### Expected phase-1 footprint
- storage: 100–300 GB
- RAM: 64–128 GB helpful, less is workable with DuckDB chunking
- CPU: 16+ cores preferred
- GPU: optional except for local expensive scorer runs
- cloud OK but not required

### Cost-control rules
- expensive scorer budget enforced in config
- no raw-image or huge tensor retention unless needed
- sequence windows cached by hash
- all heavy joins pushed into DuckDB/Polars

---

## 22. Milestones

### Milestone 0 — Skeleton (week 1)
- repo bootstrapped
- manifests implemented
- one source ingested
- one test fixture committed

### Milestone 1 — Projection QC (weeks 2–3)
- projection on one chromosome
- state caller working
- QC plots

### Milestone 2 — Assay join and benchmark frame (weeks 4–5)
- MPRA + CRISPRi labels joined
- holdout groups frozen
- cheap baseline working

### Milestone 3 — Scorer fanout + shortlist (weeks 6–7)
- open-model scorer integrated
- AlphaGenome shortlist scoring integrated
- disagreement features computed

### Milestone 4 — Registry alpha (weeks 8–9)
- ranking and evaluation complete
- first case-study loci
- registry files produced

### Milestone 5 — Paper-ready freeze (weeks 10–12)
- frozen splits
- reproducible build script
- final figures and top-hit tables

---

## 23. Coding-agent implementation tickets

### Epic A — Data plumbing
- A1: manifest schema and validator
- A2: raw download manager
- A3: source parser registry
- A4: parquet writer with provenance columns

### Epic B — Projection
- B1: reference interval loader
- B2: haplotype alignment adapter
- B3: flank rescue aligner
- B4: projection QC report

### Epic C — State calling and candidate discovery
- C1: state threshold config
- C2: state caller
- C3: local window extractor
- C4: repeat/TE annotator
- C5: candidate generator

### Epic D — Features and scorers
- D1: cheap feature extractor
- D2: open-model scorer adapter
- D3: AlphaGenome adapter
- D4: scorer result normalizer
- D5: disagreement feature module

### Epic E — Ranking and evaluation
- E1: matched-negative builder
- E2: holdout generator
- E3: baseline models
- E4: ranker
- E5: evaluation report

### Epic F — Registry and API
- F1: registry builder
- F2: API
- F3: download bundle generator
- F4: case-study report builder

Each ticket must include:
- input contract
- output contract
- unit tests
- acceptance criterion
- expected runtime on test fixture

---

## 24. Non-goals and traps

### Non-goals
- “Solve gene regulation”
- detect all regulatory elements in the human pangenome
- prove pathogenicity
- build a giant sequence model

### Traps
- spending weeks on graph formats before defining the registry schema
- evaluating on random row splits
- calling any inserted interval a regulatory element
- letting expensive scorers define the product
- skipping provenance because “we’ll add it later”

---

## 25. Exact handoff to coding agents

Start here:

1. Implement the manifest system and raw source download manager.
2. Implement `ccre_ref` parsing and write the first Parquet table.
3. Implement a chromosome-20 test fixture with three haplotypes.
4. Implement the projection layer and emit `hap_projection`.
5. Freeze the state-calling thresholds in config and emit `ccre_state`.
6. Join one assay source and build `validation_link`.
7. Build the cheap feature matrix and cheap baseline ranker.
8. Only after steps 1–7 work, integrate AlphaGenome scoring on a shortlist.

The first code milestone is not a model. It is a **correct, queryable, reproducible registry build on a small fixture**.
