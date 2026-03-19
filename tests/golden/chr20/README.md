# Chromosome 20 Fixture

This fixture is the phase-1 smoke dataset for early pipeline development.

- `encode_ccre_chr20_fixture.bed`: 100 synthetic chr20 cCRE-like rows.
- `haplotypes_chr20_fixture.tsv`: three haplotypes for upcoming projection tests.
- `hap_projection_variants_fixture.vcf`: synthetic VCF with 3 samples for `project-vcf` tests.

The cCRE fixture keeps a BED-like contract used by `panccre.ingest.parse_ccre_bed`:
`chr`, `start`, `end`, `ccre_id`, `score`, `strand`, `ccre_class`, `biosample_count`.
