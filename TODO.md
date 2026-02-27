# ANONIMIZE Enhancement TODO

## Phase 1: Research & Core Architecture [IN PROGRESS]
- [x] Implement email anonymizer (replace, hash, mask, domain_only)
- [x] Implement phone anonymizer (replace, hash, mask, last4)
- [x] Implement SSN anonymizer (replace, hash, mask, last4, invalid)
- [x] Implement credit card anonymizer (replace, hash, mask, last4, token)
- [ ] Build differential privacy noise injection
- [ ] Write architecture decision records
- [ ] Research k-anonymity, l-diversity, t-closeness patterns
- [ ] Design policy-based rules engine

## Phase 2: Database & File Support [IN PROGRESS]
- [x] Directory structure for connectors
- [x] Directory structure for formats
- [ ] Database connector abstraction
- [ ] PostgreSQL connector
- [ ] MySQL connector
- [ ] SQLite connector
- [ ] MongoDB connector
- [ ] Parquet file support
- [ ] Excel file support
- [ ] Avro file support
- [ ] XML file support

## Phase 3: Enterprise Features [PENDING]
- [ ] Policy-based rules engine
- [ ] Data lineage tracking
- [ ] Reversibility/encryption options
- [ ] CLI with progress bars
- [ ] Web dashboard
- [ ] Batch processing with multiprocessing

## Phase 4: Testing & Polish [PENDING]
- [ ] 90%+ test coverage
- [ ] Performance benchmarks
- [ ] Documentation site
- [ ] CI/CD pipeline
- [ ] GitHub release automation

---
Status: Phase 1 in progress (4/8 complete) | Last updated: 2026-02-28
