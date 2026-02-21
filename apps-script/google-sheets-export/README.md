## Versioning

This project follows MAJOR.MINOR.PATCH.

MAJOR
Breaking or incompatible changes.

MINOR
Behavior or feature changes affecting runtime, outputs, or logic.

PATCH
Internal changes only (bug fixes, tests, refactors, docs, infra).

## Vertion History 

### 1,2.0
- added new tables creator_seedings, order_freebies, and sku_pricing
- new safety check ensuring sheet headers contain internal_uuid and processed_at
- updated Google hit limit workaround

### 1.1.0
- deletion logging added
- schema drift handling
- new safety checks
- tests added

### 1.0.0
- functioning exporter script
- tests