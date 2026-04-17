# Ratings And Filters Specification

This document describes how rating and filter variables are computed in the DB pipeline.

## Score range and normalization

All final rating categories are stored in `airfoil_ratings` on a `0..100` scale.

The pipeline uses two percentile normalizations:

1. Metric normalization:
   - For each raw metric `m`, compute dataset percentiles `p5(m)` and `p95(m)`.
   - `norm(m) = clamp(100 * (m - p5) / (p95 - p5), 0, 100)`.

2. Category normalization:
   - Compute weighted raw category scores (sum of `weight * norm(metric)`).
   - Normalize each category raw score again with category-level `p5/p95` to get final stored score.

Implementation: `build_ratings_db.py` (`normalize_to_score`, `compute_category_score`, `build_normalizers`, `build_category_normalizers`).

## Ratings (`airfoil_ratings`)

### Performance score

Weights:
- `best_ld`: `0.40`
- `best_cl`: `0.25`
- `usable_alpha_span`: `0.35`

Raw metrics:
- `best_ld = max(CL/CD)` over converged polar points with `CL > 0` and `CD > 0`
- `best_cl = max(CL)` over converged polar points
- `usable_alpha_span = max(alpha_deg) - min(alpha_deg)` over converged polar points

### Docility score

Weights:
- `coverage_ratio`: `0.30`
- `cl_smoothness`: `0.20`
- `cd_smoothness`: `0.15`
- `cm_stability`: `0.15`
- `camber_moderation`: `0.10`
- `thickness_moderation`: `0.10`

Raw metrics:
- `coverage_ratio = total_converged / total_expected`
- `cl_smoothness = -mean(abs(second_difference(CL)))` per Reynolds then averaged
- `cd_smoothness = -mean(abs(second_difference(CD)))` per Reynolds then averaged
- `cm_stability = -mean(max(CM)-min(CM))` per Reynolds
- `camber_moderation = -abs(max_camber)`
- `thickness_moderation = -abs(max_thickness - 0.12)`

### Robustness score (geometry proxy)

Weights:
- `thickness_mean_ratio`: `0.25`
- `spar_thickness_ratio`: `0.35`
- `bending_stiffness_proxy`: `0.25`
- `thickness_x_moderation`: `0.15`

Raw metrics:
- `thickness_mean_ratio`: mean thickness along normalized chord samples.
- `spar_thickness_ratio`: mean thickness in spar zone (`x in [0.25, 0.35]`, fallback `[0.20, 0.40]`).
- `bending_stiffness_proxy`: mean of `thickness(x)^3` over samples.
- `thickness_x_moderation = -abs(max_thickness_x - 0.30)`.

Notes:
- Thickness distribution is reconstructed from upper/lower surfaces (`x_json`, `y_json`) with linear interpolation and sampling.
- If geometry reconstruction fails, fallback estimates from `max_thickness` are used.

### Confidence score (aero data quality)

Weights:
- `coverage_ratio`: `0.30`
- `valid_reynolds_ratio`: `0.25`
- `converged_points`: `0.20`
- `usable_alpha_span`: `0.10`
- `reynolds_consistency`: `0.15`

Raw metrics:
- `coverage_ratio = total_converged / total_expected`
- `valid_reynolds_ratio = reynolds_with_converged_points / total_reynolds_runs`
- `converged_points = total_converged`
- `usable_alpha_span = max(alpha_deg) - min(alpha_deg)` over converged points
- `reynolds_consistency = -pstdev(coverage_ratio_per_reynolds)`

### Versatility score

Stored in `airfoil_ratings.versatility_score` (`0..100` after category normalization).

Raw formula (before normalization):

`0.35*log1p(usage_rows)`
`+ 0.25*log1p(distinct_aircraft)`
`+ 0.15*log1p(distinct_roles)`
`+ 0.10*log1p(distinct_sections)`
`+ 0.10*log1p(distinct_reasons)`
`+ 0.05*log1p(distinct_profile_types)`

Usage stats source: `airfoil_applications`.

## Usage summary and derived filter scores (`airfoil_usage_summary`)

`merge_airfoil_db.py` rebuilds `airfoil_usage_summary` every run.

Main fields:
- `usage_count`
- `top_usage`
- `top_aircraft`
- `top_usages`
- `top_sources`
- `famous_score`
- `high_lift_score`
- `autostable_score`
- `autostable_cm0_est`
- `autostable_slope_est`
- `autostable_re_triplets`

High-lift score is derived from `max(CL)` over converged polar points:

- `best_cl(airfoil) = max(CL)` for that profile.
- Let `min_best_cl` and `max_best_cl` be dataset bounds over all profiles.
- Formula:
  - `high_lift_score = 100 * (best_cl - min_best_cl) / (max_best_cl - min_best_cl)`
  - clamped by SQL boundary handling and rounded to 3 decimals.
  - if dataset span is degenerate, score is `0`.

Famous score is derived from usage frequency:

- Let `usage_count(airfoil)` be total matched usage rows for that profile.
- Let `min_usage_count` and `max_usage_count` be dataset bounds over all profiles.
- Formula:
  - `famous_score = 100 * (usage_count - min_usage_count) / (max_usage_count - min_usage_count)`
  - rounded to 3 decimals.
  - if dataset span is degenerate, score is `0`.

Autostable metrics are derived from converged polar data at `alpha = {0, 2, 4}`:

- Fit `CM(alpha)` with linear regression:
  - slope `dcm_dalpha`
  - intercept `cm0_est`
- Count Reynolds triplets with all three alpha points converged: `re_triplet_count`.

Autostable score formula:

`autostable_score = 100 * (`
`  0.65 * clamp((-dcm_dalpha)/0.004, -1, 1)`
`+ 0.25 * clamp(1 - abs(cm0_est)/0.030, -1, 1)`
`+ 0.10 * clamp((re_triplet_count/3) - 1, -1, 1)`
`)`

(stored rounded to 3 decimals).

## Filter presets (`airfoil_filter_presets`)

DB table fields:
- `label`
- `profile_type_filter`
- `usage_filter`
- `display_order`
- `enabled`
- `note`

Default presets inserted by merge:
- `All`
- `Symmetric`
- `Autostable`
- `Rotating`
- `High Lift`
- `Famous`

Runtime behavior (GUI + DB query):
- Presets are loaded from `airfoil_filter_presets` (no hardcoded logic required for list/order).
- `usage_filter` applies textual search on application role/section/aircraft fields.
- For `profile_type_filter = autostable`, filtering uses:
  - `COALESCE(airfoil_usage_summary.autostable_score, -1000) >= autostable_min_score`
  - GUI default threshold is `20`.
- For `profile_type_filter = high_lift`, filtering uses:
  - `COALESCE(airfoil_usage_summary.high_lift_score, -1000) >= high_lift_min_score`
- For `profile_type_filter = famous`, filtering uses:
  - `COALESCE(airfoil_usage_summary.famous_score, -1000) >= famous_min_score`
- Other profile-type tokens are matched on `profile_type_tag`, `reason_tag`, and text fallback fields.

## Related tables

- `airfoil_ratings`: final category scores
- `airfoil_rating_details`: per-metric contributions
- `airfoil_rating_reynolds`: per-Reynolds diagnostic metrics
- `airfoil_usage_summary`: compact usage summary + derived filter scores
- `airfoil_filter_presets`: DB-driven preset definitions
