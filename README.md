# Airfoil DB Maker

Build pipeline for a production-ready SQLite airfoil database based on public upstream sources and large-scale XFOIL processing.

This repository contains the generators. The distributable dataset is the SQLite database published in [Releases](https://github.com/Giuliodori/airfoil-db-maker/releases), not committed to the repository.

## What It Is For

Airfoil DB Maker exists to turn fragmented upstream airfoil data into a single database that is easier to consume in tools, CAD workflows, analysis pipelines, and selection interfaces.

The final artifact is a SQLite database with:
- thousands of airfoil profiles
- closed trailing edges for CAD-ready curves
- resampled/upscaled profile coordinates for consistent downstream use
- thousands of generated XFOIL simulations with stored aerodynamic values
- usage/application references linked to airfoils
- airfoil ratings to support quick filtering and ranking
- usage-summary derived filter scores (`autostable_score`, `high_lift_score`, `famous_score`)

The database is designed to be used as a ready-made asset. The Python modules in this repository exist to generate and refresh that database.

## Why It Is Useful

- One portable SQLite file instead of multiple scripts, web pages, and raw archives.
- Airfoil geometries are normalized, validated, and filtered before entering the final dataset.
- Curves are closed and resampled to a consistent format, which makes them more practical for CAD and geometry pipelines.
- Aerodynamic values are already computed for a large set of profiles, avoiding repeated local XFOIL runs in downstream tools.
- Usage/application data helps group profiles by real-world references.
- Ratings provide a fast first-pass way to sort and compare profiles by intended use.

## Get The Database

Download the latest SQLite artifact from [Releases](https://github.com/Giuliodori/airfoil-db-maker/releases).

Expected final output name:
- `airfoil.db`

If you want to regenerate it locally, this repository contains the full build pipeline.
The generated `output/airfoil.db` is a local build artifact and should stay out of git history.

## Repository Scope

Main build modules:
- `build_profiles_db.py`
- `build_usage_db.py`
- `build_polars_db.py`
- `build_ratings_db.py`
- `merge_airfoil_db.py`
- `main.py`

Pipeline summary:
1. import and normalize airfoil geometries
2. ingest airfoil usage/application references
3. run XFOIL and store polar data
4. compute ratings
5. merge everything into the final SQLite database

Detailed technical documentation:
- [Pipeline and build details](docs/PIPELINE.md)
- [Sources, attribution, and redistribution notes](docs/ATTRIBUTIONS.md)
- [Ratings and filter formulas](RATINGS_AND_FILTERS.md)

## Related Project

This database is used by [manta-airlab](https://github.com/Giuliodori/manta-airlab), an airfoil design and generation toolchain.

## Sponsor

This project was made possible by [duilio.cc](https://duilio.cc).

## License

The code in this repository is licensed under `GPL-3.0-or-later`. See [LICENSE](LICENSE).

The generated database, downloaded upstream data, and external tools may be subject to additional terms from their original sources. See [docs/ATTRIBUTIONS.md](docs/ATTRIBUTIONS.md).

## Safety Notice

This project is provided for research, software development, education, and preliminary engineering exploration.

It is not certified aeronautical data, not a flight-safety product, and not a substitute for qualified aerodynamic design, structural analysis, wind-tunnel testing, flight testing, or regulatory review.

If you use this repository or the generated database for aircraft, UAV, model aircraft, propeller, or other vehicle design, you do so at your own risk. The authors and contributors provide the project `as is`, without warranties of accuracy, fitness for a particular purpose, or flight suitability.
