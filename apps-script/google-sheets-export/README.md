Make sure you cd into Bark-Worthy/Infra/

To rebuild:
    docker compose build sheets_exporter

To rerun
    docker compose run --rm sheets_exporter

TO run tests
    pytest -v