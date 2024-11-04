This repository includes a segment of a project I co-led at New AutoMotive that focused on tracking global Electric Vehicle (EV) sales.

* `swissModules.py` and `swissECC.py`: These Python scripts scraped monthly new car registration data by fuel type for Switzerland. Similar web scrapers were developed for other countries.

* `finnishModules.py` and `finnishECC.py`: These Python scripts scraped monthly new car registration data by fuel type and manufacturer for Finland. Similar web scrapers were developed for other countries.

* `layout.py` and `app.py`: These Python scripts were used to create a Plotly Dash dashboard, available at https://global-ev-tracker-127009394760.europe-west2.run.app. 

* `Plot 1 (Switzerland).png`: A stacked line chart illustrating new vehicle registrations by fuel type over time (absolute sales) for Switzerland. This chart was part of the Plotly Dash dashboard.

* `Plot 1 (Finland).png`: A stacked line chart illustrating new vehicle registrations by fuel type over time (market share) for Finland. This chart was part of the Plotly Dash dashboard.

* `Plot 2 (Finland).png`: A line chart illustrating monthly Battery Electric Vehicle (BEV) sales from the top ten BEV manufacturers for Finland. This chart was part of the Plotly Dash dashboard.

* `API.py`: This Python script was used to create a Flask API to populate country-specific pages on the company website through SquareSpace’s custom JavaScript feature.
