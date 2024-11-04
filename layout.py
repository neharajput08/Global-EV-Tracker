from dash import dcc, html
import dash_bootstrap_components as dbc
from modules import AVAILABLE_COUNTRY_LIST, DATASET_DICT, CONFIG

layout = dbc.Container(
    [
        dcc.ConfirmDialog(
            id = 'no-data-popup',
            message = 'No Make data for the selected country',
        ),
        dcc.Download(
            id = "download-csv"
        ),
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.Div(
                            [
                                html.Label(
                                    'Dataset:',
                                    className = 'dropdown-label'
                                ),
                                dcc.Dropdown(
                                    id = 'dataset-dropdown',
                                    options = [{'label' : label, 'value' : idx} for idx, label in DATASET_DICT.items()],
                                    value = 1,
                                    className = 'rounded-dropdown'
                                )
                            ],
                            className = 'div-control'
                        ),
                        html.Br(),
                        html.Div(
                            [
                                html.Label(
                                    'Country Selector:', 
                                    className = 'country-dropdown-label'
                                ),
                                html.Div(
                                    [
                                        dbc.Switch(
                                            id = {
                                                'type' : 'dynamic-switch',
                                                'index' : f'{country} - dyn'
                                            },
                                            label = (
                                                "New Zealand" if country.upper() == "NZ" else
                                                "Hong Kong" if country.upper() == "HK" else
                                                country.upper() if country.upper() in ["US", "UK"] else
                                                country.title()
                                            ),
                                            value = True if country.lower() == 'uk' else False,
                                            className = 'switch'
                                        )
                                        for country in AVAILABLE_COUNTRY_LIST
                                    ],
                                    id = 'div-countries',
                                    style = {
                                        'overflowY' : 'scroll',
                                        'height' : '200px'
                                    },
                                    className = 'country-switch-container'
                                ),
                            ],
                            className = 'country-selector-box'
                        ),
                        html.Br(),
                        html.Div(
                            [
                                html.Label(
                                    'Rolling Window:',
                                    className = 'dropdown-label'
                                ),
                                dcc.Dropdown(
                                    id = 'rolling-window-dropdown',
                                    options = [{'label' : str(i), 'value' : i} for i in range(1, 13)],
                                    value = 3,
                                    className = 'rounded-dropdown'
                                ),
                                html.Br(),
                                html.Label(
                                    'Market Share:',
                                    className = 'dropdown-label'
                                ),
                                dbc.Switch(
                                    id = {
                                        'type' : 'dynamic-abs-per-radio',
                                        'index' : f'type - plot - dyn'
                                    },
                                    label = None,
                                    value = True,
                                    className = 'switch'
                                )
                            ],
                            className = 'div-control',
                            id = 'lateral-div-control'
                        ),
                    ],
                    width = 3
                ),
                dbc.Col(
                    [
                        dcc.Graph(
                            id = 'graph',
                            className = 'chart-box',
                            config = CONFIG
                        )
                    ]
                )
            ],
            style = {
                "position" : "relative", 
                "width" : "100%",
                "height" : "56.25%"
            }
        )
    ],
    fluid = False,
    className = 'main-container'
)
