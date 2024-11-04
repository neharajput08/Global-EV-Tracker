from dash import callback_context, dcc, no_update, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State, ALL
import pandas as pd
from pandas import DataFrame
from dash_extensions.enrich import DashProxy, MultiplexerTransform
import plotly.graph_objects as go
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
from dotenv import load_dotenv
from flask_caching import Cache
from modules import COUNTRY_WITH_MAKES, AVAILABLE_COUNTRY_LIST, OPTIONS_MAKE, NO_PLOT
from modules import SCurve_plot, area_plot, mkt_share_plot, top_makes_plot, generate_plotly_layout, generate_colors
from layout import layout
from modules.connector import MySQL

load_dotenv()

sql = MySQL(db = 'explorer', 
            GCR = os.getenv("ENV") == 'GCR', 
            credentials_files = './credentials/explorer_credentials.json')

app = DashProxy(prevent_initial_callbacks = False, 
                transforms = [MultiplexerTransform()],
                external_stylesheets = [dbc.themes.LUX])

cache = Cache(app.server, 
              config = {'CACHE_TYPE' : 'simple'})

@cache.memoize(timeout = 3600)
def SCurve_query(country : str) -> DataFrame:
    if country != 'world':
        query = f"""
                WITH total_table AS (
                    SELECT 
                        date, 
                        SUM(registrations) AS Total 
                    FROM 
                        `{country}`
                    WHERE 
                        date >= '2018-01-01'
                    GROUP BY 
                        date
                ),
                BEV_table AS (
                    SELECT 
                        date, 
                        SUM(registrations) AS BEV 
                    FROM 
                        `{country}`
                    WHERE 
                        fuelType = 'BEV' 
                        AND date >= '2018-01-01'	
                    GROUP BY 
                        date
                )
                SELECT 
                    total_table.date,
                    BEV_table.BEV, 
                    total_table.Total 
                FROM 
                    total_table
                INNER JOIN
                    BEV_table 
                ON 
                    BEV_table.date = total_table.date
                ORDER BY 
                    date ASC;
                """
        df_total = sql.from_sql_to_pandas(query).set_index('date').sort_index()
    else:
        query = f"""
                SELECT 
                    * 
                FROM 
                    `world_scurve`
                """        
        df_total = sql.from_sql_to_pandas(query).set_index('date').sort_index()
    return df_total

@cache.memoize(timeout = 3600)
def get_national_mkt_share(country : str,
                           fuel_type : str,
                           make_mapping : str) -> DataFrame:
    query_share =   f"""
                    WITH totalSales AS (
                        SELECT 
                            date, 
                            SUM(registrations) AS total
                        FROM 
                            explorer.{country}
                        WHERE 
                            fuelType = "{fuel_type}"
                        GROUP BY 
                            date
                    )
                    SELECT 
                        A.date, 
                        SUM(A.registrations) AS partial, 
                        B.total AS total
                    FROM 
                        explorer.{country} AS A
                    JOIN 
                        totalSales AS B 
                    ON 
                        B.date = A.date
                    WHERE 
                        A.fuelType = "{fuel_type}"
                        AND A.make IN ({make_mapping})
                        AND A.date >= "2019-01-01"
                    GROUP BY
                        A.date, 
                        B.total
                    ORDER BY 
                        A.date ASC;
                    """
    df = sql.from_sql_to_pandas(query_share)
    return df

@cache.memoize(timeout = 3600)
def national_area_plot(country : str) -> DataFrame:
    return sql.from_sql_to_pandas(sql_query =   f"""
                                                SELECT
                                                    date,
                                                    fuelType,
                                                    SUM(registrations) AS registrations
                                                FROM 
                                                    {country}
                                                WHERE 
                                                    date >= '2018-01-01'
                                                GROUP BY 
                                                    date, 
                                                    fuelType
                                                """)

@cache.memoize(timeout = 3600)
def top_makes_query(country : str) -> DataFrame:
    return sql.from_sql_to_pandas(sql_query =   f"""
                                                WITH TopMakes AS (
                                                    SELECT 
                                                        make
                                                    FROM 
                                                        `{country}`
                                                    WHERE 
                                                        fuelType = 'BEV'
                                                    GROUP BY
                                                        make
                                                    ORDER BY
                                                        SUM(registrations) DESC
                                                    LIMIT 5
                                                )
                                                SELECT 
                                                    Country.*, 
                                                    'thick' AS width
                                                FROM 
                                                    `{country}` AS Country
                                                INNER JOIN 
                                                    TopMakes 
                                                ON 
                                                    TopMakes.make = Country.make
                                                WHERE 
                                                    fuelType = 'BEV'
                                                """)

@cache.memoize(timeout = 3600)
def top_makes_query2(country : str) -> DataFrame:
    return sql.from_sql_to_pandas(sql_query =   f"""
                                                WITH Looker AS (
                                                    SELECT
                                                        *, 
                                                        ROW_NUMBER() OVER (PARTITION BY date ORDER BY BEV_sales DESC) AS row_num
                                                    FROM 
                                                        `looker_national_top_makers`
                                                    WHERE 
                                                        country = '{country}'
                                                ),
                                                TopMakes AS (
                                                    SELECT 
                                                        DISTINCT make
                                                    FROM
                                                        `Looker`
                                                    WHERE
                                                        row_num <= 5
                                                )
                                                SELECT
                                                    Country.*,
                                                    'thin' AS width
                                                FROM
                                                    `{country}` AS Country
                                                INNER JOIN 
                                                    TopMakes
                                                ON 
                                                    TopMakes.make = Country.make
                                                WHERE
                                                    fuelType = 'BEV'
                                                """)

@app.callback(
    [
     Output('graph', 'figure'),
     Output('no-data-popup', 'displayed'),
     Output('no-data-popup', 'message')
    ],
    [
     State('dataset-dropdown', 'value'),
     Input(component_id = {'type' : 'dynamic-option', 'index' : ALL}, component_property = 'value'),
     Input('rolling-window-dropdown', 'value'),
     Input(component_id = {'type' : 'dynamic-backtesting', 'index' : ALL},component_property = 'value'),
     Input(component_id = {'type' : 'dynamic-abs-per-radio', 'index' : ALL},component_property = 'value'),
     Input(component_id = {'type' : 'dynamic-switch', 'index' : ALL}, component_property = 'value'),
    ]
)
def update_graph(type_dataset,
                 add_options,
                 rolling_window,
                 months_back_list,
                 abs_per_radio,
                 values):
    fig = go.Figure()
    message_popup = ''
    is_popup = False

    if type_dataset in [2, 4]:
        selected_countries = [country for country, value in zip(COUNTRY_WITH_MAKES, values) if value]
    else:
        selected_countries = [country for country, value in zip(AVAILABLE_COUNTRY_LIST, values) if value]

    if len(selected_countries) == 0:
        return NO_PLOT, no_update, no_update

    df_list = []

    if type_dataset == 1:
        display_per = abs_per_radio[0]
        country = selected_countries[0]
        df = national_area_plot(country = country.lower())
        fig,df = area_plot(df = df,
                           rolling_window = rolling_window,
                           display_per = display_per)
        condition = 'New Zealand' if country.upper() == 'NZ' else 'Hong Kong' if country.upper() == 'HK' else country.upper() if country.upper() in ['US', 'UK'] else country.title()
        title = f"Market Share by Fuel Type Over Time: {condition}" if display_per else f"New Registrations by Fuel Type Over Time: {condition}"
        max_value = 100 if display_per == True else df.groupby('date').sum()['registrations'].max() 
        fig.update_layout(generate_plotly_layout(title = title,
                                                 yaxis_title = "Market Share (%)" if display_per else "New Registrations",
                                                 per = display_per,
                                                 range = [0, max_value]))
        df_list.append(df)

    elif type_dataset == 2:
        missing_countries = []
        fuelType = add_options[0]
        make_mapping = add_options[1]
        for country in selected_countries:
            country = country.lower()
            df = get_national_mkt_share(country,
                                        fuelType,
                                        make_mapping)
            if df.shape[0] == 0:
                missing_countries.append(country)
            df.set_index('date',
                         inplace = True)
            if rolling_window:
                df = df.rolling(rolling_window).mean().dropna()
            df.reset_index(inplace = True)
            df['mktShare'] = (df['partial']/ df['total']) * 100
            fig = mkt_share_plot(fig,
                                 df,
                                 country)
            df = df[['date',
                     'mktShare']]
            df['country'] = country
            df_list.append(df)
        make = [option['label'] for option in OPTIONS_MAKE for key, value in option.items() if value == make_mapping][0]
        fig.update_layout(generate_plotly_layout(title = f"Market Share by Manufacturer Over Time",
                                                 yaxis_title = f"{fuelType} Market Share (%)"))

        selected_countries = [country.lower() for country in selected_countries]
        is_popup = True if len(missing_countries) > 0 else False

        missing_countries = ["New Zealand" if country.upper() == "NZ" else "Hong Kong" if country.upper() == "HK" else country.upper() if country.upper() in ["US", "UK"] else country.title() for country in missing_countries]
        message_popup = f"{make}, {fuelType} not sold in: {', '.join(missing_countries)}"

    if type_dataset == 3:
        selected_countries = [c.lower() for c in selected_countries]
        months_back_list = months_back_list[0]
        color_dict = generate_colors(selected_countries)
        for country in selected_countries:
            for months_back in months_back_list:
                df = SCurve_query(country = country)
                if months_back !=0:
                    df = df.iloc[:-months_back]
                    fig, df = SCurve_plot(fig,
                                          df,
                                          rolling_window = rolling_window,
                                          country = country,
                                          color_dict = color_dict,
                                          month_label = (datetime.today() - relativedelta(months = months_back + 1)).strftime("%b %Y"))
                else:
                    fig, df = SCurve_plot(fig,
                                          df,
                                          rolling_window = rolling_window,
                                          country = country,
                                          color_dict = color_dict,
                                          month_label = False)
                df_list.append(df)
        fig.add_trace(go.Scatter(x = [None],
                                 y = [None], 
                                 name = '<b>Data Points</b>',
                                 mode = 'markers', 
                                 marker = dict(color = 'white', 
                                               opacity = 1,  
                                               line = dict(color = 'black',
                                                           width = 3))))
        fig.add_trace(go.Scatter(x = [None],
                                 y = [None],
                                 name = '<b>Fit</b>',
                                 mode = 'lines',
                                 line = dict(color = 'black',
                                             width = 3)))
        fig.add_trace(go.Scatter(x = [None],
                                 y = [None],
                                 mode = 'lines',
                                 name = '<b>Projection</b>',
                                 line = dict(color = 'black',
                                             dash = 'dash',
                                             width = 3)))
        fig.update_layout(generate_plotly_layout(title = f"S-Curve Like Adoption",
                                                 yaxis_title = "BEV Market Share (%)"))
        selected_countries = [country.lower() for country in selected_countries]

    elif type_dataset == 4:
        country = selected_countries[0]
        df = top_makes_query(country = country.lower())
        df2 = top_makes_query2(country = country.lower())
        fig, df = top_makes_plot(df = df, 
                                 df2 = df2,
                                 rolling_window = rolling_window)
        max_registrations = max(df['registrations'].max(), 
                                df2['registrations'].max())
        fig.update_layout(generate_plotly_layout(title = f"Sales of Top BEV Manufacturers Over Time: {'New Zealand' if country.upper() == 'NZ' else 'Hong Kong' if country.upper() == 'HK' else country.upper() if country.upper() in ['US', 'UK'] else country.title()}",
                                                 yaxis_title = "BEV Sales",
                                                 per = False,
                                                 range = [0, max_registrations]))
        df_list.append(df)

    df_csv = pd.concat(df_list)

    return fig, is_popup, message_popup

@app.callback(
    Output('div-countries', 'children'),
    Output('lateral-div-control', 'children'),   
    Input('dataset-dropdown', 'value'),
    Input(component_id = {'type' : 'dynamic-switch', 'index' : ALL}, component_property = 'value'),
    State('lateral-div-control', 'children'),
    prevent_initial_call = True
)
def update_switch_output(type_dataset,
                         switch_values,
                         controller):
    controller = [ 
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
    ]

    triggered_id = callback_context.triggered[0]['prop_id'].split('.')[0]
    countries_selector = []
 
    if 'dataset-dropdown' == triggered_id:
        if type_dataset in [2, 4]: 
            list_countries = COUNTRY_WITH_MAKES
            if type_dataset == 2:
                controller += [
                        dbc.Col(
                            [
                                html.Label(
                                    'Fuel Type:',
                                    className = 'dropdown-label'
                                ),
                                dcc.Dropdown(
                                    id = {
                                        'type' : 'dynamic-option',
                                        'index' : 'fuel-dyn'
                                    },
                                    options = [
                                        {'label' : 'Petrol', 'value' : 'Petrol'},
                                        {'label' : 'Diesel', 'value' : 'Diesel'},
                                        {'label' : 'BEV', 'value' : 'BEV'},
                                        #{'label' : 'HEV/ BEV', 'value' : 'HEV/ BEV'},
                                        {'label' : 'HEV', 'value' : 'HEV'},
                                        {'label' : 'PHEV', 'value' : 'PHEV'},
                                        {'label' : 'FHEV', 'value' : 'FHEV'},
                                        #{'label' : 'Other', 'value' : 'Other'}
                                        {'label' : 'ICE', 'value' : 'ICE'}
                                    ],
                                    value = 'BEV',
                                    className = 'rounded-dropdown',
                                )
                            ]
                        ),
                        dbc.Col(
                            [
                                html.Label(
                                    'Make:',
                                    className = 'dropdown-label'),
                                dcc.Dropdown(
                                    id = {
                                        'type' : 'dynamic-option',
                                        'index' : 'make-dyn'
                                    },
                                    options = OPTIONS_MAKE,
                                    value = "'TESLA','Tesla Motors','Tesla'",
                                    className = 'rounded-dropdown',
                                )
                            ]
                        )
                ]
            else:
                controller         
        else:
            list_countries = AVAILABLE_COUNTRY_LIST
            if type_dataset == 3: 
                controller += [
                    dbc.Col(
                        [
                            html.Label(
                                'Backtesting Calendar',
                                className = 'dropdown-label'
                            ),
                            dcc.Dropdown(
                                id = {
                                    'type' : 'dynamic-backtesting',
                                    'index' : 'backtesting-dyn'
                                },
                                options= [
                                    {'label' : (datetime.today() - relativedelta(months = i + 1)).strftime("%b %Y"), 'value' : i} for i in range(0, 37)
                                ],
                                multi = True,
                                value = [0],
                                className = 'rounded-dropdown'
                            )
                        ]
                    )
                ]     
            elif type_dataset == 1:
                controller += [ 
                    html.Br(),
                    html.Label(
                        'Market Share', 
                        className = 'dropdown-label'
                    ),
                    dbc.Switch(
                        id = {
                            'type' : 'dynamic-abs-per-radio',
                            'index' : f'type-plot-dyn'
                        },
                        label = None,
                        value = True,
                        className = 'switch'
                    )
                ]

        for country in list_countries:
            countries_selector.append(
                dbc.Switch(
                    id = {
                        'type' : 'dynamic-switch',
                        'index' : f'{country}-dyn'
                    },
                    label = "New Zealand" if country.upper() == "NZ" else "Hong Kong" if country.upper() == "HK" else country.upper() if country.upper() in ["US", "UK"] else country.title(),
                    value = False,
                    className = 'switch'
                )
            )

    elif type_dataset in [1, 4]:
        list_countries = AVAILABLE_COUNTRY_LIST if type_dataset == 1 else COUNTRY_WITH_MAKES
        for country, value in zip(list_countries, switch_values):
            activation = value if f'{country}-dyn' in triggered_id else False
            countries_selector.append(
                dbc.Switch(
                    id = {
                        'type' : 'dynamic-switch',
                        'index' : f'{country}-dyn'
                    },
                    label = "New Zealand" if country.upper() == "NZ" else "Hong Kong" if country.upper() == "HK" else country.upper() if country.upper() in ["US", "UK"] else country.title(),
                    value = activation,
                    className = 'switch'
                )                 
            )
        controller = no_update

    else:
        countries_selector = no_update
        controller = no_update 
        
    return countries_selector, controller

if __name__ == '__main__':
    app.layout = layout
    app.run_server(debug = True)