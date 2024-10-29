from flask import Flask, request, jsonify
from modules.mysql import MySQL
import os
from dotenv import load_dotenv
from flask_cors import CORS

load_dotenv()
env = os.getenv("ENV_API")
GCR = True if env is not None else False

sql = MySQL(db = "explorer",
            credentials_file = "./credentials/explorer_credentials.json",
            verbose = False,
            GCR = GCR)

COUNTRY_LIST = ['austria',
                'belgium',
                'bulgaria',
                'china',
                'croatia',
                'cyprus',
                'czechia',
                'denmark',
                'estonia',
                'finland',
                'france',
                'germany',
                'greece',
                'hungary',
                'india',
                'ireland',
                'italy',
                'japan',
                'latvia',
                'lithuania',
                'luxembourg',
                'malta',
                'netherlands',
                'norway',
                'poland',
                'portugal',
                'romania',
                'slovakia',
                'slovenia',
                'sweden',
                'uk',
                'mexico',
                'spain',
                'brazil',
                'thailand',
                'canada',
                'us',
                'switzerland',
                'iceland',
                'australia',
                'singapore',
                'turkey',
                'nz',
                'hk',
                'chile',
                'taiwan']

MAKE_COUNTRY_LIST = ['uk',
                     'italy',
                     'china',
                     'netherlands',
                     'japan',
                     'czechia',
                     'spain',
                     'portugal',
                     'india',
                     'germany',
                     'sweden',
                     'finland',
                     'singapore',
                     'nz',
                     'hk']

app = Flask(__name__)
CORS(app)

@app.route("/historicals", 
           methods = ["GET"])
def historicals_endpoint():
    country = request.args.get("country")

    if country not in COUNTRY_LIST:
        return jsonify({"error": "data not available for the specified country"}), 400

    query = f"""
            SELECT 
                date,
                fuelType,
                SUM(registrations) AS registrations
            FROM 
                `{country}`
            WHERE 
                date >= '2018-01-01'
            GROUP BY 
                date, 
                fuelType
            """
    df = sql.read_df(query)
    df = df.pivot_table(index = "date", 
                        columns = "fuelType", 
                        values = "registrations")
    df = df.reset_index()
    df = df.fillna(0)
    result = {}
    for column in df.columns:
        result[column] = df[column].to_list()
    result["lastUpdate"] = df["date"].max()
    return jsonify(result), 200

@app.route("/top_makers", 
           methods = ["GET"])
def top_makers_endpoint():
    country = request.args.get("country")

    if country not in MAKE_COUNTRY_LIST:
        return jsonify({"error": "make data not available for the specified country"}), 400
    
    query = f"""
            SELECT 
                make,
                BEV_sales,
                BEV_percentage
            FROM 
                `looker_national_top_makers`
            WHERE 
                country = '{country}'
                AND date = (SELECT MAX(date) FROM `looker_national_top_makers` WHERE country = '{country}')
            """
    df = sql.read_df(query)
    df = df.fillna(0)
    result = {}
    for column in df.columns:
        result[column] = df[column].to_list()
    return jsonify(result), 200

@app.route("/table", 
           methods = ["GET"])
def table_endpoint():
    country = request.args.get("country")

    if country not in COUNTRY_LIST:
        return jsonify({"error": "data not available for the specified country"}), 400

    query = f"""
            WITH current_period AS (
                SELECT 
                    DISTINCT date
                FROM 
                    `{country}`
                ORDER BY 
                    date DESC
                LIMIT 12
            ),
            previous_period AS (
                SELECT
                    DISTINCT date
                FROM
                    `{country}`
                ORDER BY 
                    date DESC
                LIMIT 12 OFFSET 12
            ),
            current_period_agg AS (
                SELECT 
                    fuelType,
                    SUM(registrations) AS current_period_reg
                FROM 
                    `{country}`
                WHERE 
                    date IN (SELECT date FROM current_period)
                GROUP BY 
                    fuelType
            ),
            previous_period_agg AS (
                SELECT 
                    fuelType,
                    SUM(registrations) AS previous_period_reg
                FROM 
                    `{country}`
                WHERE 
                    date IN (SELECT date FROM previous_period)
                GROUP BY 
                    fuelType
            )
            SELECT 
                current_period_agg.fuelType,
                current_period_agg.current_period_reg AS total,
                ((current_period_agg.current_period_reg - previous_period_agg.previous_period_reg) / previous_period_agg.previous_period_reg) * 100 AS perc_change,
                (current_period_agg.current_period_reg / (SELECT SUM(current_period_reg) FROM `current_period_agg`)) * 100 AS share
            FROM 
                `current_period_agg`
            INNER JOIN
                `previous_period_agg`
            ON
                current_period_agg.fuelType = previous_period_agg.fuelType
            ORDER BY 
                total DESC
            """
    df = sql.read_df(query)
    df = df.fillna(0)
    result = {}
    for column in df.columns:
        result[column] = df[column].to_list()
    return jsonify(result), 200

@app.route("/table2", 
           methods = ["GET"])
def table2_endpoint():
    country = request.args.get("country")

    if country not in COUNTRY_LIST:
        return jsonify({"error": "data not available for the specified country"}), 400

    query = f"""
            WITH current_month_agg AS (
                SELECT 
                    fuelType,
                    SUM(registrations) AS current_month_reg
                FROM 
                    `{country}`
                WHERE 
                    date = (SELECT MAX(date) FROM `{country}`)
                GROUP BY 
                    fuelType
            ),
            previous_month_agg AS (
                SELECT 
                    fuelType,
                    SUM(registrations) AS previous_month_reg
                FROM 
                    `{country}`
                WHERE 
                    date = DATE_SUB((SELECT MAX(date) FROM `{country}`), INTERVAL 1 YEAR)
                GROUP BY 
                    fuelType
            )
            SELECT 
                current_month_agg.fuelType,
                current_month_agg.current_month_reg AS total,
                ((current_month_agg.current_month_reg - previous_month_agg.previous_month_reg) / previous_month_agg.previous_month_reg) * 100 AS perc_change,
                (current_month_agg.current_month_reg / (SELECT SUM(current_month_reg) FROM `current_month_agg`)) * 100 AS share
            FROM 
                `current_month_agg`
            INNER JOIN
                `previous_month_agg`
            ON
                current_month_agg.fuelType = previous_month_agg.fuelType
            ORDER BY 
                total DESC
            """
    df = sql.read_df(query)
    df = df.fillna(0)
    result = {}
    for column in df.columns:
        result[column] = df[column].to_list()
    return jsonify(result), 200

@app.route("/top_makers2", 
           methods = ["GET"])
def top_makers2_endpoint():
    country = request.args.get("country")

    if country not in MAKE_COUNTRY_LIST:
        return jsonify({"error": "make data not available for the specified country"}), 400
    
    query = f"""
            WITH top_five_makes AS (
                SELECT 
                    make 
                FROM 
                    `{country}` 
                WHERE 
                    date = (SELECT MAX(date) FROM `{country}`) 
                GROUP BY 
                    make 
                ORDER BY 
                    SUM(registrations) DESC 
                LIMIT 5
            ),
            table1 AS (
                SELECT 
                    make,
                    fuelType,
                    SUM(registrations) AS registrations
                FROM 
                    `{country}`
                WHERE 
                    date = (SELECT MAX(date) FROM `{country}`) 
                    AND make IN (SELECT make FROM `top_five_makes`)
                GROUP BY 
                    make, 
                    fuelType
            ),
            table2 AS (
                SELECT
                    make,
                    SUM(registrations) AS total_registrations
                FROM
                    `table1`
                GROUP BY 
                    make
            )
            SELECT
                table1.make,
                fuelType,
                ROUND((table1.registrations / table2.total_registrations) * 100, 1) AS market_share
            FROM
                `table1`
            LEFT JOIN
                `table2`
            ON 
                table1.make = table2.make
            """
    df = sql.read_df(query)
    df = df.fillna(0)
    result = {}
    for column in df.columns:
        result[column] = df[column].to_list()
    return jsonify(result), 200

if not GCR:
    app.run(debug = True)
