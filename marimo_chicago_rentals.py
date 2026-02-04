# td_bixi_marimo.py
# ============================================================
# TD Marimo — Analyse des données Bixi avec DuckDB
# M1 MIAGE UT Capitole — Antoine Giraud
# ============================================================

import marimo

__generated_with = "0.19.6"
app = marimo.App(app_title="1ère explo DuckDB")


@app.cell(hide_code=True)
def intro(mo):
    mo.md(r"""
    # TD — Analyse des stations **divvy** 🚲 @Chicago

    Ce TD vous guide dans l’exploration de données réelles :
    - Locations journalières (.csv -> .parquet)

    Vous utiliserez **DuckDB** et son extension **spatial**.

    👉 Certaines cellules contiennent des `TODO` à compléter. 🧪
    """)
    return


@app.cell
def imports():
    import marimo as mo
    import duckdb

    # Create a DuckDB connection
    conn = duckdb.connect("explo_chicago.db")
    # on va travailler avec des coordonnées
    conn.sql("INSTALL spatial; LOAD spatial;")
    return conn, mo


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1. Téléchargement des "rentals" d'une année

    Les opérateurs de **vélo en libre** (bikeshare) d'Amérique du Nord mettent à disposition fréquement :
    - le **GBFS** : offre de service temps réel des stations (.json)
    - l'**historique** des **locations de vélo** ou "rentals" (.csv.zip)

    Nous vous invitons pour ce TP de choisir par groupe de 2 une année de rentals pour la ville de Chicago. Leur marque de bikeshare s'appelle divvy (à l'instar de bixi pour Montréal)

    Exemple de recherche sur google : [chicago bike rentals opendata](https://www.google.com/search?q=chicago+bike+rentals+opendata) fait bien remonter en 1er résultat la page [divvybikes.com/system-data](https://divvybikes.com/system-data)
    ![screen_google_search_divvy_data](public/screen_google_search_divvy_data.png)

    RDV là bas pour y télécharger les fichiers de votre année :)

    Nous vous invitons
    - à les ranger dans le dossier `data/rentals_divvy/annee=yyyy/`
    - à les dézipper vous même, DuckDB ne sait pas lire les .csv.zip !

    Dans un premier temps, vous pouvez vérifier si vos sources de données sont bien présentes dans le dossier :
    """)
    return


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        SELECT
            filename,
            (size/1024/1024)::int AS size_mb,
        FROM read_text('data/**')
        """,
        engine=conn
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 2. Découverte du schéma des données

    Dans un premier temps, vérifiez que vous voyez l'ensemble de vos données issues de vos sources :
    """)
    return


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        -- regardons le contenu d'un fichiers
        from 'data/annee=2025/202501-divvy-tripdata.csv'
        """,
        engine=conn
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    Suite à la première analyse de lensemble des données, quelles sont les colonnes qui peuvent être exploitées en tant quaxe d'analyse ?
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 3. Retravaillons les données

    Vous devez avoir 12 fichiers au format .csv

    Vous allez créer la première table avec l'ensemble des données de lannée que vous avec choisi.
    """)
    return


@app.cell
def _(conn, mo):
    fact_rentals = mo.sql(
        f"""
        -- préparation fact_rentals
        create or replace table fact_rentals as
        select
            annee,
            DATE_TRUNC('month', started_at) as dt_mois,
            start_station_id,
            end_station_id,
            member_casual,
            DATE_TRUNC('minute', started_at) as started_at,
            DATE_DIFF('second', started_at, ended_at) duration,
        from 'data/annee=2025/*.csv';

        -- affichons les données
        from fact_rentals;
        """,
        engine=conn
    )
    return (fact_rentals,)


@app.cell
def _(mo):
    mo.md(r"""
    Utilisez la fonction Summarize pour évaluez le contenu de chaque colonnes :
    """)
    return


@app.cell
def _(conn, fact_rentals, mo):
    _df = mo.sql(
        f"""
        summarize (from fact_rentals where dt_mois = '2025-01-01')
        """,
        engine=conn
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    Suite à l'analyse des colonnes, vous pouvez créer la première table de dimension :
    """)
    return


@app.cell
def _(conn, mo):
    dim_station = mo.sql(
        f"""
        -- préparation dim_station
        create or replace table dim_station as
        select
            annee,
            start_station_id as station_id,
            any_value(start_station_name) as nom,
            count(distinct start_station_name) as nb_uq_nom,
            count(1) nb_rentals,
            count(distinct CONCAT_WS('||', start_lng, start_lat)) nb_uq_coords,
            any_value(ST_Point(
                (start_lng)::DOUBLE,
                (start_lat)::DOUBLE
            )) AS station_geom,
            ST_AsGeoJSON(station_geom) AS geom_json,
        from 'data/annee=2025/*.csv'
        group by all;

        -- affichons les données
        from dim_station;
        """,
        engine=conn
    )
    return (dim_station,)


@app.cell
def _(mo):
    mo.md(r"""
    Vous faites ensuite une copie de ces tables dans un fichier au format .parquet
    """)
    return


@app.cell
def _(conn, dim_station, fact_rentals, mo):
    _df = mo.sql(
        f"""
        copy dim_station to 'data/dim_station.parquet';
        copy fact_rentals to 'data/fact_rentals.parquet';
        """,
        engine=conn
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    Dans un second temps, vous faites de même avec les fichiers bruts
    """)
    return


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        copy (
            from 'data/annee=2025/*.csv'
        ) to 'data/raw_rentals_2025.parquet';
        """,
        engine=conn
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    Que constatez-vous sur les temps de traitement et la taille des fichiers ?
    """)
    return


@app.cell
def _(conn, fact_rentals, mo):
    _df = mo.sql(
        f"""
        copy (
            select
                annee,
                DATE_TRUNC('month', started_at) as dt_mois,
                start_station_id,
                end_station_id,
                member_casual,
                DATE_TRUNC('hour', started_at) as started_at_hour,
                count(1) as nb_trips,
                sum(duration) as duration,
            from fact_rentals
            group by all
        ) to 'data/fact_rentals_hourly_grain.parquet'
        """,
        engine=conn
    )
    return


@app.cell
def _(conn, fact_rentals, mo):
    _df = mo.sql(
        f"""
        copy (
            select
                annee,
                DATE_TRUNC('month', started_at) as dt_mois,
                start_station_id,
                end_station_id,
                member_casual,
                DATE_TRUNC('day', started_at) as started_date,
                count(1) as nb_trips,
                sum(duration) as duration,
            from fact_rentals
            group by all
        ) to 'data/fact_rentals_day_grain.parquet'
        """,
        engine=conn
    )
    return


@app.cell
def _(conn, fact_rentals, mo):
    _df = mo.sql(
        f"""
        copy (
            select
                annee,
                DATE_TRUNC('month', started_at) as dt_mois,
                start_station_id,
                -- member_casual,
                DATE_TRUNC('day', started_at) as started_date,
                count(1) as nb_trips,
                sum(duration) as duration,
            from fact_rentals
            group by all
        ) to 'data/fact_rentals_day_grain_no_od.parquet'
        """,
        engine=conn
    )
    return


@app.cell
def _(
    conn,
    dim_station,
    fact_rentals,
    mo,
    station_ends_recap,
    station_starts_recap,
):
    _df = mo.sql(
        f"""
        copy (

        with station_starts_recap as (
            select
                DATE_TRUNC('day', started_at) as dt,
                start_station_id as station_id,
                count(1) as nb_starts,
                sum(duration) as sum_duration_starts,
                COUNT_IF(member_casual='casual') as nb_starts_casual,
                sum(duration) FILTER (member_casual='casual') as sum_duration_casual_starts,
            from fact_rentals
            group by all
        ), station_ends_recap as (
            select
                DATE_TRUNC('day', started_at) as dt,
                end_station_id as station_id,
                count(1) as nb_ends,
                sum(duration) as sum_duration_ends,
                COUNTIF(member_casual='casual') as nb_ends_casual,
                sum(duration) FILTER (member_casual='casual') as sum_duration_casual_ends,
            from fact_rentals
            group by all
        )
        select
        	coalesce(starts.dt, ends.dt) as dt,
        	coalesce(starts.station_id, ends.station_id) as station_id,
            dim_station.nom,
            starts.* exclude(dt, station_id),
            ends.* exclude(dt, station_id),
        from station_starts_recap as starts
        full outer join station_ends_recap as ends using(dt, station_id)
        --> à risque ... sur QUEL station_id a-t-il vraiment fait la jointure ...
        left join dim_station using(station_id)

        ) to 'data/station_daily_recap.parquet'
        """,
        engine=conn
    )
    return


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        copy (from 'data/station_daily_recap.parquet')
        to 's3://bucket-m1-miage-tout-pour-le-collectif/divvy/rentals/annee=2025/station_recap.parquet'
        """,
        engine=conn
    )
    return


if __name__ == "__main__":
    app.run()
