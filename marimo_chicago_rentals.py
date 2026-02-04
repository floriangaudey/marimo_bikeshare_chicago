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
    Suite à la première analyse de l'ensemble des données, quelles sont les colonnes qui peuvent être exploitées en tant qu'axe d'analyse ?
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 3. Première réduction des données

    A partir des 12 fichiers, représentant l'activité des locations de vélos pour chaque mois sur un an, vous allez créer la première table avec l'ensemble des données de lannée que vous avec choisi.
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


@app.cell(hide_code=True)
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


@app.cell(hide_code=True)
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


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 4. Cas pratique :

    Vous avez agrégé vos données dans une table de dimension et une table de Fait (correspondant au layer Silver).
    Vous avez remarqué que la taille du parquet et le nombre de données a diminué.

    Désormais, vous allez proposer plusieurs cas d'usage suite à l'exploration des donnés que vous allez effectuer sur les données de Chicago (Correspondant au layer Gold).

    L'attendu est le suivant :
    - Proposer une agrégation des données issues de la table de Fait et de la table de dimension.
    - Expliquer la démarche de votre table Gold en adéquation avec l'étude des locations de vélos dans Chicago.

    Lorsque votre réduction de dimension est terminée, vous exécutez le script ci-dessous pour envoyer votre fichier parquet sur un bucket.

    NB : N'oubliez de changer la bonne année pour être au bon endroit
    """)
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
