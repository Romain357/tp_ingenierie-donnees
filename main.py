# main.py
from extractionstation import extraire_stations
from extractiondepartement import \
    extraire_stations as extraire_departements  # Attention au nom de ta fonction dans ce fichier !
from extractpolluant import executer_extraction_polluants
from extraction import extraire_mesures
from TransformationTableJaune import transformer_communes
from TransformationTableBleu import transformer_polluants
from TransformationTableOrange import transformer_stations
from TransformationTableVerte import transformer_mesures


def run_etl():
    print("--- 1. PHASE D'EXTRACTION ---")
    extraire_departements()
    extraire_stations()
    executer_extraction_polluants()
    extraire_mesures()

    print("--- 2. PHASE DE TRANSFORMATION ET CHARGEMENT ---")
    transformer_communes()
    transformer_polluants()
    transformer_stations()
    transformer_mesures()

    print("🚀 ETL terminé avec succès !")


if __name__ == "__main__":
    run_etl()