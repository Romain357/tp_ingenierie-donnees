import pandas as pd
from pathlib import Path


def transformer_polluants():
    dossier_entree = Path("data")
    dossier_sortie = Path("data/transform")

    dossier_sortie.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(dossier_entree / "polluants.csv")

    df_polluants = df[
        [
            "codeue",
            "code",
            "libelle_fr",
            "code_unite_concentration"
        ]
    ].drop_duplicates()

    df_polluants = df_polluants.rename(columns={
        "codeue": "id_poll_ue",
        "code": "code_poll",
        "libelle_fr": "nom_poll",
        "code_unite_concentration": "unite"
    })

    df_polluants = df_polluants.dropna(subset=["id_poll_ue"])

    fichier_sortie = dossier_sortie / "polluants.csv"

    df_polluants.to_csv(
        fichier_sortie,
        index=False,
        encoding="utf-8"
    )

    print(f"Fichier créé : {fichier_sortie}")
    print(f"{len(df_polluants)} polluants récupérés")
    print(df_polluants.head())


if __name__ == "__main__":
    transformer_polluants()