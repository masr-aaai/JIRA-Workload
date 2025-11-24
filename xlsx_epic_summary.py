import pandas as pd

# Pfad zur Excel-Datei anpassen
datei = "C:/Users/masr/OneDrive - VINCI Energies/Dokumente/eco2VE/eco2ve Billing/worklogs_hierarchical_2025-04_2025-10.xlsx"


# Alle Sheets einlesen
xls = pd.ExcelFile(datei)

alle_df = []

for sheet in xls.sheet_names:
    # Falls du bestimmte Sheets überspringen willst, hier abfangen:
    # if sheet == "Epic_Summary":
    #     continue

    # Spalten A, B und F einlesen
    df = pd.read_excel(
        xls,
        sheet_name=sheet,
        usecols="A,B,F"   # A = Issue, B = Issue Type, F = Sum h epic
    )

    # Spaltennamen ggf. vereinheitlichen (nur falls nötig)
    df.columns = ["Issue", "Issue Type", "Sum h epic"]

    # Optional: Herkunftssheet merken (nur für Debug / Kontrolle)
    df["Source Sheet"] = sheet

    alle_df.append(df)

# Alle Sheets zu einer großen Tabelle zusammenführen
gesamt = pd.concat(alle_df, ignore_index=True)

# Nur Epics behalten (Groß-/Kleinschreibung robust behandeln)
mask_epic = gesamt["Issue Type"].astype(str).str.lower() == "epic"
epics = gesamt[mask_epic].copy()

# Nach Issue gruppieren und Summe der Stunden bilden
summary = (
    epics
    .groupby("Issue", as_index=False)["Sum h epic"]
    .sum()
    .sort_values("Issue")
)

# Ergebnis-Sheet-Name
summary_sheet_name = "Epic_Summary"

# Ergebnis in die Excel-Datei zurückschreiben (neues/überschriebenes Sheet)
with pd.ExcelWriter(
    datei,
    engine="openpyxl",
    mode="a",                 # an bestehende Datei anhängen
    if_sheet_exists="replace" # vorhandenes Summary-Sheet ersetzen
) as writer:
    summary.to_excel(writer, sheet_name=summary_sheet_name, index=False)

print(f"Fertig. Zusammenfassung steht im Sheet '{summary_sheet_name}'.")
