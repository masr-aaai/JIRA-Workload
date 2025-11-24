import pandas as pd
from typing import Optional  # <- wichtig für Optional[str]


def build_summary(excel_path: str) -> None:
    """
    Befüllt das Blatt 'Summary' in der angegebenen Excel-Datei mit einer
    Übersicht: Stunden pro Version & Scope (Planned/Unplanned) pro Monat.

    Annahmen:
    - Es gibt ein Blatt 'Summary' (wird ersetzt).
    - Alle anderen Blätter mit Daten haben Namen wie '2025-04', '2025-05', ...
    - In den Datenblättern:
        Spalte E = Stunden
        Spalte G = Scope ('Planned', 'Unplanned', 'Bug')
        Spalte H = Version ('Version 2.2', ...)
    - Es gibt eine Kopfzeile, aber wir greifen über Spaltenposition zu.
    """

    # Alle Sheets einlesen
    xls = pd.read_excel(excel_path, sheet_name=None, engine="openpyxl")

    # Alle Datenblätter außer 'Summary'
    month_sheets = [name for name in xls.keys() if name != "Summary"]

    if not month_sheets:
        print("Keine Monatsblätter gefunden.")
        return

    # Aggregation: (version, scope_summary) -> {monat: stunden}
    agg = {}

    def map_scope(raw_scope: str) -> Optional[str]:
        """
        Mappt Original-Scope auf den Summary-Scope.
        'Planned' + 'Bug' => 'Planned'
        'Unplanned'       => 'Unplanned'
        andere Werte      => None (wird ignoriert)
        """
        if raw_scope == "Planned" or raw_scope == "Bug":
            return "Planned"
        if raw_scope == "Unplanned":
            return "Unplanned"
        return None

    for sheet_name in month_sheets:
        df = xls[sheet_name]

        # Sicherstellen, dass genügend Spalten da sind
        if df.shape[1] < 8:
            # weniger als 8 Spalten -> E,G,H existieren nicht
            continue

        # Spalten nach Position (0-basiert)
        hours_col = pd.to_numeric(df.iloc[:, 4], errors="coerce")  # Spalte E
        scope_col = df.iloc[:, 6].astype(str)                      # Spalte G
        version_col = df.iloc[:, 7].astype(str)                    # Spalte H

        temp = pd.DataFrame({
            "version": version_col,
            "scope_raw": scope_col,
            "hours": hours_col
        })

        # Nur Zeilen mit Stunden
        temp = temp[temp["hours"].notna()]

        # Scopes mappen
        temp["scope"] = temp["scope_raw"].apply(map_scope)
        temp = temp[temp["scope"].notna()]

        if temp.empty:
            continue

        # Gruppieren nach Version + zusammengefasstem Scope
        grouped = temp.groupby(["version", "scope"], dropna=False)["hours"].sum()

        for (version, scope), hours_sum in grouped.items():
            key = (version, scope)
            if key not in agg:
                agg[key] = {}
            agg[key][sheet_name] = float(hours_sum)

    if not agg:
        print("Keine passenden Daten gefunden.")
        return

    # Sortierung der Keys (zuerst Version alphabetisch, dann Scope)
    keys = sorted(agg.keys(), key=lambda x: (x[0], x[1]))

    # Spalten in der Summary-Tabelle
    months_sorted = sorted(month_sheets)  # z. B. '2025-04', '2025-05', ...

    # DataFrame für Summary aufbauen
    rows = []
    for version, scope in keys:
        row_label = f"{version} - {scope}"
        row = {"Version - Scope": row_label}
        for m in months_sorted:
            row[m] = agg.get((version, scope), {}).get(m, 0.0)
        rows.append(row)

    summary_df = pd.DataFrame(rows, columns=["Version - Scope"] + months_sorted)

    # Summary-Sheet zurück in die Excel-Datei schreiben (ersetzt bestehendes)
    with pd.ExcelWriter(
        excel_path,
        engine="openpyxl",
        mode="a",
        if_sheet_exists="replace"
    ) as writer:
        summary_df.to_excel(writer, sheet_name="Summary", index=False)

    print(f"Summary in '{excel_path}' aktualisiert.")



build_summary("C:/Users/masr/OneDrive - VINCI Energies/Dokumente/eco2VE/eco2ve Billing/worklogs_hierarchical_2025-04_2025-11 edited.xlsx")