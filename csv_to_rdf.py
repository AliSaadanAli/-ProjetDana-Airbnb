# csv_to_rdf.py
import pandas as pd, re
from pathlib import Path
from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, XSD

CSV = "paris_airbnb.csv"
OUT = "airbnb.ttl"

SCHEMA = Namespace("https://schema.org/")
GEO    = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
DCT    = Namespace("http://purl.org/dc/terms/")
DBR    = Namespace("http://dbpedia.org/resource/")
EX     = Namespace("https://example.org/vocab/")
BASE   = "https://example.org/airbnb/"

# ---------- Helpers qui ignorent les vides ----------
def is_missing(x):
    return x is None or (isinstance(x, float) and pd.isna(x)) or (isinstance(x, str) and x.strip() == "")

def to_decimal(x):
    if is_missing(x): return None
    s = str(x)
    # supprime $, €, %, espaces, séparateurs de milliers, puis convertit virgule -> point
    s = re.sub(r"[€$£%\s,]", "", s).replace(",", ".")
    try:
        return float(s)
    except:
        return None

def to_int(x):
    if is_missing(x): return None
    try:
        return int(float(str(x).strip()))
    except:
        return None

# ---------- Lecture CSV ----------
df = pd.read_csv(CSV)

# Si pas d'id, on crée une clé stable depuis l'index
if "id" not in df.columns:
    df = df.reset_index().rename(columns={"index":"id"})

# Certaines colonnes peuvent ne pas exister : on détecte ce qui est présent
cols = set(df.columns.str.strip())
def has(c): return c in cols

# colonne accommodates avec ou sans faute de frappe
acc_col = "accommodates" if has("accommodates") else ("accomodates" if has("accomodates") else None)

# ---------- Graphe ----------
g = Graph()
g.bind("schema", SCHEMA)
g.bind("geo", GEO)
g.bind("dct", DCT)
g.bind("xsd", XSD)
g.bind("dbr", DBR)
g.bind("ex", EX)

for _, row in df.iterrows():
    rid = row.get("id")
    if is_missing(rid):  # si ID vide, on saute la ligne
        continue

    s = URIRef(BASE + f"listing/{to_int(rid)}")
    g.add((s, RDF.type, SCHEMA.Accommodation))

    # --- Localisation ---
    if has("latitude"):
        lat = to_decimal(row.get("latitude"))
        if lat is not None: g.add((s, GEO.lat, Literal(lat, datatype=XSD.decimal)))
    if has("longitude"):
        lon = to_decimal(row.get("longitude"))
        if lon is not None: g.add((s, GEO.long, Literal(lon, datatype=XSD.decimal)))

    if has("city") and not is_missing(row.get("city")):
        g.add((s, SCHEMA.address, Literal(str(row.get("city")).strip())))
    if has("zipcode") and not is_missing(row.get("zipcode")):
        g.add((s, SCHEMA.postalCode, Literal(str(row.get("zipcode")).strip())))
    if has("state") and not is_missing(row.get("state")):
        g.add((s, SCHEMA.addressRegion, Literal(str(row.get("state")).strip())))

    # --- Caractéristiques ---
    if acc_col:
        acc = to_int(row.get(acc_col))
        if acc is not None: g.add((s, SCHEMA.occupancy, Literal(acc, datatype=XSD.integer)))

    if has("room_type") and not is_missing(row.get("room_type")):
        g.add((s, SCHEMA.roomType, Literal(str(row.get("room_type")).strip())))

    if has("bedrooms"):
        bedrooms = to_int(row.get("bedrooms"))
        if bedrooms is not None: g.add((s, SCHEMA.numberOfRooms, Literal(bedrooms, datatype=XSD.integer)))

    if has("bathrooms"):
        bathrooms = to_decimal(row.get("bathrooms"))
        if bathrooms is not None: g.add((s, SCHEMA.numberOfBathroomsTotal, Literal(bathrooms, datatype=XSD.decimal)))

    if has("beds"):
        beds = to_int(row.get("beds"))
        if beds is not None: g.add((s, SCHEMA.numberOfBeds, Literal(beds, datatype=XSD.integer)))

    # --- Prix & règles ---
    if has("price"):
        price = to_decimal(row.get("price"))
        if price is not None: g.add((s, SCHEMA.price, Literal(price, datatype=XSD.decimal)))

    if has("cleaning_fee"):
        cleaning_fee = to_decimal(row.get("cleaning_fee"))
        if cleaning_fee is not None: g.add((s, SCHEMA.cleaningFee, Literal(cleaning_fee, datatype=XSD.decimal)))

    if has("security_deposit"):
        sec_dep = to_decimal(row.get("security_deposit"))
        if sec_dep is not None: g.add((s, SCHEMA.securityDeposit, Literal(sec_dep, datatype=XSD.decimal)))

    if has("minimum_nights"):
        min_n = to_int(row.get("minimum_nights"))
        if min_n is not None: g.add((s, EX.minimumNights, Literal(min_n, datatype=XSD.integer)))

    if has("maximum_nights"):
        max_n = to_int(row.get("maximum_nights"))
        if max_n is not None: g.add((s, EX.maximumNights, Literal(max_n, datatype=XSD.integer)))

    if has("number_of_reviews"):
        reviews = to_int(row.get("number_of_reviews"))
        if reviews is not None: g.add((s, SCHEMA.reviewCount, Literal(reviews, datatype=XSD.integer)))

    # --- Agrégats côté hôte (si présents) ---
    if has("host_response_rate"):
        resp = to_decimal(row.get("host_response_rate"))
        if resp is not None: g.add((s, EX.hostResponseRate, Literal(resp, datatype=XSD.decimal)))

    if has("host_acceptance_rate"):
        acc_rate = to_decimal(row.get("host_acceptance_rate"))
        if acc_rate is not None: g.add((s, EX.hostAcceptanceRate, Literal(acc_rate, datatype=XSD.decimal)))

    if has("host_listings_count"):
        host_count = to_int(row.get("host_listings_count"))
        if host_count is not None: g.add((s, EX.hostListingsCount, Literal(host_count, datatype=XSD.integer)))

    # Lien utile pour plus tard
    g.add((s, SCHEMA.containedInPlace, DBR.Paris))

# Métadonnées dataset
dataset = URIRef(BASE + "dataset/paris")
g.add((dataset, DCT.title, Literal("Airbnb Paris (extrait)")))

g.serialize(destination=OUT, format="turtle")
print(f"✅ RDF écrit: {OUT} | Triples: {len(g)}")