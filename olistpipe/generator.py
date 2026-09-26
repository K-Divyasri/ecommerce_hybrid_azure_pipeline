"""
Deterministic generator for Olist-shaped data.

The real dataset lives on Kaggle behind a login and is licensed CC BY-NC-SA 4.0, so this
project cannot ship it. Instead this module produces the same eight tables with the same
column names and the same quirks that make the real one interesting:

  * customer_id is per ORDER, customer_unique_id is the actual person
  * most people buy once (frequency is heavily tied at 1)
  * Sao Paulo dominates both customers and sellers (a hot-partition trap for NoSQL)
  * ~1.5% of products have no category, and "pc_gamer" has no English translation
  * delivery estimates are generous, but some states are late far more often
  * a Black Friday spike on 2017-11-24
  * a small fraction of deliberately bad rows (negative prices, review score 6,
    delivered-before-purchase) so the data quality gate has something to catch

Same seed + same n_orders = byte-identical output.
"""
from __future__ import annotations

import random
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from .schema import TABLES

START = datetime(2017, 1, 1)
END = datetime(2018, 8, 31, 23, 59, 59)
# The snapshot moment: the OLTP database is seeded "as it looked" at this instant.
AS_OF = datetime(2018, 9, 1)
BLACK_FRIDAY = datetime(2017, 11, 24)

# customer share per state, roughly the real Olist distribution
CUSTOMER_STATE_WEIGHTS = {
    "SP": 42.0, "RJ": 12.9, "MG": 11.7, "RS": 5.5, "PR": 5.1, "SC": 3.7, "BA": 3.4,
    "DF": 2.2, "ES": 2.0, "GO": 2.0, "PE": 1.7, "CE": 1.3, "PA": 1.0, "MT": 0.9,
    "MA": 0.75, "MS": 0.7, "PB": 0.55, "PI": 0.5, "RN": 0.5, "AL": 0.4, "SE": 0.35,
    "TO": 0.28, "RO": 0.25, "AM": 0.15, "AC": 0.08, "AP": 0.07, "RR": 0.05,
}
SELLER_STATE_WEIGHTS = {
    "SP": 59.7, "PR": 11.3, "MG": 7.9, "SC": 6.1, "RJ": 5.4, "RS": 4.1, "GO": 1.3,
    "DF": 1.0, "ES": 0.8, "BA": 0.6, "CE": 0.4, "PE": 0.3, "MT": 0.2, "MS": 0.2,
    "RN": 0.2, "PB": 0.2, "MA": 0.1, "PA": 0.1, "PI": 0.1,
}
CITIES = {
    "SP": ["sao paulo", "campinas", "guarulhos", "santos", "sorocaba", "ribeirao preto",
           "sao jose dos campos", "osasco", "santo andre"],
    "RJ": ["rio de janeiro", "niteroi", "sao goncalo", "duque de caxias", "nova iguacu"],
    "MG": ["belo horizonte", "uberlandia", "contagem", "juiz de fora", "montes claros"],
    "RS": ["porto alegre", "caxias do sul", "pelotas", "canoas"],
    "PR": ["curitiba", "londrina", "maringa", "ponta grossa"],
    "SC": ["florianopolis", "joinville", "blumenau", "itajai"],
    "BA": ["salvador", "feira de santana", "vitoria da conquista"],
    "DF": ["brasilia"], "ES": ["vitoria", "vila velha", "serra"], "GO": ["goiania", "anapolis"],
    "PE": ["recife", "jaboatao dos guararapes", "olinda"], "CE": ["fortaleza", "caucaia"],
    "PA": ["belem", "ananindeua"], "MT": ["cuiaba", "varzea grande"], "MA": ["sao luis"],
    "MS": ["campo grande", "dourados"], "PB": ["joao pessoa", "campina grande"],
    "PI": ["teresina"], "RN": ["natal", "mossoro"], "AL": ["maceio"], "SE": ["aracaju"],
    "TO": ["palmas"], "RO": ["porto velho"], "AM": ["manaus"], "AC": ["rio branco"],
    "AP": ["macapa"], "RR": ["boa vista"],
}
# first two digits of the 5-digit CEP prefix, per state (rough real ranges)
ZIP_RANGE = {
    "SP": (1000, 19999), "RJ": (20000, 28999), "ES": (29000, 29999), "MG": (30000, 39999),
    "BA": (40000, 48999), "SE": (49000, 49999), "PE": (50000, 56999), "AL": (57000, 57999),
    "PB": (58000, 58999), "RN": (59000, 59999), "CE": (60000, 63999), "PI": (64000, 64999),
    "MA": (65000, 65999), "PA": (66000, 68899), "AP": (68900, 68999), "AM": (69000, 69299),
    "RR": (69300, 69399), "AC": (69900, 69999), "DF": (70000, 73699), "GO": (72800, 76799),
    "TO": (77000, 77999), "MT": (78000, 78899), "RO": (76800, 76999), "MS": (79000, 79999),
    "PR": (80000, 87999), "SC": (88000, 89999), "RS": (90000, 99999),
}
REGION = {
    "SP": "SE", "RJ": "SE", "MG": "SE", "ES": "SE", "PR": "S", "SC": "S", "RS": "S",
    "DF": "CO", "GO": "CO", "MT": "CO", "MS": "CO", "BA": "NE", "PE": "NE", "CE": "NE",
    "MA": "NE", "PB": "NE", "PI": "NE", "RN": "NE", "AL": "NE", "SE": "NE", "PA": "N",
    "TO": "N", "RO": "N", "AM": "N", "AC": "N", "AP": "N", "RR": "N",
}
# extra probability of a delivery shock (late) by destination state; real Olist has
# AL, MA, PI, CE, SE, BA, RJ among the worst on-time rates
LATE_BOOST = {"AL": 0.17, "MA": 0.14, "PI": 0.10, "CE": 0.10, "SE": 0.12, "BA": 0.08,
              "RJ": 0.07, "PA": 0.08, "RR": 0.12, "AM": 0.06, "ES": 0.05, "TO": 0.06}

# (portuguese, english or None, popularity weight, median price BRL)
CATEGORIES = [
    ("cama_mesa_banho", "bed_bath_table", 10.0, 70), ("beleza_saude", "health_beauty", 9.0, 90),
    ("esporte_lazer", "sports_leisure", 8.0, 80), ("moveis_decoracao", "furniture_decor", 7.5, 85),
    ("informatica_acessorios", "computers_accessories", 7.0, 95),
    ("utilidades_domesticas", "housewares", 6.5, 65),
    ("relogios_presentes", "watches_gifts", 5.5, 160), ("telefonia", "telephony", 4.0, 45),
    ("ferramentas_jardim", "garden_tools", 4.0, 90), ("automotivo", "auto", 3.8, 100),
    ("brinquedos", "toys", 3.8, 85), ("cool_stuff", "cool_stuff", 3.5, 140),
    ("perfumaria", "perfumery", 3.2, 95), ("bebes", "baby", 2.8, 110),
    ("eletronicos", "electronics", 2.5, 55), ("papelaria", "stationery", 2.3, 75),
    ("fashion_bolsas_e_acessorios", "fashion_bags_accessories", 1.9, 60),
    ("pet_shop", "pet_shop", 1.8, 90), ("moveis_escritorio", "office_furniture", 1.6, 160),
    ("consoles_games", "consoles_games", 1.0, 120),
    ("malas_acessorios", "luggage_accessories", 1.0, 140),
    ("construcao_ferramentas_construcao", "construction_tools_construction", 0.8, 110),
    ("eletrodomesticos", "home_appliances", 0.7, 95),
    ("instrumentos_musicais", "musical_instruments", 0.6, 250),
    ("livros_interesse_geral", "books_general_interest", 0.5, 70),
    ("alimentos", "food", 0.5, 45), ("bebidas", "drinks", 0.4, 55),
    # present in products but missing from the translation table, exactly like real Olist
    ("pc_gamer", None, 0.1, 900),
]

REVIEW_TEXT = {
    5: ["Produto chegou antes do prazo, recomendo", "Otimo produto, entrega rapida",
        "Muito bom, exatamente como na foto", "Adorei, chegou bem embalado",
        "Excelente vendedor, recomendo a loja"],
    4: ["Bom produto, entrega dentro do prazo", "Gostei, mas a embalagem veio amassada",
        "Produto bom, poderia ser mais barato"],
    3: ["Produto ok, nada demais", "Chegou certo mas demorou um pouco",
        "Qualidade razoavel pelo preco"],
    2: ["Produto diferente do anunciado", "Demorou muito para chegar",
        "Veio faltando uma peca"],
    1: ["Nao recebi o produto ate agora", "Produto veio com defeito, quero devolver",
        "Pessimo, entrega atrasada e sem resposta do vendedor",
        "Comprei dois e so chegou um"],
}
REVIEW_TITLES = {5: "Recomendo", 4: "Bom", 3: "Ok", 2: "Ruim", 1: "Nao recomendo"}


def _hex_id(rnd: random.Random) -> str:
    return "%032x" % rnd.getrandbits(128)


def _fmt(ts: datetime | None) -> str | None:
    return None if ts is None else ts.strftime("%Y-%m-%d %H:%M:%S")


def _weighted(d: dict[str, float]) -> tuple[list[str], np.ndarray]:
    keys = list(d)
    w = np.array([d[k] for k in keys], dtype=float)
    return keys, w / w.sum()


def _daily_weights(days: list[datetime]) -> np.ndarray:
    """Order volume per day: growth trend, weekday pattern, Black Friday week."""
    n = len(days)
    trend = np.linspace(1.0, 2.6, n)
    weekday = np.array([1.12, 1.15, 1.08, 1.05, 0.98, 0.78, 0.84])[[d.weekday() for d in days]]
    w = trend * weekday
    for i, d in enumerate(days):
        delta = (d - BLACK_FRIDAY).days
        if delta == 0:
            w[i] *= 5.0
        elif 1 <= delta <= 3:
            w[i] *= 1.8
    return w / w.sum()


def generate(n_orders: int = 5000, seed: int = 42, dirty_rate: float = 0.004,
             as_of: datetime = AS_OF) -> dict[str, pd.DataFrame]:
    """Build all eight tables as DataFrames with Kaggle column names (no updated_at:
    the real CSVs do not have one either; the loader derives it)."""
    rng = np.random.default_rng(seed)
    rnd = random.Random(seed)

    # --- translation --------------------------------------------------------------
    translation = pd.DataFrame(
        [(pt, en) for pt, en, _, _ in CATEGORIES if en is not None],
        columns=["product_category_name", "product_category_name_english"],
    )

    # --- products -----------------------------------------------------------------
    n_products = max(60, n_orders // 3)
    cat_w = np.array([c[2] for c in CATEGORIES])
    cat_idx = rng.choice(len(CATEGORIES), size=n_products, p=cat_w / cat_w.sum())
    products, product_price = [], []
    for i in range(n_products):
        pt, _en, _w, median = CATEGORIES[cat_idx[i]]
        pid = _hex_id(rnd)
        has_cat = rng.random() > 0.015
        weight = int(max(50, rng.lognormal(6.6, 1.0)))
        products.append({
            "product_id": pid,
            "product_category_name": pt if has_cat else None,
            "product_name_lenght": int(rng.integers(20, 64)) if has_cat else None,
            "product_description_lenght": int(rng.integers(80, 3000)) if has_cat else None,
            "product_photos_qty": int(rng.integers(1, 7)) if has_cat else None,
            "product_weight_g": weight,
            "product_length_cm": int(rng.integers(16, 80)),
            "product_height_cm": int(rng.integers(2, 60)),
            "product_width_cm": int(rng.integers(11, 60)),
        })
        product_price.append(round(float(median * rng.lognormal(0, 0.55)), 2))
    product_pop = rng.zipf(1.6, n_products).astype(float)
    product_pop = np.minimum(product_pop, 200)
    product_pop = product_pop / product_pop.sum()

    # --- sellers ------------------------------------------------------------------
    n_sellers = max(25, n_orders // 32)
    s_states, s_p = _weighted(SELLER_STATE_WEIGHTS)
    sellers = []
    for _ in range(n_sellers):
        st = s_states[rng.choice(len(s_states), p=s_p)]
        lo, hi = ZIP_RANGE[st]
        sellers.append({
            "seller_id": _hex_id(rnd),
            "seller_zip_code_prefix": f"{int(rng.integers(lo, hi)):05d}",
            "seller_city": CITIES[st][rng.integers(0, len(CITIES[st]))],
            "seller_state": st,
        })
    # each seller has a hidden "quality": slow sellers hand off to the carrier late
    seller_slowness = rng.gamma(2.0, 0.6, n_sellers)
    product_seller = rng.integers(0, n_sellers, n_products)

    # --- people and how many orders each places -----------------------------------
    c_states, c_p = _weighted(CUSTOMER_STATE_WEIGHTS)
    people = []  # (unique_id, state, city, zip, n_orders)
    total = 0
    while total < n_orders:
        k = int(rng.choice([1, 2, 3, 4, 5], p=[0.86, 0.10, 0.028, 0.008, 0.004]))
        k = min(k, n_orders - total)
        st = c_states[rng.choice(len(c_states), p=c_p)]
        lo, hi = ZIP_RANGE[st]
        people.append((_hex_id(rnd), st, CITIES[st][rng.integers(0, len(CITIES[st]))],
                       f"{int(rng.integers(lo, hi)):05d}", k))
        total += k

    # --- purchase timestamps ------------------------------------------------------
    days = [START + timedelta(days=i) for i in range((END - START).days + 1)]
    day_p = _daily_weights(days)
    day_idx = np.sort(rng.choice(len(days), size=n_orders, p=day_p))
    hour_w = np.array([1, .5, .3, .2, .2, .3, .8, 1.5, 2.5, 3.5, 4.2, 4.4,
                       4.0, 4.3, 4.5, 4.3, 4.2, 4.1, 3.9, 3.9, 4.2, 4.5, 3.8, 2.4])
    hours = rng.choice(24, size=n_orders, p=hour_w / hour_w.sum())
    secs = rng.integers(0, 3600, n_orders)
    purchase_times = [days[d] + timedelta(hours=int(h), seconds=int(s))
                      for d, h, s in zip(day_idx, hours, secs)]
    # hand timestamps to people: shuffle order slots, repeat buyers get several
    slots = rng.permutation(n_orders)
    slot_owner = []
    for p_i, person in enumerate(people):
        slot_owner += [p_i] * person[4]
    owners = np.empty(n_orders, dtype=int)
    owners[slots] = slot_owner

    # product picks drawn in one vectorised call (drawing inside the loop is O(n^2))
    first_pick = rng.choice(n_products, size=n_orders, p=product_pop)
    extra_pick = rng.choice(n_products, size=n_orders * 4, p=product_pop)

    customers, orders, items, payments, reviews = [], [], [], [], []
    for o in range(n_orders):
        uid, st, city, zipc, _k = people[owners[o]]
        purchase = purchase_times[o]
        cid, oid = _hex_id(rnd), _hex_id(rnd)
        customers.append({"customer_id": cid, "customer_unique_id": uid,
                          "customer_zip_code_prefix": zipc, "customer_city": city,
                          "customer_state": st})

        roll = rng.random()
        status_override = "canceled" if roll < 0.006 else "unavailable" if roll < 0.011 else None

        # items
        n_items = int(rng.choice([1, 2, 3, 4], p=[0.90, 0.075, 0.018, 0.007]))
        pidx = int(first_pick[o])
        s_idx = int(product_seller[pidx])
        seller = sellers[s_idx]
        approved = purchase + timedelta(minutes=float(rng.exponential(600)))
        carrier = approved + timedelta(days=float(rng.gamma(2.0, 0.9) * seller_slowness[s_idx] + 0.3))
        shipping_limit = approved + timedelta(days=6)
        order_items = []
        if status_override != "unavailable":
            for it in range(n_items):
                if it > 0 and rng.random() < 0.5:  # a second different product
                    pidx = int(extra_pick[o * 4 + it])
                price = round(product_price[pidx] * float(rng.uniform(0.92, 1.08)), 2)
                weight = products[pidx]["product_weight_g"]
                far = REGION[seller["seller_state"]] != REGION[st]
                freight = round(8 + weight / 1000 * 2.2 + (9 if far else 0) + float(rng.uniform(0, 6)), 2)
                order_items.append({
                    "order_id": oid, "order_item_id": it + 1,
                    "product_id": products[pidx]["product_id"], "seller_id": seller["seller_id"],
                    "shipping_limit_date": _fmt(shipping_limit), "price": price,
                    "freight_value": freight,
                })
        items += order_items

        # delivery timeline
        if seller["seller_state"] == st:
            transit_mu = 4.5
        elif REGION[seller["seller_state"]] == REGION[st]:
            transit_mu = 8.0
        elif REGION[st] in ("N", "NE"):
            transit_mu = 16.0
        else:
            transit_mu = 11.0
        transit = float(rng.gamma(4.0, transit_mu / 4.0))
        late_p = 0.035 + LATE_BOOST.get(st, 0.0)
        if 0 <= (purchase - BLACK_FRIDAY).days <= 10:
            late_p += 0.12
        if rng.random() < late_p:
            transit += float(rng.uniform(12, 30))
        delivered = carrier + timedelta(days=transit)
        est_days = int(round(transit_mu + 12 + rng.integers(0, 8)))
        estimated = datetime(purchase.year, purchase.month, purchase.day) + timedelta(days=est_days)

        if status_override:
            status = status_override
            approved_v = approved if rng.random() < 0.7 and approved <= as_of else None
            carrier_v = delivered_v = None
        elif delivered <= as_of:
            status, approved_v, carrier_v, delivered_v = "delivered", approved, carrier, delivered
        elif carrier <= as_of:
            status, approved_v, carrier_v, delivered_v = "shipped", approved, carrier, None
        elif approved <= as_of:
            status = "invoiced" if rng.random() < 0.5 else "processing"
            approved_v, carrier_v, delivered_v = approved, None, None
        else:
            status, approved_v, carrier_v, delivered_v = "created", None, None, None

        orders.append({
            "order_id": oid, "customer_id": cid, "order_status": status,
            "order_purchase_timestamp": _fmt(purchase), "order_approved_at": _fmt(approved_v),
            "order_delivered_carrier_date": _fmt(carrier_v),
            "order_delivered_customer_date": _fmt(delivered_v),
            "order_estimated_delivery_date": _fmt(estimated),
        })

        # payments
        total_value = round(sum(i["price"] + i["freight_value"] for i in order_items), 2)
        if not order_items:
            total_value = round(float(rng.uniform(30, 300)), 2)
        ptype = str(rng.choice(["credit_card", "boleto", "voucher", "debit_card"],
                               p=[0.74, 0.19, 0.055, 0.015]))
        if ptype == "voucher" and total_value > 40 and rng.random() < 0.5:
            first = round(float(rng.uniform(10, total_value / 2)), 2)
            payments.append({"order_id": oid, "payment_sequential": 1, "payment_type": "voucher",
                             "payment_installments": 1, "payment_value": first})
            payments.append({"order_id": oid, "payment_sequential": 2, "payment_type": "credit_card",
                             "payment_installments": int(rng.integers(1, 6)),
                             "payment_value": round(total_value - first, 2)})
        else:
            inst = int(rng.integers(1, 11)) if ptype == "credit_card" else 1
            payments.append({"order_id": oid, "payment_sequential": 1, "payment_type": ptype,
                             "payment_installments": inst, "payment_value": total_value})

        # reviews: nearly every delivered order, most canceled ones
        review_at = None
        if status == "delivered" and rng.random() < 0.99:
            review_at = delivered
        elif status in ("canceled", "unavailable") and rng.random() < 0.7:
            review_at = purchase + timedelta(days=float(rng.uniform(3, 20)))
        if review_at is not None:
            created = datetime(review_at.year, review_at.month, review_at.day) + timedelta(days=1)
            if created > as_of:
                review_at = None
        if review_at is not None:
            is_late = delivered_v is not None and delivered_v.date() > estimated.date()
            if status != "delivered":
                p = [0.70, 0.12, 0.08, 0.05, 0.05]
            elif is_late:
                p = [0.46, 0.12, 0.14, 0.12, 0.16]
            else:
                p = [0.07, 0.025, 0.075, 0.20, 0.63]
            score = int(rng.choice([1, 2, 3, 4, 5], p=p))
            has_msg = rng.random() < (0.75 if score <= 2 else 0.35)
            reviews.append({
                "review_id": _hex_id(rnd), "order_id": oid, "review_score": score,
                "review_comment_title": REVIEW_TITLES[score] if rng.random() < 0.12 else None,
                "review_comment_message": (REVIEW_TEXT[score][rng.integers(0, len(REVIEW_TEXT[score]))]
                                           if has_msg else None),
                "review_creation_date": _fmt(created),
                "review_answer_timestamp": _fmt(min(created + timedelta(hours=float(rng.uniform(8, 72))), as_of)),
            })

    frames = {
        "olist_customers": pd.DataFrame(customers),
        "olist_orders": pd.DataFrame(orders),
        "olist_order_items": pd.DataFrame(items),
        "olist_order_payments": pd.DataFrame(payments),
        "olist_order_reviews": pd.DataFrame(reviews),
        "olist_products": pd.DataFrame(products),
        "olist_sellers": pd.DataFrame(sellers),
        "product_category_name_translation": translation,
    }
    _inject_dirt(frames, rng, dirty_rate)
    for name, df in frames.items():
        frames[name] = df[TABLES[name].column_names]
    return frames


def _inject_dirt(frames: dict[str, pd.DataFrame], rng: np.random.Generator, rate: float) -> None:
    """Small, known amounts of mess. Some is fixable by cleaning (case, whitespace),
    some must be quarantined by the data quality gate (impossible values)."""
    if rate <= 0:
        return
    items = frames["olist_order_items"]
    bad = rng.random(len(items)) < rate
    items.loc[bad, "price"] = -items.loc[bad, "price"]  # negative price: quarantine

    reviews = frames["olist_order_reviews"]
    bad = rng.random(len(reviews)) < rate
    reviews.loc[bad, "review_score"] = 6  # outside 1..5: quarantine

    orders = frames["olist_orders"]
    delivered = orders["order_delivered_customer_date"].notna()
    bad = delivered & (rng.random(len(orders)) < rate)
    # delivered two days BEFORE purchase: impossible, quarantine
    orders.loc[bad, "order_delivered_customer_date"] = (
        pd.to_datetime(orders.loc[bad, "order_purchase_timestamp"]) - pd.Timedelta(days=2)
    ).dt.strftime("%Y-%m-%d %H:%M:%S")

    customers = frames["olist_customers"]
    messy = rng.random(len(customers)) < rate * 5
    # fixable mess: lower-case state with a trailing space, shouting city names
    customers.loc[messy, "customer_state"] = customers.loc[messy, "customer_state"].str.lower() + " "
    customers.loc[messy, "customer_city"] = "  " + customers.loc[messy, "customer_city"].str.upper()


def write_csvs(frames: dict[str, pd.DataFrame], out_dir: Path) -> dict[str, int]:
    """Write with the real Kaggle file names so the real dataset is a drop-in swap."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    counts = {}
    for name, df in frames.items():
        df.to_csv(out_dir / TABLES[name].csv, index=False)
        counts[name] = len(df)
    return counts


def read_csvs(src_dir: Path) -> dict[str, pd.DataFrame]:
    """Read either the generated CSVs or the real Kaggle ones. utf-8-sig strips the BOM
    that the real translation file starts with. Ids and zip codes stay strings."""
    src_dir = Path(src_dir)
    frames = {}
    for name, t in TABLES.items():
        path = src_dir / t.csv
        if not path.exists():
            raise FileNotFoundError(f"missing {path}. Run `python -m olistpipe generate` first "
                                    "or copy the Kaggle CSVs there.")
        dtypes = {c: "string" for c, ty in t.columns if ty == "str"}
        df = pd.read_csv(path, dtype=dtypes, encoding="utf-8-sig")
        frames[name] = df[t.column_names]
    return frames
