import os
import csv
import shutil
from faker import Faker
from datetime import date

# Tento skript simuluje Bronze vstup v Databricks
fake = Faker()
base_dir = "/mnt/data/bronze"
os.makedirs(base_dir, exist_ok=True)

# Definice tabulek a počtů
tables = {
    "agents":    {"snapshot": True,  "count": 50},
    "customers": {"snapshot": True,  "count": 100},
    "policies":  {"snapshot": True,  "count": 200},
    "claims":    {"snapshot": False, "count": 300},
    "products":  {"snapshot": False, "count": 10}
}

# Tři dny snapshotů pro SCD2
snapshot_dates = [
    date(2024, 1, 1),
    date(2024, 1, 2),
    date(2024, 1, 3)
]

# Příprava ID poolů
id_pools = {t: [f"{t[:4].upper()}{i:05d}" for i in range(1, cfg['count']+1)]
            for t, cfg in tables.items()}

# Agents
agent_store = {aid: {
    'name': fake.name(),
    'email': fake.email(),
    'region': fake.random_element(['North','South','East','West']),
    'start_date': fake.date_between(start_date='-5y', end_date='today')
} for aid in id_pools['agents']}

# Customers
customer_store = {cid: {
    'name': fake.name(),
    'address': fake.address().replace('\n', ', '),
    'email': fake.email(),
    'income': round(fake.random_number(digits=6),2)
} for cid in id_pools['customers']}

# Policies
policy_store = {pid: {
    'customer_id': fake.random_element(id_pools['customers']),
    'product_id': fake.random_element(id_pools['products']),
    'premium': round(fake.random_number(digits=4),2)
} for pid in id_pools['policies']}

# Seznam realistických pojišťovacích produktů
insurance_products = [
    "Auto Insurance",
    "Homeowners Insurance",
    "Life Insurance",
    "Health Insurance",
    "Travel Insurance",
    "Renters Insurance",
    "Commercial Property Insurance",
    "Professional Liability Insurance",
    "Pet Insurance",
    "Flood Insurance"
]

# Korupce drobných datových chyb
def corrupt(val):
    if not isinstance(val, str) or not val:
        return val
    if fake.boolean(chance_of_getting_true=5):
        return ''
    s = val
    if fake.boolean(chance_of_getting_true=10):
        s = f"  {s}  "
    if fake.boolean(chance_of_getting_true=10):
        s = ''.join(c.upper() if fake.boolean(chance_of_getting_true=50) else c.lower() for c in s)
    return s

# Pomocná funkce zápisu CSV
def write_csv(path, headers, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

# --- GENEROVÁNÍ SNAPSHOT TABULEK (SCD2) ---
for table in ['agents', 'customers', 'policies']:
    store_map = {
        'agents': agent_store,
        'customers': customer_store,
        'policies': policy_store
    }
    store = store_map[table]
    for idx, snap in enumerate(snapshot_dates):
        folder = snap.strftime('%Y.%m.%d')
        fn = f"{table}_{snap.strftime('%Y%m%d')}.csv"
        path = os.path.join(base_dir, table, folder, fn)
        rows = []
        ids = list(store.keys())
        if idx > 0:
            # 5% nových záznamů
            new_count = int(len(ids) * 0.05)
            for _ in range(new_count):
                new_id = f"{table[:4].upper()}{len(store)+1:05d}"
                if table == 'agents':
                    store[new_id] = {
                        'name': fake.name(),
                        'email': fake.email(),
                        'region': fake.random_element(['North','South','East','West']),
                        'start_date': snap
                    }
                elif table == 'customers':
                    store[new_id] = {
                        'name': fake.name(),
                        'address': fake.address().replace('\n', ', '),
                        'email': fake.email(),
                        'income': round(fake.random_number(digits=6),2)
                    }
                else:
                    store[new_id] = {
                        'customer_id': fake.random_element(id_pools['customers']),
                        'product_id': fake.random_element(id_pools['products']),
                        'premium': round(fake.random_number(digits=4),2)
                    }
                ids.append(new_id)
        for oid in ids:
            rec = store[oid].copy()
            if idx == 2 and fake.boolean(chance_of_getting_true=20):
                # změna atributů ve třetím dni
                if table == 'agents':
                    rec['region'] = fake.random_element(['North','South','East','West'])
                elif table == 'customers':
                    rec['income'] = round(fake.random_number(digits=6),2)
                else:
                    rec['premium'] = round(fake.random_number(digits=4),2)
                store[oid] = rec
            if table == 'agents':
                rows.append([
                    oid,
                    corrupt(rec['name']),
                    corrupt(rec['email']),
                    corrupt(rec['region']),
                    rec['start_date'],
                    snap
                ])
            elif table == 'customers':
                first, last = rec['name'].split(' ', 1)
                rows.append([
                    oid,
                    corrupt(first),
                    corrupt(last),
                    corrupt(rec['email']),
                    corrupt(rec['address']),
                    rec['income'],
                    snap
                ])
            else: 
                rows.append([
                    oid,
                    rec['customer_id'],
                    rec['product_id'],
                    rec['premium'],
                    snap
                ])
        headers_map = {
            'agents': ['agent_id','name','email','region','start_date','snapshot_date'],
            'customers': ['customer_id','first_name','last_name','email','address','income','snapshot_date'],
            'policies': ['policy_id','customer_id','product_id','premium','snapshot_date']
        }
        write_csv(path, headers_map[table], rows)

# --- GENEROVÁNÍ STATICKÝCH TABULEK (Claims, Products) ---
# Products: realistické pojišťovací produkty
top_products = insurance_products
for table in ['claims','products']:
    path = os.path.join(base_dir, table, f"{table}.csv")
    rows = []
    for vid in id_pools[table]:
        if table == 'claims':
            rows.append([
                vid,
                fake.random_element(list(policy_store.keys())),
                fake.date_between(start_date='-2y', end_date='today'),
                round(fake.random_number(digits=5),2)
            ])
        else:
            prod_name = fake.random_element(top_products)
            category = prod_name.split()[0]
            rows.append([
                vid,
                corrupt(prod_name),
                corrupt(category)
            ])
    hdrs = {
        'claims': ['claim_id','policy_id','claim_date','amount'],
        'products': ['product_id','product_name','category']
    }
    write_csv(path, hdrs[table], rows)

# --- GENEROVÁNÍ TRANSAKCÍ PŘÍJMŮ (FactPremium) ---
premium_rows = []
txn = 1
for pid, p in policy_store.items():
    for snap in snapshot_dates:
        amt = p['premium']
        due = snap
        paid = fake.boolean(chance_of_getting_true=90)
        pay_date = due if paid else None
        premium_rows.append([
            f"PRM{txn:06d}",
            pid,
            due,
            amt,
            paid,
            pay_date,
            snap
        ])
        txn += 1
pp = os.path.join(base_dir, 'premium_transactions.csv')
write_csv(
    pp,
    ['premium_txn_id','policy_id','due_date','premium_amount','paid_flag','payment_date','snapshot_date'],
    premium_rows
)

print(f"Bronze ZIP: {zip_path}")

# --- POPIS TABULEK (LOGIKA A ÚČEL) ---
#
# 1) agents (snapshot dimenze, SCD2)
#    Účel: sledovat historii interních prodejních zástupců (agentů). Každý snapshot obsahuje kompletní seznam agentů v daném datu.
#    - agent_id, snapshot_date jako PK -> identifikuje verzi agenta ve čase
#    - start_date: kdy agent nastoupil, nemění se po čas
#    - atribute name, email, region: mohou se měnit a logují se nové verze
#    Logika: v každém snapshotu jsou všechny aktivní agenti. Při změně regionu nebo e-mailu se ve 3. dni objeví nový řádek pro stávající ID.
#
# 2) customers (snapshot dimenze, SCD2)
#    Účel: sledovat vývoj profilu klienta – adresy, příjmu. Každý snapshot reprezentuje stav klientelní báze.
#    - customer_id, snapshot_date jako PK
#    - address a income mohou odrážet změny životní situace klienta
#    Logika: první dva dny statický seznam; třetí den ~5% nových klientů (inserty) a ~20% změn příjmu (updates).
#
# 3) policies (snapshot dimenze, SCD2)
#    Účel: uchovávat historii pojistných smluv, sazby pojistného (premium) a vazbu na klienta a produkt.
#    - policy_id, snapshot_date jako PK určují verzi smlouvy v čase
#    - premium se mění, když se přepracuje sazba smlouvy
#    - vztah na customer_id a product_id
#    Logika: iniciální seznam smluv, v dalších dnech přidává ~5% nových polic (inserty), ve 3. dni mění ~20% sazeb (updates).
#
# 4) products (statická dimenze, SCD1)
#    Účel: katalog pojišťovacích produktů, bez historie.
#    - product_id jako PK
#    - product_name a category popisují typ služby (např. Auto, Homeowners)
#    Logika: data se jednou nahrávají ze zdroje a nepřepisují.
#
# 5) claims (faktová tabulka)
#    Účel: zachytit každou škodní událost podanou klientem.
#    - claim_id jako PK
#    - policy_id odkazuje na DimPolicy verzi platnou v době claim_date
#    - claim_date a amount měří výskyt a finanční dopad
#    Logika: každý řádek je nová událost, nikdy se nepřepisuje (append only).
#
# 6) premium_transactions (faktová tabulka)
#    Účel: sledovat cash flow z pojistného – kdy a kolik platby.
#    - premium_txn_id jako PK
#    - policy_id odkazuje na stejnou DimPolicy verzi, která platí ke due_date
#    - due_date, premium_amount, paid_flag/payment_date měří plánované a realizované platby
#    Logika: pravidelně generované transakce (např. měsíční), append only.
#
# VZTAHY:
#    agents.snapshot → FactClaim nebo FactPremium (volitelné FK)
#    DimCustomer.customer_id → FactClaim.customer_id (volitelné FK)
#    DimPolicy.policy_id,snapshot_date → FactClaim.policy_id,snapshot_date
#    DimPolicy.policy_id,snapshot_date → FactPremium.policy_id,snapshot_date
#    policies.product_id → products.product_id
#
# POVOLENÉ SCD:
#    SCD2: agents, customers, policies → zachování historie změn stavů
#    SCD1: products → přepisová dimenze, když dojde ke změně popisu produktu
#    Fakta (claims, premium_transactions) → SCD1 (append-only)
#
# Tímto pokrýváme:
#  - DimAgent/DimCustomer/DimPolicy s historií → umožní analýzu trendů a chování
#  - DimProduct jako referenční katalog
#  - FactClaim a FactPremium jako transakční fakty pro reporting výkonu a cash flow
