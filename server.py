#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dashboard RD Station — versão estável para Render

Melhorias:
- sobe rápido no Render
- endpoint /healthz
- cache em memória
- menor concorrência nas chamadas RD
- respostas JSON consistentes
- ThreadingHTTPServer

Configure no Render:
- Start command: python3 server.py

Variáveis de ambiente:
- PORT   -> definida automaticamente pelo Render
- RD_TOKEN -> token da API RD Station
"""

import os
import json
import time
import urllib.request
import urllib.parse
import datetime
import traceback
import threading

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# CONFIG
# =========================
PORT = int(os.environ.get("PORT", "10000"))
HOST = "0.0.0.0"

TOKEN = os.environ.get("RD_TOKEN", "68c30c8a73e14f0019be70b1")
BASE = "https://crm.rdstation.com/api/v1"

REQUEST_TIMEOUT = 15
REQUEST_RETRIES = 1
MAX_WORKERS = 8
CACHE_TTL_SECONDS = 300

FUNIS = {
    "rp": {
        "id": "68a714f1b3f7b8001c750c18",
        "nome": "Funil Comercial RP",
        "etapas": [
            {"id": "68c0589be520d500198b8beb", "nome": "Assinatura eletrônica", "cor": "#f0a830"},
            {"id": "68dd780d1359390014d37c2b", "nome": "Fazendo estimativa", "cor": "#a78bfa"},
            {"id": "68dd781197fb9700276860f7", "nome": "Preparando PDF", "cor": "#2dd4bf"},
            {"id": "68c058a8905a480021f2a1e9", "nome": "Apresentar", "cor": "#3ecf8e"},
            {"id": "699f22a804f22c001ec7cb5d", "nome": "PRFB", "cor": "#fb923c"},
            {"id": "69aed6bcd8e658001e6773bb", "nome": "C4", "cor": "#4f8fff"},
        ],
        "ok_stage_id": "68d99bd829688b00193d8962",
        "contrato_stage_id": "68a714f1b3f7b8001c750c1e",
        "assin_stage_id": "68c0589be520d500198b8beb",
        "prfb_stage_id": "699f22a804f22c001ec7cb5d",
        "pre_contrato_nomes": ["desenvolvimento", "tem perfil"],
    },
    "rrr": {
        "id": "693873d32abcdb001f8409c3",
        "nome": "Funil Comercial RRR Mãe",
        "etapas": [
            {"id": "693874dfb6be4c0015bf64d3", "nome": "Assinatura eletrônica", "cor": "#f0a830"},
            {"id": "6938750379e7eb001d47db46", "nome": "Fazendo estimativa", "cor": "#a78bfa"},
            {"id": "69387510ddb6b40022af1b53", "nome": "Preparando PDF", "cor": "#2dd4bf"},
            {"id": "6938751f576c0000134edfe6", "nome": "Apresentar", "cor": "#3ecf8e"},
            {"id": "69a6e61733a3ff00206a5e8d", "nome": "PRFB", "cor": "#fb923c"},
            {"id": "69aedc182221780020823bab", "nome": "C4", "cor": "#4f8fff"},
        ],
        "ok_stage_id": "6938752561fe57001f7540f5",
        "contrato_stage_id": "693873d32abcdb001f8409c6",
        "assin_stage_id": "693874dfb6be4c0015bf64d3",
        "prfb_stage_id": "69a6e61733a3ff00206a5e8d",
        "pre_contrato_nomes": ["desenvolvimento", "tem perfil"],
    },
}

CACHE = {}
CACHE_LOCK = threading.Lock()


# =========================
# CACHE
# =========================
def cache_get(key):
    with CACHE_LOCK:
        item = CACHE.get(key)
        if not item:
            return None
        expires_at, value = item
        if time.time() > expires_at:
            CACHE.pop(key, None)
            return None
        return value


def cache_set(key, value, ttl=CACHE_TTL_SECONDS):
    with CACHE_LOCK:
        CACHE[key] = (time.time() + ttl, value)


# =========================
# HELPERS
# =========================
def rd_get(path, retries=REQUEST_RETRIES):
    sep = "&" if "?" in path else "?"
    url = f"{BASE}{path}{sep}token={TOKEN}"

    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "dashboard-nobs/1.0"
        }
    )

    last_err = None
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as response:
                raw = response.read().decode("utf-8", errors="replace")
                return json.loads(raw)
        except Exception as exc:
            last_err = exc
            if attempt < retries:
                time.sleep(1)
    raise last_err


def fetch_all(pipeline_id, stage_id, extra=""):
    deals = []
    page = 1

    while True:
        data = rd_get(
            f"/deals?deal_pipeline_id={pipeline_id}&deal_stage_id={stage_id}&limit=200&page={page}{extra}"
        )
        batch = data.get("deals") or []
        deals.extend(batch)

        if len(batch) < 200:
            break
        page += 1

    return deals


def fetch_pipeline_stages(pipeline_id):
    try:
        data = rd_get(f"/deal_pipelines/{pipeline_id}")
        return data.get("deal_stages") or []
    except Exception:
        return []


def fetch_deals_by_stage_name(pipeline_id, stage_name_lower):
    stages = fetch_pipeline_stages(pipeline_id)

    for stage in stages:
        name = (stage.get("name") or "").lower()
        if stage_name_lower in name:
            sid = stage.get("_id") or stage.get("id")
            if sid:
                try:
                    deals = fetch_all(pipeline_id, sid)
                    return sid, deals
                except Exception:
                    return sid, []

    try:
        all_deals = []
        page = 1
        while True:
            data = rd_get(f"/deals?deal_pipeline_id={pipeline_id}&limit=200&page={page}")
            batch = data.get("deals") or []

            matched = [
                x for x in batch
                if stage_name_lower in (((x.get("deal_stage") or {}).get("name") or "").lower())
                and x.get("win") is None
            ]
            all_deals.extend(matched)

            if len(batch) < 200:
                break
            page += 1

        return None, all_deals
    except Exception:
        return None, []


def fetch_stage_active(pipeline_id, stage_id):
    return [x for x in fetch_all(pipeline_id, stage_id) if x.get("win") is None]


def fetch_ok_stage(pipeline_id, stage_id):
    return fetch_all(pipeline_id, stage_id)


def fetch_lost_stage(pipeline_id, stage_id):
    return fetch_all(pipeline_id, stage_id, "&win=false")


def parse_dt(value):
    if not value:
        return None
    try:
        return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def in_month(deal, month, year, field="updated_at"):
    dt = parse_dt(deal.get(field) or "")
    if not dt:
        return False
    return dt.month == month and dt.year == year


def user_name(deal):
    user = deal.get("user")
    if isinstance(user, dict):
        return user.get("name") or "desconhecido"
    return user or "desconhecido"


def get_origem(deal):
    for cf in (deal.get("deal_custom_fields") or []):
        label = ((cf.get("custom_field") or {}).get("label") or "").lower()
        if "origem" in label:
            return (cf.get("value") or "").strip()
    return ""


def get_fonte(deal):
    for key in ("fonte", "source"):
        value = (deal.get(key) or "").strip()
        if value:
            return value

    ds = deal.get("deal_source")
    if isinstance(ds, dict):
        return (ds.get("name") or ds.get("fonte") or ds.get("source") or "").strip()

    for cf in (deal.get("deal_custom_fields") or []):
        label = ((cf.get("custom_field") or {}).get("label") or "").lower()
        if "fonte" in label:
            return (cf.get("value") or "").strip()

    return ""


def is_busca_paga(deal):
    origem = get_origem(deal).lower()
    fonte = get_fonte(deal).lower()
    return origem.startswith("busca paga") or fonte.startswith("busca paga")


def get_custom_date(deal, label_substring):
    for cf in (deal.get("deal_custom_fields") or []):
        label = ((cf.get("custom_field") or {}).get("label") or "").lower()
        if label_substring.lower() in label:
            value = (cf.get("value") or "").strip()
            if not value:
                return ""
            if len(value) >= 10 and len(value) > 5 and value[2] == "/" and value[5] == "/":
                day, month, year = value[:2], value[3:5], value[6:10]
                return f"{year}-{month}-{day}"
            return value[:10]
    return ""


def fmt_custom_date(iso_date):
    if not iso_date or len(iso_date) < 10:
        return iso_date or "--"
    try:
        year, month, day = iso_date[:4], iso_date[5:7], iso_date[8:10]
        return f"{day}/{month}/{year}"
    except Exception:
        return iso_date


def custom_date_in_month(deal, label_substring, month, year):
    value = get_custom_date(deal, label_substring)
    if not value:
        return False
    try:
        dt = datetime.date.fromisoformat(value)
        return dt.month == month and dt.year == year
    except Exception:
        return False


def custom_date_equals(deal, label_substring, date_str):
    value = get_custom_date(deal, label_substring)
    if not value:
        return False
    return value == date_str


def empty_payload(error_message=""):
    return {
        "error": error_message,
        "etapas": [],
        "vendas": [],
        "contratos_mes": [],
        "em_andamento": [],
        "perdas": [],
        "feed": [],
        "vendas_busca_paga": 0,
        "contratos_busca_paga": 0,
        "assinaturas_mes": [],
        "contrato_d1": [],
        "contrato_hoje": [],
        "prfb_ativos": 0,
    }


def slim_deal(deal):
    ds = deal.get("deal_stage")
    dlr = deal.get("deal_lost_reason")
    data_contrato = get_custom_date(deal, "Data do contrato")
    data_assinatura = get_custom_date(deal, "Data da assinatura")

    return {
        "name": deal.get("name") or "",
        "user": user_name(deal),
        "updated_at": deal.get("updated_at") or "",
        "closed_at": deal.get("closed_at") or "",
        "amount_total": deal.get("amount_total") or 0,
        "origem": get_origem(deal),
        "fonte": get_fonte(deal),
        "deal_stage": {"name": ds.get("name") or ""} if isinstance(ds, dict) else None,
        "deal_lost_reason": {"name": dlr.get("name") or ""} if isinstance(dlr, dict) else None,
        "data_contrato": data_contrato,
        "data_assinatura": data_assinatura,
        "data_contrato_fmt": fmt_custom_date(data_contrato),
        "data_assinatura_fmt": fmt_custom_date(data_assinatura),
    }


def slim_d1(deal):
    dc = get_custom_date(deal, "Data do contrato")
    da = get_custom_date(deal, "Data da assinatura")
    return {
        "name": deal.get("name") or "",
        "user": user_name(deal),
        "data_contrato": dc,
        "data_assinatura": da,
        "data_contrato_fmt": fmt_custom_date(dc),
        "data_assinatura_fmt": fmt_custom_date(da),
        "current_stage": (deal.get("deal_stage") or {}).get("name") or "",
        "updated_at": deal.get("updated_at") or "",
    }


# =========================
# CORE
# =========================
def load_funil_data(key, month, year):
    cache_key = f"{key}:{month}:{year}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    if key not in FUNIS:
        return empty_payload("Funil inválido")

    funil = FUNIS[key]
    pid = funil["id"]

    tasks = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for etapa in funil["etapas"]:
            tasks[executor.submit(fetch_stage_active, pid, etapa["id"])] = ("active", etapa)

        tasks[executor.submit(fetch_ok_stage, pid, funil["ok_stage_id"])] = ("ok", None)

        for etapa in funil["etapas"]:
            tasks[executor.submit(fetch_lost_stage, pid, etapa["id"])] = ("lost", etapa)

        all_post_stage_ids = [etapa["id"] for etapa in funil["etapas"]] + [funil["contrato_stage_id"]]
        for sid in all_post_stage_ids:
            tasks[executor.submit(fetch_all, pid, sid)] = ("postcontrato_all", sid)

        for nome_lower in funil.get("pre_contrato_nomes", []):
            tasks[executor.submit(fetch_deals_by_stage_name, pid, nome_lower)] = ("pre_contrato", nome_lower)

        etapas_map = {etapa["id"]: {**etapa, "deals": []} for etapa in funil["etapas"]}
        ok_deals = []
        todas_perdas = []
        postcontrato_pool = {}
        pre_contrato_map = {}

        for future in as_completed(tasks, timeout=90):
            kind, meta = tasks[future]
            try:
                result = future.result()
            except Exception:
                result = [] if kind != "pre_contrato" else (None, [])

            if kind == "active":
                etapas_map[meta["id"]]["deals"] = result

            elif kind == "ok":
                ok_deals = result

            elif kind == "lost":
                todas_perdas.extend(result)

            elif kind == "postcontrato_all":
                sid = meta
                postcontrato_pool.setdefault(sid, []).extend(result)

            elif kind == "pre_contrato":
                nome_lower = meta
                sid_result, deals_result = result
                pre_contrato_map[nome_lower] = {"sid": sid_result, "deals": deals_result}

    assin_stage_id = funil.get("assin_stage_id", "")

    etapas_data = []
    for etapa in funil["etapas"]:
        all_active = etapas_map[etapa["id"]]["deals"]
        if etapa["id"] == assin_stage_id:
            deals_mes = [
                d for d in all_active
                if custom_date_in_month(d, "Data da assinatura", month, year)
            ]
        else:
            deals_mes = [d for d in all_active if in_month(d, month, year, "updated_at")]

        etapas_data.append({**etapa, "deals": deals_mes})

    vendas_mes = [
        d for d in ok_deals
        if d.get("closed_at") and in_month(d, month, year, "closed_at")
    ]

    all_postcontrato_deals = []
    for sid, deals in postcontrato_pool.items():
        all_postcontrato_deals.extend(deals)
    all_postcontrato_deals.extend(ok_deals)

    seen_contrato = set()
    contratos_mes = []
    for deal in all_postcontrato_deals:
        if not custom_date_in_month(deal, "Data do contrato", month, year):
            continue
        did = deal.get("_id") or deal.get("id")
        if did and did not in seen_contrato:
            seen_contrato.add(did)
            contratos_mes.append(deal)

    em_andamento = []
    seen_ea = set()
    for nome_lower, info in pre_contrato_map.items():
        deals = info.get("deals") or []
        for deal in deals:
            if not in_month(deal, month, year, "updated_at"):
                continue
            if deal.get("win") is not None:
                continue

            did = deal.get("_id") or deal.get("id")
            if did and did not in seen_ea:
                seen_ea.add(did)
                stage_name = (deal.get("deal_stage") or {}).get("name") or nome_lower
                em_andamento.append({**deal, "_pre_stage": stage_name})

    seen_assin = set()
    assinaturas_mes = []
    for deal in all_postcontrato_deals:
        if not custom_date_in_month(deal, "Data da assinatura", month, year):
            continue
        did = deal.get("_id") or deal.get("id")
        if did and did not in seen_assin:
            seen_assin.add(did)
            assinaturas_mes.append(deal)

    vendas_busca_paga = [d for d in vendas_mes if is_busca_paga(d)]
    contratos_busca_paga = [d for d in contratos_mes if is_busca_paga(d)]

    feed_candidates = []

    for etapa in etapas_data:
        for deal in etapa["deals"]:
            tipo = "contrato" if etapa["id"] == funil["contrato_stage_id"] else "etapa"
            feed_candidates.append({
                "nome": deal.get("name") or "desconhecido",
                "user": user_name(deal),
                "tipo": tipo,
                "etapa": etapa["nome"],
                "cor": etapa["cor"],
                "ts": deal.get("updated_at") or deal.get("created_at") or "",
                "funil": funil["nome"],
            })

    for deal in ok_deals:
        feed_candidates.append({
            "nome": deal.get("name") or "desconhecido",
            "user": user_name(deal),
            "tipo": "venda",
            "etapa": "Vendida",
            "cor": "#3ecf8e",
            "ts": deal.get("closed_at") or deal.get("updated_at") or "",
            "funil": funil["nome"],
        })

    for deal in todas_perdas:
        ds = deal.get("deal_stage")
        stage_name = (ds.get("name") or "") if isinstance(ds, dict) else ""
        feed_candidates.append({
            "nome": deal.get("name") or "desconhecido",
            "user": user_name(deal),
            "tipo": "perda",
            "etapa": stage_name or "desconhecida",
            "cor": "#f06060",
            "ts": deal.get("updated_at") or deal.get("closed_at") or "",
            "funil": funil["nome"],
        })

    feed_candidates.sort(key=lambda x: x["ts"], reverse=True)

    today = datetime.date.today()
    weekday = today.weekday()
    if weekday == 0:
        prev_wd = today - datetime.timedelta(days=3)
    elif weekday == 6:
        prev_wd = today - datetime.timedelta(days=2)
    else:
        prev_wd = today - datetime.timedelta(days=1)

    prev_wd_str = str(prev_wd)
    today_str = str(today)

    unique_deals = list({
        (d.get("_id") or d.get("id")): d
        for d in all_postcontrato_deals
    }.values())

    contrato_d1 = [
        slim_d1(d) for d in unique_deals
        if custom_date_equals(d, "Data do contrato", prev_wd_str)
        and not get_custom_date(d, "Data da assinatura")
    ]

    contrato_hoje = [
        slim_d1(d) for d in unique_deals
        if custom_date_equals(d, "Data do contrato", today_str)
        and not get_custom_date(d, "Data da assinatura")
    ]

    prfb_stage_id = funil.get("prfb_stage_id", "")
    prfb_ativos = []
    if prfb_stage_id and prfb_stage_id in etapas_map:
        prfb_ativos = [
            d for d in etapas_map[prfb_stage_id]["deals"]
            if d.get("win") is None
        ]
    elif prfb_stage_id:
        try:
            prfb_ativos = fetch_stage_active(pid, prfb_stage_id)
        except Exception:
            prfb_ativos = []

    payload = {
        "error": "",
        "etapas": [{**e, "deals": [slim_deal(d) for d in e["deals"]]} for e in etapas_data],
        "vendas": [slim_deal(d) for d in vendas_mes],
        "contratos_mes": [slim_deal(d) for d in contratos_mes],
        "em_andamento": [slim_deal(d) for d in em_andamento],
        "perdas": [slim_deal(d) for d in todas_perdas],
        "feed": feed_candidates[:60],
        "vendas_busca_paga": len(vendas_busca_paga),
        "contratos_busca_paga": len(contratos_busca_paga),
        "assinaturas_mes": [slim_deal(d) for d in assinaturas_mes],
        "contrato_d1": contrato_d1,
        "contrato_hoje": contrato_hoje,
        "prfb_ativos": len(prfb_ativos),
    }

    cache_set(cache_key, payload)
    return payload


# =========================
# HTML
# =========================
HTML = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width,initial-scale=1.0" />
<title>Dashboard Comercial</title>
<style>
:root{
  --bg:#0f1115;--card:#181c23;--muted:#9aa4b2;--text:#f5f7fa;
  --border:#2a3140;--blue:#4f8fff;--green:#3ecf8e;--red:#f06060;--amber:#f0a830;
}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);font-family:Arial,sans-serif}
header{padding:20px;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap}
h1{font-size:22px;margin:0}
small{color:var(--muted)}
main{padding:20px;max-width:1200px;margin:0 auto}
.controls{display:flex;gap:8px;flex-wrap:wrap}
button,select{
  background:#11161d;color:var(--text);border:1px solid var(--border);
  border-radius:8px;padding:10px 14px;cursor:pointer
}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;margin:18px 0}
.card{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:16px}
.label{font-size:12px;color:var(--muted);text-transform:uppercase;margin-bottom:8px}
.value{font-size:34px;font-weight:700}
.blue{color:var(--blue)} .green{color:var(--green)} .red{color:var(--red)} .amber{color:var(--amber)}
.row{display:grid;grid-template-columns:1fr;gap:12px}
pre{
  white-space:pre-wrap;word-break:break-word;background:#11161d;border:1px solid var(--border);
  padding:12px;border-radius:10px;overflow:auto
}
.status{margin-top:8px;color:var(--muted)}
.tabs{display:flex;gap:8px;margin:16px 0;flex-wrap:wrap}
.tab.active{border-color:var(--blue);color:var(--blue)}
.table-wrap{overflow:auto}
table{width:100%;border-collapse:collapse}
th,td{padding:10px;border-bottom:1px solid var(--border);text-align:left}
th{color:var(--muted);font-size:12px;text-transform:uppercase}
.error{background:rgba(240,96,96,.12);border:1px solid rgba(240,96,96,.35);padding:14px;border-radius:10px;color:#ffb1b1}
</style>
</head>
<body>
<header>
  <div>
    <h1>Dashboard Comercial</h1>
    <small>RD Station CRM</small>
  </div>
  <div class="controls">
    <select id="funil">
      <option value="rp">Funil Comercial RP</option>
      <option value="rrr">Funil Comercial RRR Mãe</option>
    </select>
    <select id="month"></select>
    <select id="year"></select>
    <button onclick="loadData()">Atualizar</button>
  </div>
</header>

<main>
  <div id="status" class="status">Carregando...</div>
  <div id="error"></div>
  <div id="cards" class="grid"></div>
  <div class="row">
    <div class="card">
      <div class="label">Vendas</div>
      <div class="table-wrap"><table id="tbl-vendas"></table></div>
    </div>
    <div class="card">
      <div class="label">Contratos do mês</div>
      <div class="table-wrap"><table id="tbl-contratos"></table></div>
    </div>
    <div class="card">
      <div class="label">Feed</div>
      <pre id="feed"></pre>
    </div>
  </div>
</main>

<script>
const statusEl = document.getElementById("status");
const errorEl = document.getElementById("error");
const cardsEl = document.getElementById("cards");
const feedEl = document.getElementById("feed");

function pad(n){ return String(n).padStart(2,"0"); }

function fillPeriods(){
  const month = document.getElementById("month");
  const year = document.getElementById("year");
  const now = new Date();

  for(let m=1;m<=12;m++){
    const opt = document.createElement("option");
    opt.value = m;
    opt.textContent = pad(m);
    if(m === now.getMonth() + 1) opt.selected = true;
    month.appendChild(opt);
  }

  for(let y=2025;y<=2027;y++){
    const opt = document.createElement("option");
    opt.value = y;
    opt.textContent = y;
    if(y === now.getFullYear()) opt.selected = true;
    year.appendChild(opt);
  }
}

function money(v){
  const n = Number(v || 0);
  return n.toLocaleString("pt-BR", {minimumFractionDigits:2, maximumFractionDigits:2});
}

function renderTable(elId, rows, kind){
  const el = document.getElementById(elId);
  if(!rows || !rows.length){
    el.innerHTML = "<tr><td>Sem dados</td></tr>";
    return;
  }

  let html = "";
  if(kind === "vendas"){
    html += "<tr><th>Nome</th><th>Responsável</th><th>Fechado</th><th>Valor</th></tr>";
    html += rows.map(r => `
      <tr>
        <td>${r.name || "--"}</td>
        <td>${r.user || "--"}</td>
        <td>${r.closed_at || "--"}</td>
        <td>${money(r.amount_total)}</td>
      </tr>
    `).join("");
  } else {
    html += "<tr><th>Nome</th><th>Responsável</th><th>Contrato</th></tr>";
    html += rows.map(r => `
      <tr>
        <td>${r.name || "--"}</td>
        <td>${r.user || "--"}</td>
        <td>${r.data_contrato_fmt || "--"}</td>
      </tr>
    `).join("");
  }

  el.innerHTML = html;
}

function renderCards(data){
  const vendas = (data.vendas || []).length;
  const contratos = (data.contratos_mes || []).length;
  const andamento = (data.em_andamento || []).length;
  const perdas = (data.perdas || []).length;
  const assinaturas = (data.assinaturas_mes || []).length;
  const prfb = Number(data.prfb_ativos || 0);

  cardsEl.innerHTML = `
    <div class="card"><div class="label">Em andamento</div><div class="value blue">${andamento}</div></div>
    <div class="card"><div class="label">Contratos do mês</div><div class="value blue">${contratos}</div></div>
    <div class="card"><div class="label">Assinaturas do mês</div><div class="value amber">${assinaturas}</div></div>
    <div class="card"><div class="label">Vendas do mês</div><div class="value green">${vendas}</div></div>
    <div class="card"><div class="label">Perdas</div><div class="value red">${perdas}</div></div>
    <div class="card"><div class="label">PRFB ativos</div><div class="value">${prfb}</div></div>
  `;
}

function renderFeed(feed){
  if(!feed || !feed.length){
    feedEl.textContent = "Sem movimentações.";
    return;
  }

  feedEl.textContent = feed.map(item => {
    return `${item.ts || "--"} | ${item.funil || "--"} | ${item.user || "--"} | ${item.tipo || "--"} | ${item.nome || "--"} | ${item.etapa || "--"}`;
  }).join("\\n");
}

async function loadData(){
  errorEl.innerHTML = "";
  statusEl.textContent = "Carregando...";

  const funil = document.getElementById("funil").value;
  const month = document.getElementById("month").value;
  const year = document.getElementById("year").value;

  try{
    const res = await fetch(`/api/data?funil=${encodeURIComponent(funil)}&month=${encodeURIComponent(month)}&year=${encodeURIComponent(year)}`);
    const data = await res.json();

    if(!res.ok || data.error){
      throw new Error(data.error || `HTTP ${res.status}`);
    }

    renderCards(data);
    renderTable("tbl-vendas", data.vendas || [], "vendas");
    renderTable("tbl-contratos", data.contratos_mes || [], "contratos");
    renderFeed(data.feed || []);
    statusEl.textContent = "Atualizado com sucesso.";
  } catch(err){
    statusEl.textContent = "Erro ao carregar.";
    errorEl.innerHTML = `<div class="error"><strong>Erro:</strong> ${err.message}</div>`;
    cardsEl.innerHTML = "";
    document.getElementById("tbl-vendas").innerHTML = "";
    document.getElementById("tbl-contratos").innerHTML = "";
    feedEl.textContent = "";
  }
}

fillPeriods();
loadData();
</script>
</body>
</html>
"""


# =========================
# HTTP
# =========================
class Handler(BaseHTTPRequestHandler):
    server_version = "DashboardNobs/1.0"

    def log_message(self, fmt, *args):
        print(f"[{self.address_string()}] {fmt % args}")

    def send_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def send_html(self, html, status=200):
        body = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        qs = dict(urllib.parse.parse_qsl(parsed.query))

        try:
            if parsed.path == "/":
                self.send_html(HTML)
                return

            if parsed.path == "/healthz":
                self.send_json({"ok": True, "status": "healthy", "time": datetime.datetime.utcnow().isoformat() + "Z"})
                return

            if parsed.path == "/api/ping":
                self.send_json({"ok": True})
                return

            if parsed.path == "/api/data":
                key = qs.get("funil", "rp")
                now = datetime.datetime.now()
                month = int(qs.get("month", now.month))
                year = int(qs.get("year", now.year))

                if key not in FUNIS:
                    self.send_json(empty_payload("Funil inválido"), 400)
                    return

                data = load_funil_data(key, month, year)
                if data.get("error"):
                    self.send_json(data, 500)
                else:
                    self.send_json(data, 200)
                return

            self.send_json({"error": "Rota não encontrada"}, 404)

        except Exception as exc:
            traceback.print_exc()
            self.send_json({"error": str(exc)}, 500)


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    print("=" * 60)
    print("Dashboard Comercial — RD Station CRM")
    print("=" * 60)
    print(f"HOST: {HOST}")
    print(f"PORT: {PORT}")
    print(f"RD_TOKEN configurado: {'sim' if TOKEN else 'não'}")
    print("Subindo servidor...")

    httpd = ThreadingHTTPServer((HOST, PORT), Handler)

    try:
        print(f"Servidor ouvindo em http://{HOST}:{PORT}")
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("Encerrado manualmente.")
    finally:
        httpd.server_close()
