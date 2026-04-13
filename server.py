#!/usr/bin/env python3
"""
Dashboard RD Station — Servidor web (Render)
Rode com: python3 server.py
Porta definida pela variavel de ambiente PORT (padrao 8765)
"""
import json
import urllib.request
import urllib.parse
import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from concurrent.futures import ThreadPoolExecutor, as_completed

TOKEN = "68c30c8a73e14f0019be70b1"
BASE  = "https://crm.rdstation.com/api/v1"

FUNIS = {
    "rp": {
        "id":   "68a714f1b3f7b8001c750c18",
        "nome": "Funil Comercial RP",
        "etapas": [
            {"id": "68a714f1b3f7b8001c750c1e", "nome": "Contrato enviado",     "cor": "#4f8fff"},
            {"id": "68c0589be520d500198b8beb", "nome": "Assinatura eletronica", "cor": "#f0a830"},
            {"id": "68dd780d1359390014d37c2b", "nome": "Fazendo estimativa",    "cor": "#a78bfa"},
            {"id": "68dd781197fb9700276860f7", "nome": "Preparando PDF",        "cor": "#2dd4bf"},
            {"id": "68c058a8905a480021f2a1e9", "nome": "Apresentar",            "cor": "#3ecf8e"},
            {"id": "699f22a804f22c001ec7cb5d", "nome": "PRFB",                  "cor": "#fb923c"},
            {"id": "69aed6bcd8e658001e6773bb", "nome": "C4",                    "cor": "#4f8fff"},
        ],
        "ok_stage_id":      "68d99bd829688b00193d8962",
        "contrato_stage_id":"68a714f1b3f7b8001c750c1e",
    },
    "rrr": {
        "id":   "693873d32abcdb001f8409c3",
        "nome": "Funil Comercial RRR Mae",
        "etapas": [
            {"id": "693873d32abcdb001f8409c6", "nome": "Contrato enviado",     "cor": "#4f8fff"},
            {"id": "693874dfb6be4c0015bf64d3", "nome": "Assinatura eletronica","cor": "#f0a830"},
            {"id": "6938750379e7eb001d47db46", "nome": "Fazendo estimativa",   "cor": "#a78bfa"},
            {"id": "69387510ddb6b40022af1b53", "nome": "Preparando PDF",       "cor": "#2dd4bf"},
            {"id": "6938751f576c0000134edfe6", "nome": "Apresentar",           "cor": "#3ecf8e"},
            {"id": "69a6e61733a3ff00206a5e8d", "nome": "PRFB",                 "cor": "#fb923c"},
            {"id": "69aedc182221780020823bab", "nome": "C4",                   "cor": "#4f8fff"},
        ],
        "ok_stage_id":      "6938752561fe57001f7540f5",
        "contrato_stage_id":"693873d32abcdb001f8409c6",
    }
}

# ── helpers ───────────────────────────────────────────────────────────────────
def rd_get(path):
    sep = "&" if "?" in path else "?"
    url = f"{BASE}{path}{sep}token={TOKEN}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())

def fetch_all(pipeline_id, stage_id, extra=""):
    deals, page = [], 1
    while True:
        d = rd_get(f"/deals?deal_pipeline_id={pipeline_id}&deal_stage_id={stage_id}&limit=200&page={page}{extra}")
        batch = d.get("deals") or []
        deals.extend(batch)
        if len(batch) < 200:
            break
        page += 1
    return deals

def fetch_stage_active(pipeline_id, stage_id):
    return [x for x in fetch_all(pipeline_id, stage_id) if x.get("win") is None]

def fetch_ok_stage(pipeline_id, stage_id):
    return fetch_all(pipeline_id, stage_id)

def fetch_lost_stage(pipeline_id, stage_id):
    return fetch_all(pipeline_id, stage_id, "&win=false")

def parse_dt(val):
    if not val:
        return None
    try:
        return datetime.datetime.fromisoformat(val.replace("Z", "+00:00"))
    except Exception:
        return None

def in_month(deal, month, year, field="updated_at"):
    dt = parse_dt(deal.get(field) or "")
    if not dt:
        return False
    return dt.month == month and dt.year == year

def user_name(deal):
    u = deal.get("user")
    if isinstance(u, dict):
        return u.get("name") or "desconhecido"
    return u or "desconhecido"

def get_origem(deal):
    """Retorna valor do campo custom 'Origem especifica'."""
    for cf in (deal.get("deal_custom_fields") or []):
        lbl = (cf.get("custom_field") or {}).get("label", "")
        if "origem" in lbl.lower():
            return (cf.get("value") or "").strip()
    return ""

def is_busca_paga(deal):
    return "busca" in get_origem(deal).lower()

# ── main loader ───────────────────────────────────────────────────────────────
def load_funil_data(key, month, year):
    funil = FUNIS[key]
    pid   = funil["id"]

    tasks = {}
    with ThreadPoolExecutor(max_workers=16) as ex:
        for e in funil["etapas"]:
            tasks[ex.submit(fetch_stage_active, pid, e["id"])] = ("active", e)
        tasks[ex.submit(fetch_ok_stage,   pid, funil["ok_stage_id"])]       = ("ok",   None)
        tasks[ex.submit(fetch_ok_stage,   pid, funil["contrato_stage_id"])] = ("contrato_all", None)
        for e in funil["etapas"]:
            tasks[ex.submit(fetch_lost_stage, pid, e["id"])] = ("lost", e)

    etapas_map   = {e["id"]: {**e, "deals": []} for e in funil["etapas"]}
    ok_deals     = []
    contrato_all = []
    todas_perdas = []

    for fut in as_completed(tasks):
        kind, meta = tasks[fut]
        result = fut.result()
        if kind == "active":
            etapas_map[meta["id"]]["deals"] = result
        elif kind == "ok":
            ok_deals = result
        elif kind == "contrato_all":
            contrato_all = result
        elif kind == "lost":
            todas_perdas.extend(result)

    # FIX 1: filtrar etapas ativas pelo mes selecionado usando created_at
    # created_at = quando o deal foi criado/chegou ao funil — proxy correto para mes de entrada
    etapas_data = []
    for e in funil["etapas"]:
        all_active = etapas_map[e["id"]]["deals"]
        mes_active = [d for d in all_active if in_month(d, month, year, "created_at")]
        etapas_data.append({**e, "deals": mes_active})

    # vendas do mes (pelo closed_at)
    vendas_mes = [d for d in ok_deals
                  if d.get("closed_at") and in_month(d, month, year, "closed_at")]

    # FIX 2: contratos do mes — deals criados no mes que passaram pela etapa de contrato
    # ativos na etapa agora com created_at no mes + ganhos/perdidos com created_at no mes
    contrato_active_mes = [d for d in etapas_map[funil["contrato_stage_id"]]["deals"]
                           if in_month(d, month, year, "created_at")]
    contrato_ok_mes     = [d for d in contrato_all
                           if in_month(d, month, year, "created_at")]
    # deduplicar por _id
    seen = set()
    contratos_mes = []
    for d in contrato_active_mes + contrato_ok_mes:
        did = d.get("_id") or d.get("id")
        if did not in seen:
            seen.add(did)
            contratos_mes.append(d)

    # FIX 3: vendas e contratos por busca paga
    vendas_busca_paga    = [d for d in vendas_mes if is_busca_paga(d)]
    contratos_busca_paga = [d for d in contratos_mes if is_busca_paga(d)]

    # ── feed de movimentacoes recentes ────────────────────────────────────────
    feed_candidates = []
    for e in etapas_data:
        for d in e["deals"]:
            tipo = "contrato" if e["id"] == funil["contrato_stage_id"] else "etapa"
            feed_candidates.append({
                "nome":  d.get("name") or "desconhecido",
                "user":  user_name(d),
                "tipo":  tipo,
                "etapa": e["nome"],
                "cor":   e["cor"],
                "ts":    d.get("updated_at") or d.get("created_at") or "",
                "funil": funil["nome"],
            })
    for d in ok_deals:
        feed_candidates.append({
            "nome":  d.get("name") or "desconhecido",
            "user":  user_name(d),
            "tipo":  "venda",
            "etapa": "Vendida",
            "cor":   "#3ecf8e",
            "ts":    d.get("closed_at") or d.get("updated_at") or "",
            "funil": funil["nome"],
        })
    for d in todas_perdas:
        ds = d.get("deal_stage")
        stage_name = (ds.get("name") or "") if isinstance(ds, dict) else ""
        feed_candidates.append({
            "nome":  d.get("name") or "desconhecido",
            "user":  user_name(d),
            "tipo":  "perda",
            "etapa": stage_name or "desconhecida",
            "cor":   "#f06060",
            "ts":    d.get("updated_at") or d.get("closed_at") or "",
            "funil": funil["nome"],
        })
    feed_candidates.sort(key=lambda x: x["ts"], reverse=True)

    # FIX 4: valor total por responsavel (amount_total)
    # serializar deals com amount_total e origem para uso no front
    def slim(d, extra_fields=None):
        """Versao reduzida do deal para serializar no JSON."""
        out = {
            "name":         d.get("name") or "",
            "user":         user_name(d),
            "updated_at":   d.get("updated_at") or "",
            "closed_at":    d.get("closed_at") or "",
            "amount_total": d.get("amount_total") or 0,
            "origem":       get_origem(d),
        }
        return out

    # D+1 e Hoje: todos os deals ATIVOS na etapa "Contrato enviado" agora (sem filtro de mes)
    # o front filtra por updated_at: ontem = D+1, hoje = movidos hoje
    cid = funil["contrato_stage_id"]
    contrato_ativos_todos = etapas_map[cid]["deals"]  # ja sao win=None
    contrato_ativos_slim = [
        {
            "name":       d.get("name") or "",
            "user":       user_name(d),
            "updated_at": d.get("updated_at") or "",
            "created_at": d.get("created_at") or "",
        }
        for d in contrato_ativos_todos
    ]

    return {
        "etapas":               [{**e, "deals": [slim(d) for d in e["deals"]]} for e in etapas_data],
        "vendas":               [slim(d) for d in vendas_mes],
        "contratos_mes":        [slim(d) for d in contratos_mes],
        "perdas":               [slim(d) for d in todas_perdas],
        "feed":                 feed_candidates[:60],
        "vendas_busca_paga":    len(vendas_busca_paga),
        "contratos_busca_paga": len(contratos_busca_paga),
        "contrato_ativos":      contrato_ativos_slim,
    }


# ── HTML ──────────────────────────────────────────────────────────────────────
HTML = r"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Dashboard Comercial</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Syne:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
:root{--bg:#0d0d0f;--surface:#141416;--surface2:#1a1a1e;--border:rgba(255,255,255,.07);--border2:rgba(255,255,255,.12);--text:#f0f0f0;--muted:#666;--dim:#888;--blue:#4f8fff;--blue-dim:rgba(79,143,255,.12);--green:#3ecf8e;--green-dim:rgba(62,207,142,.12);--red:#f06060;--red-dim:rgba(240,96,96,.12);--amber:#f0a830;--amber-dim:rgba(240,168,48,.12);--purple:#a78bfa;--teal:#2dd4bf;--coral:#fb923c;--header-bg:rgba(13,13,15,.95)}
html.light{--bg:#f0f2f5;--surface:#ffffff;--surface2:#e8eaed;--border:rgba(0,0,0,.09);--border2:rgba(0,0,0,.15);--text:#1a1a2e;--muted:#999;--dim:#555;--blue:#2563eb;--blue-dim:rgba(37,99,235,.1);--green:#059669;--green-dim:rgba(5,150,105,.1);--red:#dc2626;--red-dim:rgba(220,38,38,.1);--amber:#d97706;--amber-dim:rgba(217,119,6,.1);--purple:#7c3aed;--teal:#0d9488;--coral:#ea580c;--header-bg:rgba(240,242,245,.95)}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:'Syne',sans-serif;min-height:100vh;line-height:1.5}
header{border-bottom:1px solid var(--border);padding:1.25rem 2rem;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100;background:var(--header-bg);backdrop-filter:blur(12px)}
.logo{display:flex;align-items:center;gap:10px}
.logo-dot{width:8px;height:8px;border-radius:50%;background:var(--green);box-shadow:0 0 8px var(--green);animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
.logo-text{font-size:14px;font-weight:600;letter-spacing:.04em}.logo-sub{font-size:11px;color:var(--muted);margin-top:1px}
.header-right{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.last-update{font-size:11px;color:var(--muted);font-family:'DM Mono',monospace}
.refresh-btn{background:var(--surface2);border:1px solid var(--border2);color:var(--dim);padding:6px 14px;border-radius:6px;font-size:12px;font-family:'Syne',sans-serif;cursor:pointer;display:flex;align-items:center;gap:6px;transition:all .2s}
.refresh-btn:hover{border-color:var(--blue);color:var(--blue)}
.theme-btn{background:var(--surface2);border:1px solid var(--border2);color:var(--dim);padding:6px 11px;border-radius:6px;font-size:15px;cursor:pointer;transition:all .2s;line-height:1}
.theme-btn:hover{border-color:var(--amber);color:var(--amber)}
.period-selector{display:flex;gap:4px}
.period-btn{background:transparent;border:1px solid var(--border);color:var(--muted);padding:5px 12px;border-radius:5px;font-size:11px;font-family:'DM Mono',monospace;cursor:pointer;transition:all .2s}
.period-btn.active,.period-btn:hover{border-color:var(--blue);color:var(--blue);background:var(--blue-dim)}
.next-refresh{font-size:10px;color:var(--muted);font-family:'DM Mono',monospace;padding:4px 8px;border:1px solid var(--border);border-radius:4px;white-space:nowrap}
main{padding:2rem;max-width:1400px;margin:0 auto}
.tabs{display:flex;gap:2px;background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:3px;margin-bottom:2rem;width:fit-content}
.tab-btn{padding:8px 20px;border-radius:8px;font-size:13px;font-weight:600;font-family:'Syne',sans-serif;cursor:pointer;border:none;background:transparent;color:var(--muted);transition:all .2s;letter-spacing:.02em}
.tab-btn.active{background:var(--surface2);color:var(--text);border:1px solid var(--border2)}
.tab-btn.total-tab.active{background:rgba(240,168,48,.15);color:var(--amber);border-color:rgba(240,168,48,.4)}
.summary-grid-5{display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:2rem}
.summary-card{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:1.25rem;position:relative;overflow:hidden}
.summary-card::before{content:'';position:absolute;top:0;left:0;right:0;height:2px}
.summary-card.blue::before{background:var(--blue)}.summary-card.green::before{background:var(--green)}.summary-card.red::before{background:var(--red)}.summary-card.amber::before{background:var(--amber)}.summary-card.purple::before{background:var(--purple)}
.sc-label{font-size:11px;color:var(--muted);letter-spacing:.04em;text-transform:uppercase;margin-bottom:.5rem}
.sc-val{font-size:38px;font-weight:700;line-height:1;margin-bottom:.3rem}
.sc-val.blue{color:var(--blue)}.sc-val.green{color:var(--green)}.sc-val.red{color:var(--red)}.sc-val.amber{color:var(--amber)}.sc-val.purple{color:var(--purple)}
.sc-sub{font-size:11px;color:var(--muted);font-family:'DM Mono',monospace}
.section-hd{display:flex;align-items:center;gap:10px;margin-bottom:1rem}
.section-hd h3{font-size:12px;font-weight:600;letter-spacing:.08em;text-transform:uppercase;color:var(--muted)}
.cnt{font-size:11px;font-family:'DM Mono',monospace;padding:2px 8px;border-radius:4px;border:1px solid}
.cnt.blue{color:var(--blue);border-color:rgba(79,143,255,.3);background:var(--blue-dim)}
.cnt.red{color:var(--red);border-color:rgba(240,96,96,.3);background:var(--red-dim)}
.cnt.green{color:var(--green);border-color:rgba(62,207,142,.3);background:var(--green-dim)}
.cnt.amber{color:var(--amber);border-color:rgba(240,168,48,.3);background:var(--amber-dim)}
.section-line{flex:1;height:1px;background:var(--border)}
/* FEED */
.feed-wrap{background:var(--surface);border:1px solid var(--border);border-radius:12px;overflow:hidden;margin-bottom:2rem}
.feed-hd{padding:.85rem 1.25rem;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:8px}
.feed-hd-title{font-size:12px;font-weight:600;letter-spacing:.06em;text-transform:uppercase;color:var(--muted)}
.feed-live{width:6px;height:6px;border-radius:50%;background:var(--green);box-shadow:0 0 6px var(--green);animation:pulse 2s infinite;margin-left:auto;flex-shrink:0}
.feed-item{display:flex;align-items:flex-start;gap:12px;padding:.7rem 1.25rem;border-bottom:1px solid var(--border)}
.feed-item:last-child{border-bottom:none}
.feed-dot{width:8px;height:8px;border-radius:50%;flex-shrink:0;margin-top:5px}
.feed-body{flex:1;min-width:0}
.feed-deal{font-size:13px;font-weight:600;color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.feed-meta{font-size:11px;color:var(--muted);font-family:'DM Mono',monospace;margin-top:2px;display:flex;align-items:center;gap:4px;flex-wrap:wrap}
.feed-badge{display:inline-block;font-size:10px;font-family:'DM Mono',monospace;padding:1px 7px;border-radius:4px;border:1px solid}
.feed-ts{font-size:10px;color:var(--muted);font-family:'DM Mono',monospace;flex-shrink:0;white-space:nowrap}
/* RESPONSAVEIS */
.resp-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:12px;margin-bottom:2rem}
.resp-card{background:var(--surface);border:1px solid var(--border);border-radius:12px;overflow:hidden}
.resp-header{padding:1rem 1.25rem;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:12px}
.resp-avatar{width:36px;height:36px;border-radius:8px;display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:700;flex-shrink:0}
.resp-name{font-size:14px;font-weight:600}.resp-total{margin-left:auto;font-family:'DM Mono',monospace;font-size:22px;font-weight:500}
.resp-rows{padding:.5rem 0}
.resp-row{display:flex;align-items:center;justify-content:space-between;padding:.4rem 1.25rem}
.resp-row-label{font-size:12px;color:var(--dim);display:flex;align-items:center;gap:8px}
.resp-row-dot{width:6px;height:6px;border-radius:50%;flex-shrink:0}
.resp-row-val{font-family:'DM Mono',monospace;font-size:14px;font-weight:500}
.resp-divider{border:none;border-top:1px solid var(--border);margin:.4rem 0}
.resp-deal-list{padding:.1rem 0 .4rem}
.resp-deal-item{display:flex;align-items:center;justify-content:space-between;padding:.28rem 1.25rem .28rem 2rem;gap:8px}
.resp-deal-name{font-size:11px;color:var(--dim);font-family:'DM Mono',monospace;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:190px}
.resp-deal-date{font-size:10px;color:var(--muted);font-family:'DM Mono',monospace;flex-shrink:0}
/* STAGES */
.stages-wrap{display:grid;gap:8px;margin-bottom:2rem}
.stage-row{background:var(--surface);border:1px solid var(--border);border-radius:10px;overflow:hidden}
.stage-header{display:flex;align-items:center;gap:12px;padding:.75rem 1.25rem;cursor:pointer;user-select:none}
.stage-color{width:3px;height:28px;border-radius:2px;flex-shrink:0}
.stage-name{font-size:13px;font-weight:600;flex:1}
.stage-count{font-family:'DM Mono',monospace;font-size:20px;font-weight:500}
.stage-arrow{font-size:12px;color:var(--muted);transition:transform .2s;margin-left:8px}
.stage-row.open .stage-arrow{transform:rotate(90deg)}
.stage-deals{display:none;border-top:1px solid var(--border)}
.stage-row.open .stage-deals{display:block}
table.dt{width:100%;border-collapse:collapse}
table.dt th{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;padding:.6rem 1.25rem;text-align:left;border-bottom:1px solid var(--border);background:rgba(0,0,0,.2)}
table.dt td{font-size:12px;padding:.6rem 1.25rem;border-bottom:1px solid rgba(255,255,255,.04);font-family:'DM Mono',monospace}
table.dt tr:last-child td{border-bottom:none}
table.dt tr:hover td{background:rgba(255,255,255,.02)}
.dn{color:var(--text);font-family:'Syne',sans-serif;font-size:13px;font-weight:500}
.du{color:var(--dim)}.dd{color:var(--muted)}
.motivos-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:8px;margin-bottom:1rem}
.mc{background:var(--surface);border:1px solid rgba(240,96,96,.2);border-radius:8px;padding:.75rem 1rem;display:flex;align-items:center;justify-content:space-between}
.mc-n{font-size:12px;color:var(--dim)}.mc-v{font-family:'DM Mono',monospace;font-size:20px;font-weight:500;color:var(--red)}
.tw{background:var(--surface);border:1px solid var(--border);border-radius:10px;overflow:hidden;margin-bottom:2rem}
.pill{display:inline-block;font-size:10px;font-family:'DM Mono',monospace;padding:2px 8px;border-radius:4px;border:1px solid rgba(240,96,96,.3);background:var(--red-dim);color:var(--red)}
.empty{padding:2rem;text-align:center;color:var(--muted);font-size:12px;font-family:'DM Mono',monospace}
#loading{position:fixed;inset:0;background:var(--bg);display:flex;flex-direction:column;align-items:center;justify-content:center;gap:16px;z-index:999}
.loader-bar{width:240px;height:2px;background:var(--border2);border-radius:2px;overflow:hidden}
.loader-fill{height:100%;background:var(--blue);width:0%;transition:width .4s;border-radius:2px}
.loader-text{font-size:12px;color:var(--muted);font-family:'DM Mono',monospace}
.spin{display:inline-block;animation:spin .7s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
::-webkit-scrollbar{width:4px;height:4px}::-webkit-scrollbar-thumb{background:var(--border2);border-radius:2px}
.proj-bar-wrap{margin-top:.6rem;height:4px;background:var(--border2);border-radius:2px;overflow:hidden}
.proj-bar{height:100%;border-radius:2px;transition:width .6s}
.total-split{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:2rem}
.total-funil-block{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:1.25rem}
.total-funil-title{font-size:12px;font-weight:600;letter-spacing:.06em;text-transform:uppercase;color:var(--muted);margin-bottom:1rem;padding-bottom:.75rem;border-bottom:1px solid var(--border)}
.total-row{display:flex;justify-content:space-between;align-items:center;padding:.4rem 0}
.total-row-label{color:var(--dim);display:flex;align-items:center;gap:8px;font-size:12px}
.total-row-val{font-family:'DM Mono',monospace;font-size:16px;font-weight:600}
.d1-wrap{background:var(--surface);border:1px solid var(--border);border-radius:12px;overflow:hidden;margin-bottom:2rem}
.d1-hd{padding:.85rem 1.25rem;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.d1-title{font-size:12px;font-weight:600;letter-spacing:.06em;text-transform:uppercase;color:var(--muted);flex:1}
.d1-badge{font-size:11px;font-family:'DM Mono',monospace;padding:2px 10px;border-radius:4px;border:1px solid;font-weight:600}
.d1-badge.yellow{color:var(--amber);border-color:rgba(240,168,48,.4);background:var(--amber-dim)}
.d1-badge.blue{color:var(--blue);border-color:rgba(79,143,255,.4);background:var(--blue-dim)}
.d1-section{padding:.6rem 1.25rem .2rem;border-bottom:1px solid var(--border)}
.d1-section:last-child{border-bottom:none}
.d1-section-label{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;margin-bottom:.4rem;font-family:'DM Mono',monospace}
.d1-item{display:flex;align-items:center;justify-content:space-between;padding:.35rem 0;border-bottom:1px solid var(--border)}
.d1-item:last-child{border-bottom:none}
.d1-item-name{font-size:13px;font-weight:600;color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:260px}
.d1-item-user{font-size:11px;color:var(--dim);font-family:'DM Mono',monospace}
.d1-empty{padding:.75rem 0;font-size:12px;color:var(--muted);font-family:'DM Mono',monospace}
</style>
</head>
<body>
<div id="loading">
  <div style="font-size:13px;color:var(--dim);letter-spacing:.06em;text-transform:uppercase;margin-bottom:8px">Carregando dados</div>
  <div class="loader-bar"><div class="loader-fill" id="lf"></div></div>
  <div class="loader-text" id="lt">Conectando...</div>
</div>
<header>
  <div class="logo">
    <div class="logo-dot" id="sdot"></div>
    <div><div class="logo-text">Dashboard Comercial</div><div class="logo-sub">RD Station CRM</div></div>
  </div>
  <div class="header-right">
    <div class="period-selector">
      <button class="period-btn" data-m="3" data-y="2026">Mar/26</button>
      <button class="period-btn active" data-m="4" data-y="2026">Abr/26</button>
    </div>
    <div class="last-update" id="lu">--</div>
    <div class="next-refresh" id="nr">proximo em --</div>
    <button class="theme-btn" id="tbtn" onclick="toggleTheme()" title="Alternar tema">🌙</button>
    <button class="refresh-btn" id="rbtn" onclick="loadAll()"><span id="rspin">&#8635;</span> Atualizar</button>
  </div>
</header>
<main>
  <div class="tabs">
    <button class="tab-btn" onclick="switchF('rp',this)">Funil Comercial RP</button>
    <button class="tab-btn" onclick="switchF('rrr',this)">Funil Comercial RRR Mae</button>
    <button class="tab-btn total-tab active" onclick="switchF('total',this)">+ Total</button>
  </div>
  <div id="pane-rp" style="display:none"></div>
  <div id="pane-rrr" style="display:none"></div>
  <div id="pane-total"></div>
</main>
<script>
let STATE={rp:null,rrr:null},selM=4,selY=2026,curF='total';

function toggleTheme(){const l=document.documentElement.classList.toggle('light');document.getElementById('tbtn').textContent=l?'☀️':'🌙';localStorage.setItem('theme',l?'light':'dark');}
(function(){if(localStorage.getItem('theme')==='light'){document.documentElement.classList.add('light');const b=document.getElementById('tbtn');if(b)b.textContent='☀️';}})();
const MN=['','Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez'];
const COLORS=['#4f8fff','#a78bfa','#3ecf8e','#f0a830','#fb923c','#2dd4bf','#f06060'];
const EXCLUDED=new Set(['Felipe Fernando','Luciano Santana']);

function workdaysInMonth(m,y){const days=new Date(y,m,0).getDate();let w=0;for(let d=1;d<=days;d++){const dw=new Date(y,m-1,d).getDay();if(dw>0&&dw<6)w++;}return w;}
function workdaysUntilToday(m,y){const today=new Date();const isCurrent=(today.getMonth()+1===m&&today.getFullYear()===y);const last=isCurrent?today.getDate():new Date(y,m,0).getDate();let w=0;for(let d=1;d<=last;d++){const dw=new Date(y,m-1,d).getDay();if(dw>0&&dw<6)w++;}return w;}

let nextRefreshAt=null;
function startCountdown(){nextRefreshAt=Date.now()+60*60*1000;}
function tickCountdown(){
  if(!nextRefreshAt){document.getElementById('nr').textContent='proximo em --';return;}
  const diff=Math.max(0,Math.round((nextRefreshAt-Date.now())/1000));
  const h=Math.floor(diff/3600),m=Math.floor((diff%3600)/60),s=diff%60;
  document.getElementById('nr').textContent=`proximo em ${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`;
}

function setLoad(p,t){document.getElementById('lf').style.width=p+'%';document.getElementById('lt').textContent=t;}

async function loadAll(){
  document.getElementById('rbtn').querySelector('#rspin').className='spin';
  document.getElementById('sdot').style.cssText='width:8px;height:8px;border-radius:50%;background:var(--amber);box-shadow:0 0 8px var(--amber)';
  document.getElementById('loading').style.display='flex';
  setLoad(10,'Buscando dados...');
  try{
    const [rp,rrr]=await Promise.all([
      fetch(`/api/data?funil=rp&month=${selM}&year=${selY}`).then(r=>{if(!r.ok)throw new Error(r.statusText);return r.json();}),
      fetch(`/api/data?funil=rrr&month=${selM}&year=${selY}`).then(r=>{if(!r.ok)throw new Error(r.statusText);return r.json();})
    ]);
    setLoad(90,'Renderizando...');STATE.rp=rp;STATE.rrr=rrr;
    await new Promise(r=>setTimeout(r,250));setLoad(100,'Pronto!');await new Promise(r=>setTimeout(r,250));
    document.getElementById('loading').style.display='none';
    renderAll();
    document.getElementById('lu').textContent='Atualizado '+new Date().toLocaleTimeString('pt-BR',{hour:'2-digit',minute:'2-digit'});
    document.getElementById('sdot').style.cssText='width:8px;height:8px;border-radius:50%;background:var(--green);box-shadow:0 0 8px var(--green);animation:pulse 2s infinite';
    startCountdown();
  }catch(e){
    document.getElementById('loading').style.display='none';
    document.getElementById('pane-'+curF).innerHTML=`<div style="background:var(--red-dim);border:1px solid rgba(240,96,96,.3);border-radius:10px;padding:1.25rem;color:var(--red);font-size:13px"><strong>Erro:</strong> ${e.message}</div>`;
    document.getElementById('sdot').style.cssText='width:8px;height:8px;border-radius:50%;background:var(--red);box-shadow:0 0 8px var(--red)';
  }
  document.getElementById('rbtn').querySelector('#rspin').className='';
}

function uname(d){return d.user||'--';}
function fdate(iso){if(!iso)return'--';const d=new Date(iso);return`${String(d.getDate()).padStart(2,'0')}/${String(d.getMonth()+1).padStart(2,'0')}`;}
function ftime(iso){
  if(!iso)return'--';const d=new Date(iso);const now=new Date();
  const diff=Math.round((now-d)/60000);
  if(diff<1)return'agora';if(diff<60)return`${diff}min`;if(diff<1440)return`${Math.floor(diff/60)}h atras`;
  return fdate(iso);
}
function fmoney(v){if(!v)return'--';return'R$ '+Number(v).toLocaleString('pt-BR',{minimumFractionDigits:2,maximumFractionDigits:2});}
function calcProj(v,m,y){
  const wdT=workdaysInMonth(m,y),wdD=workdaysUntilToday(m,y);
  if(!wdD)return{proj:0,wdT,wdD,ritmo:'0.00'};
  const r=v/wdD;return{proj:Math.round(r*wdT),wdT,wdD,ritmo:r.toFixed(2)};
}

function renderAll(){
  document.getElementById('pane-rp').innerHTML=renderPane('rp');
  document.getElementById('pane-rrr').innerHTML=renderPane('rrr');
  document.getElementById('pane-total').innerHTML=renderTotal();
}

// ─── renderFeed ──────────────────────────────────────────────────────────────
function renderFeed(feed,limit){
  const items=(feed||[]).slice(0,limit);
  let h=`<div class="feed-wrap"><div class="feed-hd"><span class="feed-hd-title">Ultimas movimentacoes no CRM</span><div class="feed-live"></div></div>`;
  if(!items.length){h+=`<div class="empty">Nenhuma movimentacao encontrada</div>`;}
  items.forEach(f=>{
    const cor=f.cor||'#888';
    let label='';
    if(f.tipo==='venda')      label='Vendida';
    else if(f.tipo==='contrato') label='Contrato enviado';
    else if(f.tipo==='perda') label=`Perdida em: ${f.etapa}`;
    else                      label=`Na etapa: ${f.etapa}`;
    const funiTag=f.funil&&f.funil.includes('RRR')?'RRR':'RP';
    h+=`<div class="feed-item">
      <div class="feed-dot" style="background:${cor}"></div>
      <div class="feed-body">
        <div class="feed-deal">${f.nome}</div>
        <div class="feed-meta">
          <span style="color:var(--dim)">${f.user}</span>
          <span style="color:var(--muted)">&#183;</span>
          <span class="feed-badge" style="color:${cor};border-color:${cor}44;background:${cor}18">${label}</span>
          <span style="color:var(--muted);font-size:10px">[${funiTag}]</span>
        </div>
      </div>
      <div class="feed-ts">${ftime(f.ts)}</div>
    </div>`;
  });
  h+=`</div>`;return h;
}

// ─── renderPane ───────────────────────────────────────────────────────────────
function renderPane(key){
  const s=STATE[key];if(!s)return'';
  const totAtivo=s.etapas.reduce((a,e)=>a+e.deals.length,0);
  const totV=s.vendas.length,totC=(s.contratos_mes||[]).length,totP=s.perdas.length;
  const {proj,wdT,wdD,ritmo}=calcProj(totV,selM,selY);
  const pct=Math.min(100,Math.round((totV/Math.max(proj,1))*100));
  const mmap={};s.perdas.forEach(d=>{const m=d.deal_lost_reason?.name||'--';mmap[m]=(mmap[m]||0)+1;});
  const msorted=Object.entries(mmap).sort((a,b)=>b[1]-a[1]);

  let h=`<div class="summary-grid-5">
    <div class="summary-card blue"><div class="sc-label">Em andamento</div><div class="sc-val blue">${totAtivo}</div><div class="sc-sub">no mes selecionado</div></div>
    <div class="summary-card blue"><div class="sc-label">Contratos enviados - ${MN[selM]}/${String(selY).slice(2)}</div><div class="sc-val blue">${totC}</div><div class="sc-sub">no mes</div></div>
    <div class="summary-card green"><div class="sc-label">Vendas - ${MN[selM]}/${String(selY).slice(2)}</div><div class="sc-val green">${totV}</div><div class="sc-sub">${wdD} dias uteis decorridos</div></div>
    <div class="summary-card amber"><div class="sc-label">Projecao do mes</div><div class="sc-val amber">${proj}</div><div class="sc-sub">${ritmo}/dia - ${wdT} dias uteis<div class="proj-bar-wrap"><div class="proj-bar" style="width:${pct}%;background:var(--amber)"></div></div></div></div>
    <div class="summary-card red"><div class="sc-label">Perdas</div><div class="sc-val red">${totP}</div><div class="sc-sub">historico total</div></div>
  </div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:2rem">
    <div class="summary-card purple"><div class="sc-label">Vendas por busca paga</div><div class="sc-val purple">${s.vendas_busca_paga||0}</div><div class="sc-sub">origem "busca" no mes</div></div>
    <div class="summary-card purple"><div class="sc-label">Contratos por busca paga</div><div class="sc-val purple">${s.contratos_busca_paga||0}</div><div class="sc-sub">contratos enviados via busca</div></div>
  </div>
  <div style="display:grid;grid-template-columns:1fr;gap:12px;margin-bottom:2rem">
    <div class="summary-card green"><div class="sc-label">Valor total negociacoes ativas</div><div class="sc-val green" style="font-size:24px">${fmoney(s.etapas.reduce((a,e)=>a+e.deals.reduce((b,d)=>b+(d.amount_total||0),0),0))}</div><div class="sc-sub">soma das etapas do mes</div></div>
  </div>`;

  h+=renderFeed(s.feed,15);

  h+=renderD1(key==='rp'?s.contrato_ativos:[], key==='rrr'?s.contrato_ativos:[]);

  // responsaveis
  const umap={};
  s.etapas.forEach(e=>e.deals.forEach(d=>{const u=uname(d);if(!umap[u])umap[u]={ativo:0,et:{},vendas:[],contratos:[],perdas:0,valor:0};umap[u].ativo++;umap[u].et[e.nome]=(umap[u].et[e.nome]||0)+1;umap[u].valor+=(d.amount_total||0);}));
  s.vendas.forEach(d=>{const u=uname(d);if(!umap[u])umap[u]={ativo:0,et:{},vendas:[],contratos:[],perdas:0,valor:0};umap[u].vendas.push(d);umap[u].valor+=(d.amount_total||0);});
  if(s.contratos_mes)s.contratos_mes.forEach(d=>{const u=uname(d);if(!umap[u])umap[u]={ativo:0,et:{},vendas:[],contratos:[],perdas:0,valor:0};umap[u].contratos.push(d);});
  s.perdas.forEach(d=>{const u=uname(d);if(!umap[u])umap[u]={ativo:0,et:{},vendas:[],contratos:[],perdas:0,valor:0};umap[u].perdas++;});
  const users=Object.entries(umap).filter(([n])=>!EXCLUDED.has(n)).sort((a,b)=>b[1].ativo-a[1].ativo);

  h+=`<div class="section-hd"><h3>Por responsavel</h3><span class="cnt blue">${users.length} vendedores</span><div class="section-line"></div></div><div class="resp-grid">`;
  users.forEach(([name,data],i)=>{
    const color=COLORS[i%COLORS.length],init=name.split(' ').slice(0,2).map(w=>w[0]||'').join('').toUpperCase();
    h+=`<div class="resp-card">
      <div class="resp-header">
        <div class="resp-avatar" style="background:${color}22;color:${color}">${init}</div>
        <div style="flex:1;min-width:0"><div style="display:flex;align-items:center;gap:8px"><div class="resp-name">${name}</div><span style="font-family:'DM Mono',monospace;font-size:13px;font-weight:700;color:var(--green)">${data.vendas.length}v</span></div>
        <div style="font-size:11px;color:var(--muted);font-family:'DM Mono',monospace">${Object.keys(data.et).length} etapas · ${fmoney(data.valor)}</div></div>
        <div class="resp-total" style="color:${color}">${data.ativo}</div>
      </div><div class="resp-rows">`;
    s.etapas.forEach(e=>{const c=data.et[e.nome]||0;if(!c)return;h+=`<div class="resp-row"><span class="resp-row-label"><span class="resp-row-dot" style="background:${e.cor}"></span>${e.nome}</span><span class="resp-row-val" style="color:${e.cor}">${c}</span></div>`;});
    h+=`<hr class="resp-divider">`;
    // contratos com lista
    h+=`<div class="resp-row"><span class="resp-row-label"><span class="resp-row-dot" style="background:var(--blue)"></span>Contratos enviados</span><span class="resp-row-val" style="color:var(--blue)">${data.contratos.length}</span></div>`;
    if(data.contratos.length){h+=`<div class="resp-deal-list">`;data.contratos.forEach(d=>{h+=`<div class="resp-deal-item"><span class="resp-deal-name" title="${d.name||''}">${d.name||'--'}</span><span class="resp-deal-date">${fdate(d.updated_at)}</span></div>`;});h+=`</div>`;}
    // vendas com lista
    h+=`<div class="resp-row"><span class="resp-row-label"><span class="resp-row-dot" style="background:var(--green)"></span>Vendas</span><span class="resp-row-val" style="color:var(--green)">${data.vendas.length}</span></div>`;
    if(data.vendas.length){h+=`<div class="resp-deal-list">`;data.vendas.forEach(d=>{h+=`<div class="resp-deal-item"><span class="resp-deal-name" title="${d.name||''}">${d.name||'--'}</span><span class="resp-deal-date">${fdate(d.closed_at)}</span></div>`;});h+=`</div>`;}
    h+=`<div class="resp-row"><span class="resp-row-label"><span class="resp-row-dot" style="background:var(--red)"></span>Perdas</span><span class="resp-row-val" style="color:var(--red)">${data.perdas}</span></div>
    </div></div>`;
  });
  h+=`</div>`;

  // etapas
  h+=`<div class="section-hd"><h3>Negociacoes por etapa</h3><span class="cnt blue">${totAtivo} total</span><div class="section-line"></div></div><div class="stages-wrap">`;
  s.etapas.forEach((e,ei)=>{
    const count=e.deals.length;
    h+=`<div class="stage-row" id="sr-${key}-${ei}"><div class="stage-header" onclick="tog('sr-${key}-${ei}')"><div class="stage-color" style="background:${e.cor}"></div><span class="stage-name">${e.nome}</span><span class="stage-count" style="color:${e.cor}">${count}</span><span class="stage-arrow">&#9654;</span></div><div class="stage-deals">`;
    if(!count){h+=`<div class="empty">Nenhuma negociacao em andamento</div>`;}
    else{h+=`<table class="dt"><thead><tr><th>Negociacao</th><th>Responsavel</th><th>Atualizado</th></tr></thead><tbody>`;e.deals.forEach(d=>{h+=`<tr><td class="dn">${d.name||'--'}</td><td class="du">${uname(d)}</td><td class="dd">${fdate(d.updated_at)}</td></tr>`;});h+=`</tbody></table>`;}
    h+=`</div></div>`;
  });h+=`</div>`;

  // contratos do mes
  h+=`<div class="section-hd" style="margin-top:2rem"><h3>Contratos enviados - ${MN[selM]}/${selY}</h3><span class="cnt blue">${totC} total</span><div class="section-line"></div></div>`;
  if(!totC){h+=`<div class="empty" style="background:var(--surface);border:1px solid var(--border);border-radius:10px;margin-bottom:2rem">Nenhum contrato neste periodo</div>`;}
  else{h+=`<div class="tw" style="border-color:rgba(79,143,255,.2);margin-bottom:2rem"><table class="dt"><thead><tr><th>Negociacao</th><th>Responsavel</th><th>Data</th></tr></thead><tbody>`;s.contratos_mes.forEach(d=>{h+=`<tr><td class="dn" style="color:var(--blue)">${d.name||'--'}</td><td class="du">${uname(d)}</td><td class="dd">${fdate(d.updated_at)}</td></tr>`;});h+=`</tbody></table></div>`;}

  // vendas do mes
  h+=`<div class="section-hd"><h3>Vendas fechadas - ${MN[selM]}/${selY}</h3><span class="cnt green">${totV} total</span><div class="section-line"></div></div>`;
  if(!totV){h+=`<div class="empty" style="background:var(--surface);border:1px solid var(--border);border-radius:10px;margin-bottom:2rem">Nenhuma venda neste periodo</div>`;}
  else{h+=`<div class="tw" style="border-color:rgba(62,207,142,.2);margin-bottom:2rem"><table class="dt"><thead><tr><th>Negociacao</th><th>Responsavel</th><th>Fechado em</th></tr></thead><tbody>`;s.vendas.forEach(d=>{h+=`<tr><td class="dn" style="color:var(--green)">${d.name||'--'}</td><td class="du">${uname(d)}</td><td class="dd">${fdate(d.closed_at)}</td></tr>`;});h+=`</tbody></table></div>`;}

  // perdas
  h+=`<div class="section-hd" style="margin-top:2rem"><h3>Perdas nas etapas finais</h3><span class="cnt red">${totP} total</span><div class="section-line"></div></div>`;
  h+=`<div class="motivos-grid">`+msorted.map(([m,c])=>`<div class="mc"><span class="mc-n">${m}</span><span class="mc-v">${c}</span></div>`).join('')+`</div>`;
  if(totP){
    h+=`<div class="stage-row" id="perdas-${key}" style="margin-bottom:2rem"><div class="stage-header" onclick="tog('perdas-${key}')"><div class="stage-color" style="background:var(--red)"></div><span class="stage-name">Negociacoes perdidas</span><span class="stage-count" style="color:var(--red)">${totP}</span><span class="stage-arrow">&#9654;</span></div><div class="stage-deals"><table class="dt"><thead><tr><th>Negociacao</th><th>Responsavel</th><th>Etapa</th><th>Motivo</th></tr></thead><tbody>`;
    s.perdas.forEach(d=>{const m=d.deal_lost_reason?.name||'--';h+=`<tr><td class="dn">${d.name||'--'}</td><td class="du">${uname(d)}</td><td class="dd">${d.deal_stage?.name||'--'}</td><td><span class="pill">${m}</span></td></tr>`;});
    h+=`</tbody></table></div></div>`;}
  return h;
}

// ─── renderTotal ──────────────────────────────────────────────────────────────
function renderTotal(){
  const rp=STATE.rp,rrr=STATE.rrr;if(!rp||!rrr)return'';
  const rpV=rp.vendas.length,rrrV=rrr.vendas.length,totV=rpV+rrrV;
  const rpC=(rp.contratos_mes||[]).length,rrrC=(rrr.contratos_mes||[]).length,totC=rpC+rrrC;
  const rpA=rp.etapas.reduce((a,e)=>a+e.deals.length,0),rrrA=rrr.etapas.reduce((a,e)=>a+e.deals.length,0),totA=rpA+rrrA;
  const totP=rp.perdas.length+rrr.perdas.length;
  const {proj,wdT,wdD,ritmo}=calcProj(totV,selM,selY);
  const pct=Math.min(100,Math.round((totV/Math.max(proj,1))*100));

  const totBuscaPagaV=(rp.vendas_busca_paga||0)+(rrr.vendas_busca_paga||0);
  const totBuscaPagaC=(rp.contratos_busca_paga||0)+(rrr.contratos_busca_paga||0);
  let h=`<div class="summary-grid-5">
    <div class="summary-card blue"><div class="sc-label">Em andamento - ambos funis</div><div class="sc-val blue">${totA}</div><div class="sc-sub">no mes selecionado</div></div>
    <div class="summary-card blue"><div class="sc-label">Contratos enviados - ${MN[selM]}/${String(selY).slice(2)}</div><div class="sc-val blue">${totC}</div><div class="sc-sub">RP: ${rpC} / RRR: ${rrrC}</div></div>
    <div class="summary-card green"><div class="sc-label">Vendas - ${MN[selM]}/${String(selY).slice(2)}</div><div class="sc-val green">${totV}</div><div class="sc-sub">RP: ${rpV} / RRR: ${rrrV}</div></div>
    <div class="summary-card amber"><div class="sc-label">Projecao total do mes</div><div class="sc-val amber">${proj}</div><div class="sc-sub">${ritmo}/dia - ${wdT} dias uteis<div class="proj-bar-wrap"><div class="proj-bar" style="width:${pct}%;background:var(--amber)"></div></div></div></div>
    <div class="summary-card red"><div class="sc-label">Perdas - ambos funis</div><div class="sc-val red">${totP}</div><div class="sc-sub">historico total</div></div>
  </div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:2rem">
    <div class="summary-card purple"><div class="sc-label">Vendas por busca paga</div><div class="sc-val purple">${totBuscaPagaV}</div><div class="sc-sub">ambos funis · origem "busca"</div></div>
    <div class="summary-card purple"><div class="sc-label">Contratos por busca paga</div><div class="sc-val purple">${totBuscaPagaC}</div><div class="sc-sub">contratos enviados via busca</div></div>
  </div>
  <div style="display:grid;grid-template-columns:1fr;gap:12px;margin-bottom:2rem">
    <div class="summary-card green"><div class="sc-label">Valor total negociacoes ativas</div><div class="sc-val green" style="font-size:22px">${fmoney([...rp.etapas,...rrr.etapas].reduce((a,e)=>a+e.deals.reduce((b,d)=>b+(d.amount_total||0),0),0))}</div><div class="sc-sub">soma de todas as etapas do mes</div></div>
  </div>`;

  h+=renderContratosPorDia(rp.contratos_mes, rrr.contratos_mes);

  // split RP x RRR
  const etapasNomes='Contrato enviado, Assinatura eletronica, Fazendo estimativa, Preparando PDF, Apresentar, PRFB, C4';
  h+=`<div class="total-split">
    <div class="total-funil-block"><div class="total-funil-title">Funil Comercial RP</div>
      <div class="total-row"><span class="total-row-label"><span class="resp-row-dot" style="background:var(--blue)"></span>Contratos enviados</span><span class="total-row-val" style="color:var(--blue)">${rpC}</span></div>
      <div class="total-row"><span class="total-row-label"><span class="resp-row-dot" style="background:var(--green)"></span>Vendas fechadas</span><span class="total-row-val" style="color:var(--green)">${rpV}</span></div>
      <div class="total-row"><span class="total-row-label"><span class="resp-row-dot" style="background:var(--dim)"></span>Em andamento</span><span class="total-row-val" style="color:var(--dim)">${rpA}</span></div>
      <div style="margin-top:.5rem;font-size:10px;color:var(--muted);font-family:'DM Mono',monospace;line-height:1.6">Etapas: ${etapasNomes}</div>
    </div>
    <div class="total-funil-block"><div class="total-funil-title">Funil Comercial RRR Mae</div>
      <div class="total-row"><span class="total-row-label"><span class="resp-row-dot" style="background:var(--blue)"></span>Contratos enviados</span><span class="total-row-val" style="color:var(--blue)">${rrrC}</span></div>
      <div class="total-row"><span class="total-row-label"><span class="resp-row-dot" style="background:var(--green)"></span>Vendas fechadas</span><span class="total-row-val" style="color:var(--green)">${rrrV}</span></div>
      <div class="total-row"><span class="total-row-label"><span class="resp-row-dot" style="background:var(--dim)"></span>Em andamento</span><span class="total-row-val" style="color:var(--dim)">${rrrA}</span></div>
      <div style="margin-top:.5rem;font-size:10px;color:var(--muted);font-family:'DM Mono',monospace;line-height:1.6">Etapas: ${etapasNomes}</div>
    </div>
  </div>`;

  // responsaveis totais — PRIMEIRO, com listas colapsaveis
  const umap={};
  function addU(u,f,d){if(!umap[u])umap[u]={ativo:0,vendas:[],contratos:[],perdas:0,valor:0};if(f==='ativo')umap[u].ativo++;else if(f==='perda')umap[u].perdas++;else{umap[u][f].push(d);umap[u].valor+=(d.amount_total||0);}}
  rp.vendas.forEach(d=>addU(uname(d),'vendas',d));
  rrr.vendas.forEach(d=>addU(uname(d),'vendas',d));
  if(rp.contratos_mes)rp.contratos_mes.forEach(d=>addU(uname(d),'contratos',d));
  if(rrr.contratos_mes)rrr.contratos_mes.forEach(d=>addU(uname(d),'contratos',d));
  rp.perdas.forEach(d=>addU(uname(d),'perda',d));
  rrr.perdas.forEach(d=>addU(uname(d),'perda',d));
  rp.etapas.forEach(e=>e.deals.forEach(d=>addU(uname(d),'ativo',d)));
  rrr.etapas.forEach(e=>e.deals.forEach(d=>addU(uname(d),'ativo',d)));
  const users=Object.entries(umap).filter(([n])=>!EXCLUDED.has(n)).sort((a,b)=>b[1].vendas.length-a[1].vendas.length||b[1].contratos.length-a[1].contratos.length);

  h+=`<div class="section-hd"><h3>Por responsavel - Total (ambos funis)</h3><span class="cnt amber">${users.length} vendedores</span><div class="section-line"></div></div><div class="resp-grid">`;
  users.forEach(([name,data],i)=>{
    const color=COLORS[i%COLORS.length],init=name.split(' ').slice(0,2).map(w=>w[0]||'').join('').toUpperCase();
    const uid=`tot-${i}`;
    h+=`<div class="resp-card">
      <div class="resp-header">
        <div class="resp-avatar" style="background:${color}22;color:${color}">${init}</div>
        <div style="flex:1;min-width:0"><div style="display:flex;align-items:center;gap:8px"><div class="resp-name">${name}</div><span style="font-family:'DM Mono',monospace;font-size:13px;font-weight:700;color:var(--green)">${data.vendas.length}v</span></div>
        <div style="font-size:11px;color:var(--muted);font-family:'DM Mono',monospace">${data.ativo} ativas · ${fmoney(data.valor)}</div></div>
        <div class="resp-total" style="color:${color}">${data.vendas.length}</div>
      </div><div class="resp-rows">`;
    // contratos — clicavel para abrir/fechar lista
    h+=`<div class="resp-row" style="cursor:${data.contratos.length?'pointer':'default'}" onclick="${data.contratos.length?`togInline('${uid}-c','${uid}-c-arrow')`:''}">
      <span class="resp-row-label"><span class="resp-row-dot" style="background:var(--blue)"></span>Contratos enviados</span>
      <span style="display:flex;align-items:center;gap:6px"><span class="resp-row-val" style="color:var(--blue)">${data.contratos.length}</span>${data.contratos.length?`<span id="${uid}-c-arrow" style="font-size:10px;color:var(--muted);display:inline-block;transition:transform .2s">&#9654;</span>`:''}</span>
    </div>`;
    if(data.contratos.length){
      h+=`<div id="${uid}-c" style="display:none;border-top:1px solid var(--border)"><div class="resp-deal-list">`;
      data.contratos.forEach(d=>{h+=`<div class="resp-deal-item"><span class="resp-deal-name" title="${d.name||''}">${d.name||'--'}</span><span class="resp-deal-date">${fdate(d.updated_at)}</span></div>`;});
      h+=`</div></div>`;
    }
    // vendas — clicavel para abrir/fechar lista
    h+=`<div class="resp-row" style="cursor:${data.vendas.length?'pointer':'default'}" onclick="${data.vendas.length?`togInline('${uid}-v','${uid}-v-arrow')`:''}">
      <span class="resp-row-label"><span class="resp-row-dot" style="background:var(--green)"></span>Vendas</span>
      <span style="display:flex;align-items:center;gap:6px"><span class="resp-row-val" style="color:var(--green)">${data.vendas.length}</span>${data.vendas.length?`<span id="${uid}-v-arrow" style="font-size:10px;color:var(--muted);display:inline-block;transition:transform .2s">&#9654;</span>`:''}</span>
    </div>`;
    if(data.vendas.length){
      h+=`<div id="${uid}-v" style="display:none;border-top:1px solid var(--border)"><div class="resp-deal-list">`;
      data.vendas.forEach(d=>{h+=`<div class="resp-deal-item"><span class="resp-deal-name" title="${d.name||''}">${d.name||'--'}</span><span class="resp-deal-date">${fdate(d.closed_at)}</span></div>`;});
      h+=`</div></div>`;
    }
    h+=`<div class="resp-row"><span class="resp-row-label"><span class="resp-row-dot" style="background:var(--red)"></span>Perdas</span><span class="resp-row-val" style="color:var(--red)">${data.perdas}</span></div>
    </div></div>`;
  });
  h+=`</div>`;

  // feed combinado — ULTIMO, apenas 2 itens
  const feedCombo=[...(rp.feed||[]),...(rrr.feed||[])].sort((a,b)=>b.ts.localeCompare(a.ts));
  h+=renderFeed(feedCombo,2);

  h+=renderD1(rp.contrato_ativos, rrr.contrato_ativos);

  return h;
}

function prevWorkday(date){
  // retorna o ultimo dia util anterior (pula sabado e domingo)
  const d=new Date(date);
  do{d.setDate(d.getDate()-1);}while(d.getDay()===0||d.getDay()===6);
  return d;
}
function dateStr(d){return`${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;}

function renderD1(rpAtivos, rrrAtivos){
  const now=new Date();
  const todayStr=dateStr(now);
  const prevWd=prevWorkday(now);
  const prevWdStr=dateStr(prevWd);
  const isMonday=now.getDay()===1;
  const d1Label=isMonday?'Sexta-feira (D+1)':'Ontem (D+1)';

  const allAtivos=[
    ...(rpAtivos||[]).map(d=>({...d,_funil:'RP'})),
    ...(rrrAtivos||[]).map(d=>({...d,_funil:'RRR'}))
  ];

  // D+1: updated_at == ultimo dia util (sexta se hoje e segunda, senao ontem)
  const d1=allAtivos.filter(d=>(d.updated_at||'').startsWith(prevWdStr));
  // Hoje: updated_at == hoje
  const dHoje=allAtivos.filter(d=>(d.updated_at||'').startsWith(todayStr));

  let h=`<div class="d1-wrap">
    <div class="d1-hd">
      <span class="d1-title">&#9200; Contrato enviado — Acompanhamento diario</span>
      <span class="d1-badge yellow">D+1: ${d1.length} aguardando</span>
      <span class="d1-badge blue">Hoje: ${dHoje.length} novos</span>
    </div>`;

  h+=`<div class="d1-section"><div class="d1-section-label">D+1 — ${d1Label}, aguardando assinatura</div>`;
  if(!d1.length){h+=`<div class="d1-empty">Nenhum contrato aguardando</div>`;}
  else{d1.forEach(d=>{h+=`<div class="d1-item"><div><div class="d1-item-name">${d.name||'--'}</div><div class="d1-item-user">${d.user||'--'} · ${d._funil}</div></div><span style="font-size:10px;color:var(--amber);font-family:'DM Mono',monospace;white-space:nowrap">${d1Label.split(' ')[0].toLowerCase()}</span></div>`;});}
  h+=`</div>`;

  h+=`<div class="d1-section"><div class="d1-section-label">Movidos para contrato enviado hoje</div>`;
  if(!dHoje.length){h+=`<div class="d1-empty">Nenhum contrato enviado hoje ainda</div>`;}
  else{dHoje.forEach(d=>{h+=`<div class="d1-item"><div><div class="d1-item-name">${d.name||'--'}</div><div class="d1-item-user">${d.user||'--'} · ${d._funil}</div></div><span style="font-size:10px;color:var(--blue);font-family:'DM Mono',monospace;white-space:nowrap">hoje</span></div>`;});}
  h+=`</div>`;

  h+=`</div>`;
  return h;
}

function renderContratosPorDia(rpContratos, rrrContratos){
  // agrupa todos os contratos por dia (updated_at), do dia 1 ao ultimo com contrato
  const todos=[...(rpContratos||[]).map(d=>({...d,_funil:'RP'})),...(rrrContratos||[]).map(d=>({...d,_funil:'RRR'}))];
  const byDay={};
  todos.forEach(d=>{
    const dt=(d.updated_at||'').slice(0,10);
    if(!dt)return;
    if(!byDay[dt])byDay[dt]=[];
    byDay[dt].push(d);
  });
  const days=Object.keys(byDay).sort();
  if(!days.length)return'';
  const todayStr=dateStr(new Date());
  const total=todos.length;

  let h=`<div class="stage-row" id="cpd-wrap" style="margin-bottom:2rem">
    <div class="stage-header" onclick="tog('cpd-wrap')">
      <div class="stage-color" style="background:var(--blue)"></div>
      <span class="stage-name">Contratos enviados por dia — ${MN[selM]}/${selY}</span>
      <span class="stage-count" style="color:var(--blue)">${total}</span>
      <span class="stage-arrow">&#9654;</span>
    </div>
    <div class="stage-deals">
      <table class="dt"><thead><tr><th>Dia</th><th>Qtd</th><th>Negociacoes</th><th>Responsavel</th><th>Funil</th></tr></thead><tbody>`;
  days.forEach(day=>{
    const items=byDay[day];
    const isToday=day===todayStr;
    const dayLabel=day.slice(8)+'/'+day.slice(5,7)+(isToday?' (hoje)':'');
    items.forEach((d,idx)=>{
      const rowspan=idx===0?` rowspan="${items.length}"`:'';
      h+=`<tr>`;
      if(idx===0)h+=`<td class="dd"${rowspan} style="${isToday?'color:var(--green);font-weight:700':''}">${dayLabel}</td><td class="dd"${rowspan} style="text-align:center;font-weight:700;color:var(--blue)">${items.length}</td>`;
      h+=`<td class="dn">${d.name||'--'}</td><td class="du">${d.user||'--'}</td><td class="dd">${d._funil}</td></tr>`;
    });
  });
  h+=`</tbody></table></div></div>`;
  return h;
}

function tog(id){const el=document.getElementById(id);if(!el)return;el.classList.toggle('open');const arrow=document.getElementById(id+'-arrow');if(arrow){arrow.style.transform=el.classList.contains('open')?'rotate(90deg)':'';}}
function togInline(id,arrowId){const el=document.getElementById(id);if(!el)return;const open=el.style.display==='none'||el.style.display==='';el.style.display=open?'block':'none';const arrow=document.getElementById(arrowId);if(arrow){arrow.style.transform=open?'rotate(90deg)':'';}}
function switchF(k,btn){curF=k;document.querySelectorAll('.tab-btn').forEach(b=>b.classList.remove('active'));btn.classList.add('active');['rp','rrr','total'].forEach(f=>document.getElementById('pane-'+f).style.display=f===k?'':'none');}
document.querySelectorAll('.period-btn').forEach(b=>{b.addEventListener('click',()=>{document.querySelectorAll('.period-btn').forEach(x=>x.classList.remove('active'));b.classList.add('active');selM=parseInt(b.dataset.m);selY=parseInt(b.dataset.y);loadAll();});});

setInterval(loadAll,60*60*1000);
setInterval(tickCountdown,1000);
loadAll();
</script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        print(f"  {args[0]} {args[1]} {args[2]}")

    def send_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        qs     = dict(urllib.parse.parse_qsl(parsed.query))

        if parsed.path == "/":
            body = HTML.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", len(body))
            self.end_headers()
            self.wfile.write(body)

        elif parsed.path == "/api/data":
            key   = qs.get("funil", "rp")
            now   = datetime.datetime.now()
            month = int(qs.get("month", now.month))
            year  = int(qs.get("year",  now.year))
            try:
                print(f"\n-> Buscando {key.upper()} -- {month}/{year}")
                data = load_funil_data(key, month, year)
                self.send_json(data)
                print(f"   OK  vendas={len(data['vendas'])}  contratos={len(data['contratos_mes'])}  feed={len(data['feed'])}")
            except Exception as e:
                import traceback; traceback.print_exc()
                self.send_json({"error": str(e)}, 500)
        else:
            self.send_response(404)
            self.end_headers()


if __name__ == "__main__":
    import os
    PORT = int(os.environ.get("PORT", 8765))
    HOST = "0.0.0.0"
    server = HTTPServer((HOST, PORT), Handler)
    print("=" * 50)
    print("  Dashboard Comercial -- RD Station CRM")
    print("=" * 50)
    print(f"\n  Acesse: http://{HOST}:{PORT}")
    print("  Pressione Ctrl+C para parar\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Servidor encerrado.")
