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
            # "Contrato enviado" removida da exibicao por etapa (ainda usada como contrato_stage_id)
            {"id": "68c0589be520d500198b8beb", "nome": "Assinatura eletronica", "cor": "#f0a830"},
            {"id": "68dd780d1359390014d37c2b", "nome": "Fazendo estimativa",    "cor": "#a78bfa"},
            {"id": "68dd781197fb9700276860f7", "nome": "Preparando PDF",        "cor": "#2dd4bf"},
            {"id": "68c058a8905a480021f2a1e9", "nome": "Apresentar",            "cor": "#3ecf8e"},
            {"id": "699f22a804f22c001ec7cb5d", "nome": "PRFB",                  "cor": "#fb923c"},
            {"id": "69aed6bcd8e658001e6773bb", "nome": "C4",                    "cor": "#4f8fff"},
        ],
        "ok_stage_id":        "68d99bd829688b00193d8962",
        "contrato_stage_id":  "68a714f1b3f7b8001c750c1e",
        "assin_stage_id":     "68c0589be520d500198b8beb",
        "prfb_stage_id":      "699f22a804f22c001ec7cb5d",
        # Etapas pre-contrato: IDs fixos para busca direta (evita dependencia da API /deal_pipelines)
        "pre_contrato_stages": [
            # IDs serao descobertos via fetch_deals_by_stage_name mas com fallback por nome
            # Se souber os IDs, colocar aqui: {"id": "...", "nome": "Desenvolvimento"}
        ],
        "pre_contrato_nomes": ["desenvolvimento", "tem perfil"],
    },
    "rrr": {
        "id":   "693873d32abcdb001f8409c3",
        "nome": "Funil Comercial RRR Mae",
        "etapas": [
            # "Contrato enviado" removida da exibicao por etapa (ainda usada como contrato_stage_id)
            {"id": "693874dfb6be4c0015bf64d3", "nome": "Assinatura eletronica","cor": "#f0a830"},
            {"id": "6938750379e7eb001d47db46", "nome": "Fazendo estimativa",   "cor": "#a78bfa"},
            {"id": "69387510ddb6b40022af1b53", "nome": "Preparando PDF",       "cor": "#2dd4bf"},
            {"id": "6938751f576c0000134edfe6", "nome": "Apresentar",           "cor": "#3ecf8e"},
            {"id": "69a6e61733a3ff00206a5e8d", "nome": "PRFB",                 "cor": "#fb923c"},
            {"id": "69aedc182221780020823bab", "nome": "C4",                   "cor": "#4f8fff"},
        ],
        "ok_stage_id":        "6938752561fe57001f7540f5",
        "contrato_stage_id":  "693873d32abcdb001f8409c6",
        "assin_stage_id":     "693874dfb6be4c0015bf64d3",
        "prfb_stage_id":      "69a6e61733a3ff00206a5e8d",
        "pre_contrato_stages": [],
        "pre_contrato_nomes": ["desenvolvimento", "tem perfil"],
    }
}

# ── helpers ───────────────────────────────────────────────────────────────────
def rd_get(path, retries=2):
    sep = "&" if "?" in path else "?"
    url = f"{BASE}{path}{sep}token={TOKEN}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    last_err = None
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=25) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            last_err = e
            if attempt < retries:
                import time; time.sleep(1)
    raise last_err

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

def fetch_pipeline_stages(pipeline_id):
    """Retorna lista de etapas do pipeline via API."""
    try:
        d = rd_get(f"/deal_pipelines/{pipeline_id}")
        return d.get("deal_stages") or []
    except Exception:
        return []

def fetch_deals_by_stage_name(pipeline_id, stage_name_lower):
    """Busca deals de uma etapa pelo nome via /deal_pipelines para descobrir o stage_id,
    depois busca os deals. Retorna (stage_id, deals).
    Fallback: busca todos os deals ativos do pipeline e filtra pelo nome da etapa."""
    # Primeiro: tentar via lista de stages do pipeline
    stages = fetch_pipeline_stages(pipeline_id)
    for s in stages:
        if stage_name_lower in (s.get("name") or "").lower():
            sid = s.get("_id") or s.get("id")
            if sid:
                try:
                    deals = fetch_all(pipeline_id, sid)
                    print(f"   [EA] pipeline={pipeline_id} etapa='{stage_name_lower}' sid={sid} deals={len(deals)}")
                    return sid, deals
                except Exception as e:
                    print(f"   [EA WARN] fetch_all falhou para sid={sid}: {e}")
    # Fallback: buscar pelo campo deal_stage.name nos deals ativos (sem filtro de stage)
    print(f"   [EA FALLBACK] buscando '{stage_name_lower}' por deal_stage.name no pipeline {pipeline_id}")
    try:
        all_deals, page = [], 1
        while True:
            d = rd_get(f"/deals?deal_pipeline_id={pipeline_id}&limit=200&page={page}")
            batch = d.get("deals") or []
            matched = [x for x in batch
                       if stage_name_lower in ((x.get("deal_stage") or {}).get("name") or "").lower()
                       and x.get("win") is None]
            all_deals.extend(matched)
            if len(batch) < 200:
                break
            page += 1
        print(f"   [EA FALLBACK] encontrados {len(all_deals)} deals para '{stage_name_lower}'")
        return None, all_deals
    except Exception as e:
        print(f"   [EA FALLBACK ERRO] {e}")
        return None, []

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

def get_fonte(deal):
    """Retorna o campo 'fonte' nativo do deal (ex: 'Busca Paga | Facebook Ads').
    A API do RD Station pode retornar como 'fonte', 'source' ou dentro de deal_source."""
    # campo direto
    for key in ("fonte", "source"):
        v = (deal.get(key) or "").strip()
        if v:
            return v
    # campo aninhado deal_source
    ds = deal.get("deal_source")
    if isinstance(ds, dict):
        return (ds.get("name") or ds.get("fonte") or ds.get("source") or "").strip()
    # fallback: buscar em custom fields com label "fonte"
    for cf in (deal.get("deal_custom_fields") or []):
        lbl = (cf.get("custom_field") or {}).get("label", "")
        if "fonte" in lbl.lower():
            return (cf.get("value") or "").strip()
    return ""

def is_busca_paga(deal):
    origem = get_origem(deal).lower()
    fonte  = get_fonte(deal).lower()
    return origem.startswith("busca paga") or fonte.startswith("busca paga")

def get_custom_date(deal, label_substring):
    """Retorna a data de um campo customizado pelo nome (case-insensitive).
    O RD Station retorna datas customizadas no formato DD/MM/YYYY.
    Converte sempre para YYYY-MM-DD para comparacoes internas.
    Retorna string 'YYYY-MM-DD' ou '' se nao encontrado/vazio."""
    for cf in (deal.get("deal_custom_fields") or []):
        lbl = (cf.get("custom_field") or {}).get("label", "")
        if label_substring.lower() in lbl.lower():
            val = (cf.get("value") or "").strip()
            if not val:
                return ""
            # Formato DD/MM/YYYY (padrao RD Station para campos de data)
            if len(val) >= 10 and val[2] == "/" and val[5] == "/":
                day, mon, yr = val[:2], val[3:5], val[6:10]
                return f"{yr}-{mon}-{day}"
            # Fallback: ISO YYYY-MM-DD ou timestamp
            return val[:10]
    return ""

def fmt_custom_date(iso_date):
    """Converte YYYY-MM-DD de volta para DD/MM/YYYY para exibir no frontend."""
    if not iso_date or len(iso_date) < 10:
        return iso_date or "--"
    try:
        yr, mon, day = iso_date[:4], iso_date[5:7], iso_date[8:10]
        return f"{day}/{mon}/{yr}"
    except Exception:
        return iso_date

def custom_date_in_month(deal, label_substring, month, year):
    """Verifica se o campo customizado de data esta no mes/ano dados."""
    val = get_custom_date(deal, label_substring)
    if not val:
        return False
    try:
        dt = datetime.date.fromisoformat(val)
        return dt.month == month and dt.year == year
    except Exception:
        return False

def custom_date_equals(deal, label_substring, date_str):
    """Verifica se o campo customizado de data e igual a date_str ('YYYY-MM-DD')."""
    val = get_custom_date(deal, label_substring)
    if not val:
        return False
    return val == date_str

# ── main loader ───────────────────────────────────────────────────────────────
def get_contrato_entry_date(d):
    """Retorna a data (YYYY-MM-DD) em que o deal entrou na etapa Contrato enviado.
    Tenta multiplos campos da API RD Station em ordem de confiabilidade.
    Se nao encontrar nenhum campo de data de etapa, usa updated_at como fallback."""
    candidates = [
        d.get("deal_stage_updated_at"),
        d.get("last_stage_update"),
        d.get("stage_updated_at"),
        d.get("deal_stage_changed_at"),
        d.get("moved_at"),
    ]
    ds = d.get("deal_stage")
    if isinstance(ds, dict):
        candidates += [ds.get("updated_at"), ds.get("created_at"), ds.get("entered_at")]
    # fallback: updated_at geral
    candidates.append(d.get("updated_at"))
    for v in candidates:
        if v:
            return v[:10]
    return ""

def load_funil_data(key, month, year):
    funil = FUNIS[key]
    pid   = funil["id"]

    tasks = {}
    with ThreadPoolExecutor(max_workers=20) as ex:
        for e in funil["etapas"]:
            tasks[ex.submit(fetch_stage_active, pid, e["id"])] = ("active", e)
        tasks[ex.submit(fetch_ok_stage, pid, funil["ok_stage_id"])] = ("ok", None)
        for e in funil["etapas"]:
            tasks[ex.submit(fetch_lost_stage, pid, e["id"])] = ("lost", e)
        # Buscar TODOS os deals das etapas pos-contrato (exibidas) + contrato_stage_id
        all_postcontrato_stage_ids = [e["id"] for e in funil["etapas"]] + [funil["contrato_stage_id"]]
        for sid in all_postcontrato_stage_ids:
            tasks[ex.submit(fetch_all, pid, sid)] = ("postcontrato_all", sid)
        # Buscar etapas pre-contrato (Desenvolvimento, Tem perfil) pelo nome via API
        for nome_lower in funil.get("pre_contrato_nomes", []):
            tasks[ex.submit(fetch_deals_by_stage_name, pid, nome_lower)] = ("pre_contrato", nome_lower)

    etapas_map        = {e["id"]: {**e, "deals": []} for e in funil["etapas"]}
    ok_deals          = []
    todas_perdas      = []
    postcontrato_pool = {}   # stage_id -> list of all deals (win=any)
    pre_contrato_map  = {}   # nome_lower -> list of deals

    for fut in as_completed(tasks, timeout=120):
        kind, meta = tasks[fut]
        try:
            result = fut.result()
        except Exception as e:
            print(f"   [WARN] task {kind}/{meta} falhou: {e}")
            result = [] if kind != "pre_contrato" else (None, [])
        if kind == "active":
            etapas_map[meta["id"]]["deals"] = result
        elif kind == "ok":
            ok_deals = result
        elif kind == "lost":
            todas_perdas.extend(result)
        elif kind == "postcontrato_all":
            sid = meta
            if sid not in postcontrato_pool:
                postcontrato_pool[sid] = []
            postcontrato_pool[sid].extend(result)
        elif kind == "pre_contrato":
            nome_lower = meta
            sid_result, deals_result = result  # tuple (sid, deals)
            pre_contrato_map[nome_lower] = {"sid": sid_result, "deals": deals_result}

    # ── etapas ativas filtradas pelo mes (para exibicao por etapa) ────────────
    # Assinatura eletronica: filtra por campo "Data da assinatura" no mes
    # Demais etapas: filtra por updated_at no mes
    assin_stage_id = funil.get("assin_stage_id", "")
    etapas_data = []
    for e in funil["etapas"]:
        all_active = etapas_map[e["id"]]["deals"]
        if e["id"] == assin_stage_id:
            # Assinatura eletronica: negociacoes com Data da assinatura no mes
            mes_active = [d for d in all_active
                          if custom_date_in_month(d, "Data da assinatura", month, year)]
        else:
            mes_active = [d for d in all_active if in_month(d, month, year, "updated_at")]
        etapas_data.append({**e, "deals": mes_active})

    # ── vendas do mes (pelo closed_at) ────────────────────────────────────────
    vendas_mes = [d for d in ok_deals
                  if d.get("closed_at") and in_month(d, month, year, "closed_at")]

    # ── CONTRATOS DO MES ──────────────────────────────────────────────────────
    # Usa o campo customizado "Data do contrato" para identificar contratos do mes.
    # Qualquer deal em qualquer etapa pos-contrato OU vendido (OK) que tenha
    # "Data do contrato" preenchida com data dentro do mes selecionado.
    seen_contrato = set()
    contratos_mes = []

    all_postcontrato_deals = []
    for sid, deals in postcontrato_pool.items():
        all_postcontrato_deals.extend(deals)
    all_postcontrato_deals.extend(ok_deals)

    for d in all_postcontrato_deals:
        if not custom_date_in_month(d, "Data do contrato", month, year):
            continue
        did = d.get("_id") or d.get("id")
        if did and did not in seen_contrato:
            seen_contrato.add(did)
            contratos_mes.append(d)

    # ── EM ANDAMENTO: negociacoes nas etapas Desenvolvimento / Tem perfil ────────
    # Permanece nessas etapas E foi atualizado (updated_at) no mes selecionado.
    # Usa pre_contrato_map (resultado do fetch_deals_by_stage_name por nome).
    # Se algum funil retornar vazio, loga para debug.
    em_andamento = []
    seen_ea = set()
    for nome_lower, info in pre_contrato_map.items():
        deals = info.get("deals") or []
        print(f"   [DEBUG EA] funil={key} etapa='{nome_lower}' total_deals={len(deals)}")
        for d in deals:
            if not in_month(d, month, year, "updated_at"):
                continue
            if d.get("win") is not None:  # perdido ou ganho — ignorar
                continue
            did = d.get("_id") or d.get("id")
            if did and did not in seen_ea:
                seen_ea.add(did)
                stage_name = (d.get("deal_stage") or {}).get("name") or nome_lower
                em_andamento.append({**d, "_pre_stage": stage_name})

    # ── assinaturas do mes ───────────────────────────────────────────────────
    # Deals com campo "Data da assinatura" dentro do mes selecionado
    seen_assin = set()
    assinaturas_mes = []
    for d in all_postcontrato_deals:
        if not custom_date_in_month(d, "Data da assinatura", month, year):
            continue
        did = d.get("_id") or d.get("id")
        if did and did not in seen_assin:
            seen_assin.add(did)
            assinaturas_mes.append(d)

    # ── busca paga ────────────────────────────────────────────────────────────
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

    # serializar deals
    def slim(d, extra_fields=None):
        """Versao reduzida do deal para serializar no JSON."""
        ds = d.get("deal_stage")
        dlr = d.get("deal_lost_reason")
        out = {
            "name":             d.get("name") or "",
            "user":             user_name(d),
            "updated_at":       d.get("updated_at") or "",
            "closed_at":        d.get("closed_at") or "",
            "amount_total":     d.get("amount_total") or 0,
            "origem":           get_origem(d),
            "fonte":            get_fonte(d),
            "deal_stage":       {"name": ds.get("name") or ""} if isinstance(ds, dict) else None,
            "deal_lost_reason": {"name": dlr.get("name") or ""} if isinstance(dlr, dict) else None,
            "data_contrato":     get_custom_date(d, "Data do contrato"),
            "data_assinatura":   get_custom_date(d, "Data da assinatura"),
            "data_contrato_fmt": fmt_custom_date(get_custom_date(d, "Data do contrato")),
            "data_assinatura_fmt": fmt_custom_date(get_custom_date(d, "Data da assinatura")),
        }
        return out

    # ── D+1 ───────────────────────────────────────────────────────────────────
    # D+1 = deals com "Data do contrato" == dia util anterior E "Data da assinatura" vazia.
    # Se a assinatura ja foi preenchida, sai da lista (negociacao encerrada).
    # Busca em todos os deals pos-contrato (independente de mes).
    today = datetime.date.today()
    weekday = today.weekday()  # 0=segunda
    if weekday == 0:
        prev_wd = today - datetime.timedelta(days=3)  # segunda -> sexta
    elif weekday == 6:
        prev_wd = today - datetime.timedelta(days=2)  # domingo -> sexta
    else:
        prev_wd = today - datetime.timedelta(days=1)

    prev_wd_str = str(prev_wd)
    today_str   = str(today)

    def slim_d1(d):
        dc = get_custom_date(d, "Data do contrato")
        da = get_custom_date(d, "Data da assinatura")
        return {
            "name":                d.get("name") or "",
            "user":                user_name(d),
            "data_contrato":       dc,
            "data_assinatura":     da,
            "data_contrato_fmt":   fmt_custom_date(dc),
            "data_assinatura_fmt": fmt_custom_date(da),
            "current_stage":       (d.get("deal_stage") or {}).get("name") or "",
            "updated_at":          d.get("updated_at") or "",
        }

    # Pool completo para D+1 (todos os deals pos-contrato + ok, sem filtro de mes)
    all_deals_pool = list({
        (d.get("_id") or d.get("id")): d
        for d in all_postcontrato_deals  # ja inclui ok_deals
    }.values())

    contrato_d1 = [
        slim_d1(d) for d in all_deals_pool
        if custom_date_equals(d, "Data do contrato", prev_wd_str)
        and not get_custom_date(d, "Data da assinatura")
    ]
    contrato_hoje = [
        slim_d1(d) for d in all_deals_pool
        if custom_date_equals(d, "Data do contrato", today_str)
        and not get_custom_date(d, "Data da assinatura")
    ]

    print(f"   [DEBUG D1] prev_wd={prev_wd_str} hoje={today_str} d1={len(contrato_d1)} hoje={len(contrato_hoje)}")
    print(f"   [DEBUG CONTRATOS] mes={len(contratos_mes)} pool={len(all_deals_pool)}")
    print(f"   [DEBUG EM_ANDAMENTO] pre_contrato={len(em_andamento)} etapas={list(pre_contrato_map.keys())}")

    # ── PRFB ativos (sem filtro de mes) ─────────────────────────────────────
    prfb_stage_id = funil.get("prfb_stage_id", "")
    prfb_ativos = []
    if prfb_stage_id and prfb_stage_id in etapas_map:
        prfb_ativos = [d for d in etapas_map[prfb_stage_id]["deals"]
                       if d.get("win") is None]
    elif prfb_stage_id:
        # buscar direto se nao estiver no etapas_map (pq foi removido da lista de etapas ativas)
        try:
            prfb_ativos = fetch_stage_active(pid, prfb_stage_id)
        except Exception:
            prfb_ativos = []

    return {
        "etapas":               [{**e, "deals": [slim(d) for d in e["deals"]]} for e in etapas_data],
        "vendas":               [slim(d) for d in vendas_mes],
        "contratos_mes":        [slim(d) for d in contratos_mes],
        "em_andamento":         [slim(d) for d in em_andamento],
        "perdas":               [slim(d) for d in todas_perdas],
        "feed":                 feed_candidates[:60],
        "vendas_busca_paga":    len(vendas_busca_paga),
        "contratos_busca_paga": len(contratos_busca_paga),
        "assinaturas_mes":      [slim(d) for d in assinaturas_mes],
        "contrato_d1":          contrato_d1,
        "contrato_hoje":        contrato_hoje,
        "prfb_ativos":          len(prfb_ativos),
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
.theme-btn{background:var(--surface2);border:1px solid var(--border2);color:var(--dim);padding:6px 12px;border-radius:6px;font-size:16px;cursor:pointer;transition:all .2s;line-height:1}
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
.summary-grid-5{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:12px;margin-bottom:2rem}
.total-hero{display:grid;grid-template-columns:repeat(3,1fr);gap:16px;margin-bottom:14px;justify-items:stretch}
.total-secondary{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:2rem;justify-items:stretch}
.summary-card.hero{padding:1.75rem 1.5rem}
.summary-card.hero .sc-label{font-size:13px;margin-bottom:.75rem}
.summary-card.hero .sc-val{font-size:52px}
.summary-card.hero .sc-sub{font-size:13px}
.summary-card{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:1.25rem;position:relative;overflow:hidden}
.summary-card::before{content:'';position:absolute;top:0;left:0;right:0;height:2px}
.summary-card.blue::before{background:var(--blue)}.summary-card.green::before{background:var(--green)}.summary-card.red::before{background:var(--red)}.summary-card.amber::before{background:var(--amber)}.summary-card.purple::before{background:var(--purple)}.summary-card.teal::before{background:var(--teal)}.summary-card.coral::before{background:var(--coral)}
.sc-label{font-size:12px;color:var(--dim);letter-spacing:.04em;text-transform:uppercase;margin-bottom:.5rem;font-weight:600}
.sc-val{font-size:38px;font-weight:700;line-height:1;margin-bottom:.3rem}
.sc-val.blue{color:var(--blue)}.sc-val.green{color:var(--green)}.sc-val.red{color:var(--red)}.sc-val.amber{color:var(--amber)}.sc-val.purple{color:var(--purple)}.sc-val.teal{color:var(--teal)}.sc-val.coral{color:var(--coral)}
.sc-sub{font-size:12px;color:var(--dim);font-family:'DM Mono',monospace}
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
.d1-badge{font-size:11px;font-family:'DM Mono',monospace;padding:3px 10px;border-radius:4px;border:1px solid;font-weight:600}
.d1-badge.amber{color:var(--amber);border-color:rgba(240,168,48,.4);background:var(--amber-dim)}
.d1-badge.blue{color:var(--blue);border-color:rgba(79,143,255,.4);background:var(--blue-dim)}
.d1-section{padding:.5rem 1.25rem .75rem}
.d1-section-lbl{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;margin-bottom:.5rem;font-family:'DM Mono',monospace;padding-top:.5rem;border-top:1px solid var(--border)}
.d1-section-lbl:first-child{border-top:none;padding-top:0}
.d1-item{display:flex;align-items:center;justify-content:space-between;padding:.3rem 0;border-bottom:1px solid var(--border)}
.d1-item:last-child{border-bottom:none}
.d1-name{font-size:13px;font-weight:600;color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:320px}
.d1-user{font-size:11px;color:var(--dim);font-family:'DM Mono',monospace}
.d1-tag{font-size:10px;font-family:'DM Mono',monospace;white-space:nowrap;padding:2px 8px;border-radius:4px}
.d1-empty{font-size:12px;color:var(--muted);font-family:'DM Mono',monospace;padding:.4rem 0}
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
const MN=['','Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez'];
const COLORS=['#4f8fff','#a78bfa','#3ecf8e','#f0a830','#fb923c','#2dd4bf','#f06060'];
const EXCLUDED=new Set(['Felipe Fernando','Luciano Santana']);

function toggleTheme(){
  const isLight=document.documentElement.classList.toggle('light');
  document.getElementById('tbtn').textContent=isLight?'🌑':'🌙';
  localStorage.setItem('theme',isLight?'light':'dark');
}
// restaurar tema salvo
(function(){if(localStorage.getItem('theme')==='light'){document.documentElement.classList.add('light');document.getElementById('tbtn').textContent='🌑';}})();

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
    const ctrl=new AbortController();
    const tid=setTimeout(()=>ctrl.abort(),90000); // 90s timeout
    setLoad(20,'Buscando Funil RP...');
    const rpRes=await fetch(`/api/data?funil=rp&month=${selM}&year=${selY}`,{signal:ctrl.signal});
    if(!rpRes.ok)throw new Error('RP: '+rpRes.statusText);
    const rp=await rpRes.json();
    setLoad(55,'Buscando Funil RRR Mae...');
    const rrrRes=await fetch(`/api/data?funil=rrr&month=${selM}&year=${selY}`,{signal:ctrl.signal});
    if(!rrrRes.ok)throw new Error('RRR: '+rrrRes.statusText);
    const rrr=await rrrRes.json();
    clearTimeout(tid);
    if(rp.error)console.warn('RP error:',rp.error);
    if(rrr.error)console.warn('RRR error:',rrr.error);
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
  const totAtivo=(s.em_andamento||[]).length;
  const totV=s.vendas.length,totC=(s.contratos_mes||[]).length,totP=s.perdas.length,totAssin=(s.assinaturas_mes||[]).length;
  const {proj,wdT,wdD,ritmo}=calcProj(totV,selM,selY);
  const pct=Math.min(100,Math.round((totV/Math.max(proj,1))*100));
  const mmap={};s.perdas.forEach(d=>{const m=d.deal_lost_reason?.name||'--';mmap[m]=(mmap[m]||0)+1;});
  const msorted=Object.entries(mmap).sort((a,b)=>b[1]-a[1]);

  let h=`<div class="summary-grid-5">
    <div class="summary-card blue"><div class="sc-label">Em andamento</div><div class="sc-val blue">${totAtivo}</div><div class="sc-sub">movidos no mes · Desenv. / Tem perfil</div></div>
    <div class="summary-card blue"><div class="sc-label">Contratos enviados - ${MN[selM]}/${String(selY).slice(2)}</div><div class="sc-val blue">${totC}</div><div class="sc-sub">campo Data do contrato</div></div>
    <div class="summary-card teal"><div class="sc-label">Assinaturas - ${MN[selM]}/${String(selY).slice(2)}</div><div class="sc-val teal">${totAssin}</div><div class="sc-sub">campo Data da assinatura</div></div>
    <div class="summary-card green"><div class="sc-label">Vendas - ${MN[selM]}/${String(selY).slice(2)}</div><div class="sc-val green">${totV}</div><div class="sc-sub">${wdD} dias uteis decorridos</div></div>
    <div class="summary-card amber"><div class="sc-label">Projecao do mes</div><div class="sc-val amber">${proj}</div><div class="sc-sub">${ritmo}/dia - ${wdT} dias uteis<div class="proj-bar-wrap"><div class="proj-bar" style="width:${pct}%;background:var(--amber)"></div></div></div></div>
    <div class="summary-card red"><div class="sc-label">Perdas</div><div class="sc-val red">${totP}</div><div class="sc-sub">historico total</div></div>
  </div>
  ${key==='rp'?`<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:2rem">
    <div class="summary-card purple"><div class="sc-label">Vendas por mídia social</div><div class="sc-val purple">${s.vendas_busca_paga||0}</div><div class="sc-sub">origem "busca" no mes</div></div>
    <div class="summary-card purple"><div class="sc-label">Contratos por mídia social</div><div class="sc-val purple">${s.contratos_busca_paga||0}</div><div class="sc-sub">contratos enviados via busca</div></div>
  </div>`:''}
  <div style="display:grid;grid-template-columns:1fr;gap:12px;margin-bottom:2rem">
    <div class="summary-card green"><div class="sc-label">Valor total estimativa no mes</div><div class="sc-val green" style="font-size:24px">${fmoney(s.vendas.reduce((a,d)=>a+(d.amount_total||0),0))}</div><div class="sc-sub">soma das vendas fechadas no mes</div></div>
  </div>`;

  // responsaveis — logo apos valor total estimativa
  const umap={};
  s.etapas.forEach(e=>e.deals.forEach(d=>{const u=uname(d);if(!umap[u])umap[u]={ativo:0,et:{},vendas:[],contratos:[],perdas:0,valor:0};umap[u].et[e.nome]=(umap[u].et[e.nome]||0)+1;}));
  (s.em_andamento||[]).forEach(d=>{const u=uname(d);if(!umap[u])umap[u]={ativo:0,et:{},vendas:[],contratos:[],perdas:0,valor:0};umap[u].ativo++;});
  s.vendas.forEach(d=>{const u=uname(d);if(!umap[u])umap[u]={ativo:0,et:{},vendas:[],contratos:[],perdas:0,valor:0};umap[u].vendas.push(d);umap[u].valor+=(d.amount_total||0);});
  if(s.contratos_mes)s.contratos_mes.forEach(d=>{const u=uname(d);if(!umap[u])umap[u]={ativo:0,et:{},vendas:[],contratos:[],perdas:0,valor:0};umap[u].contratos.push(d);});
  s.perdas.forEach(d=>{const u=uname(d);if(!umap[u])umap[u]={ativo:0,et:{},vendas:[],contratos:[],perdas:0,valor:0};umap[u].perdas++;});
  const users=Object.entries(umap).filter(([n])=>!EXCLUDED.has(n)).sort((a,b)=>b[1].contratos.length-a[1].contratos.length||b[1].vendas.length-a[1].vendas.length);

  h+=`<div class="section-hd"><h3>Por responsavel</h3><span class="cnt blue">${users.length} vendedores</span><div class="section-line"></div></div><div class="resp-grid">`;
  users.forEach(([name,data],i)=>{
    const color=COLORS[i%COLORS.length],init=name.split(' ').slice(0,2).map(w=>w[0]||'').join('').toUpperCase();
    h+=`<div class="resp-card">
      <div class="resp-header">
        <div class="resp-avatar" style="background:${color}22;color:${color}">${init}</div>
        <div style="flex:1;min-width:0"><div style="display:flex;align-items:center;gap:8px"><div class="resp-name">${name}</div><span style="font-family:'DM Mono',monospace;font-size:13px;font-weight:700;color:var(--blue)">${data.contratos.length}c</span></div>
        <div style="font-size:11px;color:var(--muted);font-family:'DM Mono',monospace">${data.contratos.length} contratos · ${data.vendas.length} vendas no mes</div></div>
        <div class="resp-total" style="color:${color}">${data.contratos.length}</div>
      </div><div class="resp-rows">`;
    s.etapas.forEach(e=>{const c=data.et[e.nome]||0;if(!c)return;h+=`<div class="resp-row"><span class="resp-row-label"><span class="resp-row-dot" style="background:${e.cor}"></span>${e.nome}</span><span class="resp-row-val" style="color:${e.cor}">${c}</span></div>`;});
    h+=`<hr class="resp-divider">`;
    h+=`<div class="resp-row"><span class="resp-row-label"><span class="resp-row-dot" style="background:var(--blue)"></span>Contratos enviados</span><span class="resp-row-val" style="color:var(--blue)">${data.contratos.length}</span></div>`;
    if(data.contratos.length){h+=`<div class="resp-deal-list">`;data.contratos.forEach(d=>{h+=`<div class="resp-deal-item"><span class="resp-deal-name" title="${d.name||''}">${d.name||'--'}</span><span class="resp-deal-date">${d.data_contrato_fmt||'--'}</span></div>`;});h+=`</div>`;}
    h+=`<div class="resp-row"><span class="resp-row-label"><span class="resp-row-dot" style="background:var(--green)"></span>Vendas</span><span class="resp-row-val" style="color:var(--green)">${data.vendas.length}</span></div>`;
    if(data.vendas.length){h+=`<div class="resp-deal-list">`;data.vendas.forEach(d=>{h+=`<div class="resp-deal-item"><span class="resp-deal-name" title="${d.name||''}">${d.name||'--'}</span><span class="resp-deal-date">${fdate(d.closed_at)}</span></div>`;});h+=`</div>`;}
    h+=`<div class="resp-row"><span class="resp-row-label"><span class="resp-row-dot" style="background:var(--red)"></span>Perdas</span><span class="resp-row-val" style="color:var(--red)">${data.perdas}</span></div>
    <div class="resp-row"><span class="resp-row-label"><span class="resp-row-dot" style="background:var(--teal)"></span>Valor vendas no mes</span><span class="resp-row-val" style="color:var(--teal);font-size:12px">${fmoney(data.valor)}</span></div>
    </div></div>`;
  });
  h+=`</div>`;

  h+=renderFeed(s.feed,15);

  // em andamento — Desenvolvimento / Tem perfil
  const eaList=s.em_andamento||[];
  h+=`<div class="section-hd" style="margin-top:2rem"><h3>Em andamento — Desenvolvimento / Tem perfil</h3><span class="cnt blue">${eaList.length} negociacoes</span><div class="section-line"></div></div>`;
  if(!eaList.length){h+=`<div class="empty" style="background:var(--surface);border:1px solid var(--border);border-radius:10px;margin-bottom:2rem">Nenhuma negociacao nessas etapas no mes</div>`;}
  else{
    h+=`<div class="stage-row" id="ea-${key}"><div class="stage-header" onclick="tog('ea-${key}')"><div class="stage-color" style="background:var(--blue)"></div><span class="stage-name">Desenvolvimento / Tem perfil — ${MN[selM]}/${selY}</span><span class="stage-count" style="color:var(--blue)">${eaList.length}</span><span class="stage-arrow">&#9654;</span></div><div class="stage-deals"><table class="dt"><thead><tr><th>Negociacao</th><th>Responsavel</th><th>Etapa</th><th>Movido em</th></tr></thead><tbody>`;
    eaList.forEach(d=>{
      const stgName=d.deal_stage?.name||d._pre_stage||'--';
      h+=`<tr><td class="dn">${d.name||'--'}</td><td class="du">${uname(d)}</td><td class="dd">${stgName}</td><td class="dd">${fdate(d.updated_at)}</td></tr>`;
    });
    h+=`</tbody></table></div></div>`;
  }

  // etapas
  h+=`<div class="section-hd"><h3>Negociacoes por etapa (pos-contrato)</h3><span class="cnt blue">${s.etapas.reduce((a,e)=>a+e.deals.length,0)} ativas</span><div class="section-line"></div></div><div class="stages-wrap">`;
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
  else{h+=`<div class="tw" style="border-color:rgba(79,143,255,.2);margin-bottom:2rem"><table class="dt"><thead><tr><th>Negociacao</th><th>Responsavel</th><th>Data</th></tr></thead><tbody>`;s.contratos_mes.forEach(d=>{h+=`<tr><td class="dn" style="color:var(--blue)">${d.name||'--'}</td><td class="du">${uname(d)}</td><td class="dd">${d.data_contrato_fmt||'--'}</td></tr>`;});h+=`</tbody></table></div>`;}

  // vendas do mes
  h+=`<div class="section-hd"><h3>Vendas fechadas - ${MN[selM]}/${selY}</h3><span class="cnt green">${totV} total</span><div class="section-line"></div></div>`;
  if(!totV){h+=`<div class="empty" style="background:var(--surface);border:1px solid var(--border);border-radius:10px;margin-bottom:2rem">Nenhuma venda neste periodo</div>`;}
  else{h+=`<div class="tw" style="border-color:rgba(62,207,142,.2);margin-bottom:2rem"><table class="dt"><thead><tr><th>Negociacao</th><th>Responsavel</th><th>Fechado em</th></tr></thead><tbody>`;s.vendas.forEach(d=>{h+=`<tr><td class="dn" style="color:var(--green)">${d.name||'--'}</td><td class="du">${uname(d)}</td><td class="dd">${fdate(d.closed_at)}</td></tr>`;});h+=`</tbody></table></div>`;}

  h+=renderContratosPorDia(s.contratos_mes,key);

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
  const rpA=(rp.em_andamento||[]).length,rrrA=(rrr.em_andamento||[]).length,totA=rpA+rrrA;
  const totP=rp.perdas.length+rrr.perdas.length;
  const rpAssin=(rp.assinaturas_mes||[]).length,rrrAssin=(rrr.assinaturas_mes||[]).length,totAssin=rpAssin+rrrAssin;
  const totPRFB=(rp.prfb_ativos||0)+(rrr.prfb_ativos||0);
  const {proj,wdT,wdD,ritmo}=calcProj(totV,selM,selY);
  const pct=Math.min(100,Math.round((totV/Math.max(proj,1))*100));

  const totBuscaPagaV=(rp.vendas_busca_paga||0)+(rrr.vendas_busca_paga||0);
  const totBuscaPagaC=(rp.contratos_busca_paga||0)+(rrr.contratos_busca_paga||0);
  let h=`
  <div class="total-hero">
    <div class="summary-card blue hero"><div class="sc-label">Contratos enviados — ${MN[selM]}/${String(selY).slice(2)}</div><div class="sc-val blue">${totC}</div><div class="sc-sub">RP: ${rpC} &nbsp;·&nbsp; RRR: ${rrrC}</div></div>
    <div class="summary-card teal hero"><div class="sc-label">Assinaturas — ${MN[selM]}/${String(selY).slice(2)}</div><div class="sc-val teal">${totAssin}</div><div class="sc-sub">RP: ${rpAssin} &nbsp;·&nbsp; RRR: ${rrrAssin}</div></div>
    <div class="summary-card green hero"><div class="sc-label">Vendas — ${MN[selM]}/${String(selY).slice(2)}</div><div class="sc-val green">${totV}</div><div class="sc-sub">RP: ${rpV} &nbsp;·&nbsp; RRR: ${rrrV}</div></div>
  </div>
  <div class="total-secondary">
    <div class="summary-card blue"><div class="sc-label">Em andamento</div><div class="sc-val blue">${totA}</div><div class="sc-sub">Desenv. / Tem perfil no mes</div></div>
    <div class="summary-card coral"><div class="sc-label">PRFB — ambos funis</div><div class="sc-val coral">${totPRFB}</div><div class="sc-sub">negociacoes ativas na etapa</div></div>
    <div class="summary-card amber"><div class="sc-label">Projecao do mes</div><div class="sc-val amber">${proj}</div><div class="sc-sub">${ritmo}/dia · ${wdT} dias uteis<div class="proj-bar-wrap"><div class="proj-bar" style="width:${pct}%;background:var(--amber)"></div></div></div></div>
    <div class="summary-card red"><div class="sc-label">Perdas — ambos funis</div><div class="sc-val red">${totP}</div><div class="sc-sub">historico total</div></div>
  </div>`
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:2rem">
    <div class="summary-card purple"><div class="sc-label">Vendas por mídia social</div><div class="sc-val purple">${totBuscaPagaV}</div><div class="sc-sub">ambos funis · origem "busca"</div></div>
    <div class="summary-card purple"><div class="sc-label">Contratos por mídia social</div><div class="sc-val purple">${totBuscaPagaC}</div><div class="sc-sub">contratos enviados via busca</div></div>
  </div>
  <div style="display:grid;grid-template-columns:1fr;gap:12px;margin-bottom:2rem">
    <div class="summary-card green"><div class="sc-label">Valor total estimativa no mes</div><div class="sc-val green" style="font-size:22px">${fmoney([...rp.vendas,...rrr.vendas].reduce((a,d)=>a+(d.amount_total||0),0))}</div><div class="sc-sub">soma das vendas fechadas - ambos funis</div></div>
  </div>`;

  // responsaveis totais — logo apos valor total
  const umap={};
  function addU(u,f,d){if(!umap[u])umap[u]={ativo:0,vendas:[],contratos:[],perdas:0,valor:0};if(f==='ativo')umap[u].ativo++;else if(f==='perda')umap[u].perdas++;else{umap[u][f].push(d);if(f==='vendas')umap[u].valor+=(d.amount_total||0);}}
  rp.vendas.forEach(d=>addU(uname(d),'vendas',d));
  rrr.vendas.forEach(d=>addU(uname(d),'vendas',d));
  if(rp.contratos_mes)rp.contratos_mes.forEach(d=>addU(uname(d),'contratos',d));
  if(rrr.contratos_mes)rrr.contratos_mes.forEach(d=>addU(uname(d),'contratos',d));
  rp.perdas.forEach(d=>addU(uname(d),'perda',d));
  rrr.perdas.forEach(d=>addU(uname(d),'perda',d));
  (rp.em_andamento||[]).forEach(d=>addU(uname(d),'ativo',d));
  (rrr.em_andamento||[]).forEach(d=>addU(uname(d),'ativo',d));
  const users=Object.entries(umap).filter(([n])=>!EXCLUDED.has(n)).sort((a,b)=>b[1].contratos.length-a[1].contratos.length||b[1].vendas.length-a[1].vendas.length);
  h+=`<div class="section-hd"><h3>Por responsavel - Total (ambos funis)</h3><span class="cnt amber">${users.length} vendedores</span><div class="section-line"></div></div><div class="resp-grid">`;
  users.forEach(([name,data],i)=>{
    const color=COLORS[i%COLORS.length],init=name.split(' ').slice(0,2).map(w=>w[0]||'').join('').toUpperCase();
    const uid=`tot-${i}`;
    h+=`<div class="resp-card">
      <div class="resp-header">
        <div class="resp-avatar" style="background:${color}22;color:${color}">${init}</div>
        <div style="flex:1;min-width:0"><div style="display:flex;align-items:center;gap:8px"><div class="resp-name">${name}</div><span style="font-family:'DM Mono',monospace;font-size:13px;font-weight:700;color:var(--blue)">${data.contratos.length}c</span></div>
        <div style="font-size:11px;color:var(--muted);font-family:'DM Mono',monospace">${data.contratos.length} contratos · ${data.vendas.length} vendas no mes</div></div>
        <div class="resp-total" style="color:${color}">${data.contratos.length}</div>
      </div><div class="resp-rows">`;
    h+=`<div class="resp-row" style="cursor:${data.contratos.length?'pointer':'default'}" onclick="${data.contratos.length?`togInline('${uid}-c','${uid}-c-arrow')`:''}">
      <span class="resp-row-label"><span class="resp-row-dot" style="background:var(--blue)"></span>Contratos enviados</span>
      <span style="display:flex;align-items:center;gap:6px"><span class="resp-row-val" style="color:var(--blue)">${data.contratos.length}</span>${data.contratos.length?`<span id="${uid}-c-arrow" style="font-size:10px;color:var(--muted);display:inline-block;transition:transform .2s">&#9654;</span>`:''}</span>
    </div>`;
    if(data.contratos.length){
      h+=`<div id="${uid}-c" style="display:none;border-top:1px solid var(--border)"><div class="resp-deal-list">`;
      data.contratos.forEach(d=>{h+=`<div class="resp-deal-item"><span class="resp-deal-name" title="${d.name||''}">${d.name||'--'}</span><span class="resp-deal-date">${d.data_contrato_fmt||'--'}</span></div>`;});
      h+=`</div></div>`;
    }
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
    <div class="resp-row"><span class="resp-row-label"><span class="resp-row-dot" style="background:var(--teal)"></span>Valor vendas no mes</span><span class="resp-row-val" style="color:var(--teal);font-size:12px">${fmoney(data.valor)}</span></div>
    </div></div>`;
  });
  h+=`</div>`;

  // split RP x RRR
  const etapasNomes='Contrato enviado, Assinatura eletronica, Fazendo estimativa, Preparando PDF, Apresentar, PRFB, C4';
  h+=`<div class="total-split">
    <div class="total-funil-block"><div class="total-funil-title">Funil Comercial RP</div>
      <div class="total-row"><span class="total-row-label"><span class="resp-row-dot" style="background:var(--blue)"></span>Contratos enviados</span><span class="total-row-val" style="color:var(--blue)">${rpC}</span></div>
      <div class="total-row"><span class="total-row-label"><span class="resp-row-dot" style="background:var(--teal)"></span>Assinaturas</span><span class="total-row-val" style="color:var(--teal)">${rpAssin}</span></div>
      <div class="total-row"><span class="total-row-label"><span class="resp-row-dot" style="background:var(--green)"></span>Vendas fechadas</span><span class="total-row-val" style="color:var(--green)">${rpV}</span></div>
      <div class="total-row"><span class="total-row-label"><span class="resp-row-dot" style="background:var(--blue)"></span>Em andamento (Desenv./Tem perfil)</span><span class="total-row-val" style="color:var(--blue)">${rpA}</span></div>
      <div style="margin-top:.5rem;font-size:10px;color:var(--muted);font-family:'DM Mono',monospace;line-height:1.6">Etapas: ${etapasNomes}</div>
    </div>
    <div class="total-funil-block"><div class="total-funil-title">Funil Comercial RRR Mae</div>
      <div class="total-row"><span class="total-row-label"><span class="resp-row-dot" style="background:var(--blue)"></span>Contratos enviados</span><span class="total-row-val" style="color:var(--blue)">${rrrC}</span></div>
      <div class="total-row"><span class="total-row-label"><span class="resp-row-dot" style="background:var(--teal)"></span>Assinaturas</span><span class="total-row-val" style="color:var(--teal)">${rrrAssin}</span></div>
      <div class="total-row"><span class="total-row-label"><span class="resp-row-dot" style="background:var(--green)"></span>Vendas fechadas</span><span class="total-row-val" style="color:var(--green)">${rrrV}</span></div>
      <div class="total-row"><span class="total-row-label"><span class="resp-row-dot" style="background:var(--blue)"></span>Em andamento (Desenv./Tem perfil)</span><span class="total-row-val" style="color:var(--blue)">${rrrA}</span></div>
      <div style="margin-top:.5rem;font-size:10px;color:var(--muted);font-family:'DM Mono',monospace;line-height:1.6">Etapas: ${etapasNomes}</div>
    </div>
  </div>`;

  // em andamento — Desenvolvimento / Tem perfil (ambos funis)
  const eaRP=(rp.em_andamento||[]).map(d=>({...d,_f:'RP'}));
  const eaRRR=(rrr.em_andamento||[]).map(d=>({...d,_f:'RRR'}));
  const eaTot=[...eaRP,...eaRRR];
  h+=`<div class="section-hd" style="margin-top:2rem"><h3>Em andamento — Desenvolvimento / Tem perfil</h3><span class="cnt blue">${eaTot.length} negociacoes</span><div class="section-line"></div></div>`;
  if(!eaTot.length){h+=`<div class="empty" style="background:var(--surface);border:1px solid var(--border);border-radius:10px;margin-bottom:2rem">Nenhuma negociacao nessas etapas no mes</div>`;}
  else{
    h+=`<div class="stage-row" id="ea-total"><div class="stage-header" onclick="tog('ea-total')"><div class="stage-color" style="background:var(--blue)"></div><span class="stage-name">Desenvolvimento / Tem perfil — ${MN[selM]}/${selY}</span><span class="stage-count" style="color:var(--blue)">${eaTot.length}</span><span class="stage-arrow">&#9654;</span></div><div class="stage-deals"><table class="dt"><thead><tr><th>Negociacao</th><th>Responsavel</th><th>Funil</th><th>Etapa</th><th>Movido em</th></tr></thead><tbody>`;
    eaTot.forEach(d=>{
      const stgName=d.deal_stage?.name||d._pre_stage||'--';
      h+=`<tr><td class="dn">${d.name||'--'}</td><td class="du">${uname(d)}</td><td class="dd">${d._f}</td><td class="dd">${stgName}</td><td class="dd">${fdate(d.updated_at)}</td></tr>`;
    });
    h+=`</tbody></table></div></div>`;
  }

  // feed combinado — ULTIMO, apenas 2 itens
  const feedCombo=[...(rp.feed||[]),...(rrr.feed||[])].sort((a,b)=>b.ts.localeCompare(a.ts));
  h+=renderFeed(feedCombo,2);
  h+=renderD1(rp.contrato_d1,rrr.contrato_d1,rp.contrato_hoje,rrr.contrato_hoje,'total');
  h+=renderContratosPorDia([...(rp.contratos_mes||[]),...(rrr.contratos_mes||[])],'total');

  // perdas combinadas com motivos
  const todasPerdas=[...rp.perdas,...rrr.perdas];
  const mmapT={};todasPerdas.forEach(d=>{const m=d.deal_lost_reason?.name||'--';mmapT[m]=(mmapT[m]||0)+1;});
  const msortedT=Object.entries(mmapT).sort((a,b)=>b[1]-a[1]);
  h+=`<div class="section-hd" style="margin-top:2rem"><h3>Perdas - ambos funis</h3><span class="cnt red">${totP} total</span><div class="section-line"></div></div>`;
  h+=`<div class="motivos-grid">`+msortedT.map(([m,c])=>`<div class="mc"><span class="mc-n">${m}</span><span class="mc-v">${c}</span></div>`).join('')+`</div>`;
  if(totP){
    h+=`<div class="stage-row" id="perdas-total" style="margin-bottom:2rem"><div class="stage-header" onclick="tog('perdas-total')"><div class="stage-color" style="background:var(--red)"></div><span class="stage-name">Negociacoes perdidas</span><span class="stage-count" style="color:var(--red)">${totP}</span><span class="stage-arrow">&#9654;</span></div><div class="stage-deals"><table class="dt"><thead><tr><th>Negociacao</th><th>Responsavel</th><th>Funil</th><th>Etapa</th><th>Motivo</th></tr></thead><tbody>`;
    [...rp.perdas.map(d=>({...d,_funil:'RP'})),...rrr.perdas.map(d=>({...d,_funil:'RRR'}))].forEach(d=>{
      const m=d.deal_lost_reason?.name||'--';
      h+=`<tr><td class="dn">${d.name||'--'}</td><td class="du">${uname(d)}</td><td class="dd">${d._funil}</td><td class="dd">${d.deal_stage?.name||'--'}</td><td><span class="pill">${m}</span></td></tr>`;
    });
    h+=`</tbody></table></div></div>`;
  }

  return h;
}

function prevWorkday(date){
  const d=new Date(date);
  do{d.setDate(d.getDate()-1);}while(d.getDay()===0||d.getDay()===6);
  return d;
}
function isoDate(d){return`${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;}

function renderD1(rpD1,rrrD1,rpHoje,rrrHoje,paneKey){
  const isMonday=new Date().getDay()===1;
  const d1Lbl=isMonday?'Sexta-feira':'Ontem';

  // montar listas com tag de funil
  let d1=[], dHoje=[];
  if(paneKey==='rp'){
    d1=[...(rpD1||[]).map(d=>({...d,_f:'RP'}))];
    dHoje=[...(rpHoje||[]).map(d=>({...d,_f:'RP'}))];
  }else if(paneKey==='rrr'){
    d1=[...(rrrD1||[]).map(d=>({...d,_f:'RRR'}))];
    dHoje=[...(rrrHoje||[]).map(d=>({...d,_f:'RRR'}))];
  }else{
    d1=[...(rpD1||[]).map(d=>({...d,_f:'RP'})),...(rrrD1||[]).map(d=>({...d,_f:'RRR'}))];
    dHoje=[...(rpHoje||[]).map(d=>({...d,_f:'RP'})),...(rrrHoje||[]).map(d=>({...d,_f:'RRR'}))];
  }

  let h=`<div class="d1-wrap">
    <div class="d1-hd">
      <span class="d1-title">&#9200; Contrato enviado — Acompanhamento diario</span>
      <span class="d1-badge amber">D+1: ${d1.length} sem assinatura</span>
      <span class="d1-badge blue">Hoje: ${dHoje.length} novos</span>
    </div><div class="d1-section">`;

  h+=`<div class="d1-section-lbl" style="border-top:none;padding-top:0">D+1 — contrato em ${d1Lbl}, aguardando assinatura</div>`;
  if(!d1.length){h+=`<div class="d1-empty">Nenhum contrato sem assinatura no dia util anterior</div>`;}
  else{d1.forEach(d=>{const stg=d.current_stage?` <span style="font-size:9px;color:var(--teal)">[${d.current_stage}]</span>`:'';h+=`<div class="d1-item"><div><div class="d1-name">${d.name||'--'}${stg}</div><div class="d1-user">${d.user||'--'}${paneKey==='total'?' · '+d._f:''} · contrato: ${d.data_contrato_fmt||'--'}</div></div><span class="d1-tag" style="color:var(--amber);background:var(--amber-dim)">aguard. assin.</span></div>`;});}

  h+=`<div class="d1-section-lbl">Contrato hoje — aguardando assinatura</div>`;
  if(!dHoje.length){h+=`<div class="d1-empty">Nenhum contrato hoje sem assinatura</div>`;}
  else{dHoje.forEach(d=>{const stg=d.current_stage?` <span style="font-size:9px;color:var(--teal)">[${d.current_stage}]</span>`:'';h+=`<div class="d1-item"><div><div class="d1-name">${d.name||'--'}${stg}</div><div class="d1-user">${d.user||'--'}${paneKey==='total'?' · '+d._f:''} · contrato: ${d.data_contrato_fmt||'--'}</div></div><span class="d1-tag" style="color:var(--blue);background:var(--blue-dim)">hoje</span></div>`;});}

  h+=`</div></div>`;
  return h;
}

function renderContratosPorDia(contratos,paneKey){
  // usa contratos_mes (already filtered to selM/selY by backend)
  // agrupa por dia do updated_at, somente do mes selecionado
  const byDay={};
  (contratos||[]).forEach(d=>{
    const dt=(d.data_contrato||(d.updated_at||'')).slice(0,10);
    if(!dt)return;
    const [y,m]=dt.split('-').map(Number);
    if(m!==selM||y!==selY)return;
    byDay[dt]=(byDay[dt]||0)+1;
  });
  const daysInMonth=new Date(selY,selM,0).getDate();
  const todayStr=isoDate(new Date());
  const total=Object.values(byDay).reduce((a,b)=>a+b,0);
  const idSuffix=paneKey;
  let h=`<div class="stage-row" id="cpd-${idSuffix}" style="margin-bottom:2rem">
    <div class="stage-header" onclick="tog('cpd-${idSuffix}')">
      <div class="stage-color" style="background:var(--blue)"></div>
      <span class="stage-name">Contratos enviados por dia — ${MN[selM]}/${selY}</span>
      <span class="stage-count" style="color:var(--blue)">${total}</span>
      <span class="stage-arrow">&#9654;</span>
    </div>
    <div class="stage-deals"><table class="dt"><thead><tr><th>Dia</th><th>Contratos</th></tr></thead><tbody>`;
  for(let d=1;d<=daysInMonth;d++){
    const key=`${selY}-${String(selM).padStart(2,'0')}-${String(d).padStart(2,'0')}`;
    const qty=byDay[key]||0;
    const isToday=key===todayStr;
    const lbl=`${String(d).padStart(2,'0')}/${String(selM).padStart(2,'0')}${isToday?' (hoje)':''}`;
    h+=`<tr><td class="dd" style="${isToday?'color:var(--green);font-weight:700':''}">${lbl}</td><td style="font-family:'DM Mono',monospace;font-size:13px;padding:.6rem 1.25rem;${qty>0?'color:var(--blue);font-weight:700':'color:var(--muted)'}">${qty}</td></tr>`;
  }
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
        print(f"  {' '.join(str(a) for a in args)}")

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
                print(f"   OK  vendas={len(data['vendas'])}  contratos={len(data['contratos_mes'])}  feed={len(data['feed'])}  d1={len(data['contrato_d1'])}  hoje={len(data['contrato_hoje'])}")
                # debug: mostrar campos de data do primeiro deal de contrato ativo
                if key == "rp" and data.get("contrato_d1"):
                    import pprint
                    print("   [DEBUG] exemplo d1:", data["contrato_d1"][0])
                elif key == "rp" and data.get("contrato_hoje"):
                    print("   [DEBUG] exemplo hoje:", data["contrato_hoje"][0])
            except Exception as e:
                import traceback; traceback.print_exc()
                self.send_json({"error": str(e), "etapas": [], "vendas": [],
                    "contratos_mes": [], "em_andamento": [], "perdas": [],
                    "feed": [], "vendas_busca_paga": 0, "contratos_busca_paga": 0,
                    "assinaturas_mes": [], "contrato_d1": [], "contrato_hoje": [],
                    "prfb_ativos": 0}, 500)
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
