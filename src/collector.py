
# src/collector.py
# -*- coding: utf-8 -*-
"""
Coletor de 'Last login' para membros da guild True Knife no RubinOT.

Fluxo:
1) Lê data/players.json (lista nominal enviada por você).
2) Abre a página da guild True Knife e extrai os links dos perfis dos membros.
3) Filtra para a INTERSEÇÃO: somente jogadores que estão na sua lista E na guild.
4) Para cada jogador focado, acessa o perfil e extrai 'Last login'.
5) Persiste snapshot (JSON) e histórico (CSV).

Referências públicas:
- Página de personagem exibe 'Last login:' e segue '?subtopic=characters&name=<nome>'.
- Página de guild segue '?subtopic=guilds&page=view&GuildName=<nome>' e lista membros com links.
"""

import os
import re
import csv
import json
from datetime import datetime
from urllib.parse import urlparse, urljoin, quote_plus

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dtparser  # python-dateutil

# Diretórios
BASE_DIR = os.path.dirname(os.path.dirname(__file__))   # raiz do projeto
DATA_DIR = os.path.join(BASE_DIR, "data")
SNAP_DIR = os.path.join(DATA_DIR, "snapshots")
HISTORY_FILE = os.path.join(DATA_DIR, "history.csv")
PLAYERS_FILE = os.path.join(DATA_DIR, "players.json")

# URL fixa da guild True Knife (pode ser sobrescrita por variável de ambiente GUILD_URL, se quiser)
GUILD_URL = os.environ.get(
    "GUILD_URL",
    "https://rubinot.com.br/?subtopic=guilds&page=view&GuildName=True+Knife"
).strip()

# Cabeçalhos para evitar bloqueios por user-agent padrão
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; RubinotGuildMonitor/1.0; +https://github.com/)",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml"
}

# -------- utilidades --------

def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(SNAP_DIR, exist_ok=True)

def load_target_players():
    """
    Lê 'data/players.json' e retorna set de nomes (normalizados com strip).
    Levanta erro se o arquivo não existir.
    """
    path = PLAYERS_FILE
    if not os.path.isfile(path):
        raise FileNotFoundError(
            f"Não encontrei {path}. Crie a pasta 'data/' e o arquivo 'players.json' com os nomes."
        )
    with open(path, "r", encoding="utf-8") as f:
        players = json.load(f)

    players = [p.strip() for p in players if isinstance(p, str) and p.strip()]
    return set(players)

def get_guild_member_links(guild_url: str):
    """
    Acessa a página da guild e retorna:
      - member_links: dict {nome -> URL absoluta do perfil}
      - profile_base: base para montar perfil quando não houver link (fallback)
    Critério: anchors <a> cujo href contenha 'subtopic=characters'.
    """
    resp = requests.get(guild_url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    member_links = {}

    # Anchors que levam ao perfil do personagem
    for a in soup.select("a[href*='subtopic=characters']"):
        name = a.get_text(strip=True)
        href = a.get("href", "")
        if not name or not href:
            continue
        abs_url = urljoin(guild_url, href)
        member_links[name] = abs_url

    # Se não achou links, retorna uma base para tentar montar URL de perfil
    if not member_links:
        parsed = urlparse(guild_url)
        profile_base = f"{parsed.scheme}://{parsed.netloc}/?subtopic=characters&name="
        return {}, profile_base

    return member_links, None

def fetch_last_login(profile_url: str):
    """
    Acessa a página de perfil do personagem e retorna:
      - last_login_raw: string exatamente como aparece na página
      - last_login_iso: ISO 8601, se conseguir parsear
    Busca por 'Last login' (case-insensitive) no HTML.
    """
    resp = requests.get(profile_url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    # 1) Procura nó de texto com 'Last login'
    candidate = soup.find(string=re.compile(r"last\\s*login", re.IGNORECASE))
    last_str = None
    if candidate:
        text = candidate.strip()
        # Ex.: "Last login: 24/04/2024, 15:28:07"
        parts = re.split(r":\\s*", text, maxsplit=1)
        if len(parts) == 2 and parts[1].strip():
            last_str = parts[1].strip()

    # 2) Se não achou diretamente, tenta vizinhança/irmãos próximos
    if not last_str:
        for el in soup.find_all(text=re.compile(r"last\\s*login", re.IGNORECASE)):
            parent = el.parent
            if parent:
                # procura textos próximos que pareçam data
                for sib in parent.find_all_next(string=True, limit=4):
                    t = (sib or "").strip()
                    if not t or t == el.strip():
                        continue
                    if re.search(r"\\d{1,2}/\\d{1,2}/\\d{2,4}", t) or re.search(r"\\d{4}-\\d{2}-\\d{2}", t):
                        last_str = t
                        break
            if last_str:
                break

    # 3) Tenta converter para ISO (RubinOT costuma usar dd/mm/yyyy HH:MM:SS)
    last_iso = None
    if last_str:
        try:
            dt = dtparser.parse(last_str, dayfirst=True)
            last_iso = dt.isoformat()
        except Exception:
            pass

    return last_str, last_iso

def append_history(collection_ts_iso: str, rows):
    """
    rows: lista de dicts com {'player','profile_url','last_login_raw','last_login_iso'}
    """
    file_exists = os.path.isfile(HISTORY_FILE)
    with open(HISTORY_FILE, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(["collected_at", "player", "profile_url", "last_login_raw", "last_login_iso"])
        for r in rows:
            w.writerow([
                collection_ts_iso,
                r.get("player"),
                r.get("profile_url"),
                r.get("last_login_raw"),
                r.get("last_login_iso"),
            ])

def save_snapshot(collection_ts_iso: str, rows):
    """Salva snapshot JSON com os dados coletados."""
    fname = os.path.join(SNAP_DIR, f"snapshot_{collection_ts_iso.replace(':','-')}.json")
    with open(fname, "w", encoding="utf-8") as f:
        json.dump({
            "timestamp": collection_ts_iso,
            "guild_url": GUILD_URL,
            "players": rows
        }, f, ensure_ascii=False, indent=2)

# -------- execução --------

def main():
    ensure_dirs()

    # 1) Carrega sua lista nominal
    target = load_target_players()
    if not target:
        print("[ERRO] Lista de jogadores vazia em data/players.json.")
        return

    # 2) Lê membros da guild True Knife pela página
    member_links, profile_base = get_guild_member_links(GUILD_URL)

    # Segurança: se não conseguimos obter NENHUMA forma de confirmar membros, aborta
    if not member_links and not profile_base:
        print("[ERRO] Não foi possível obter membros na página da guild. Verifique GUILD_URL.")
        return

    guild_members = set(member_links.keys()) if member_links else set()

    # 3) Foco EXCLUSIVO: interseção entre sua lista e os membros da guild
    if guild_members:
        focus = sorted(target & guild_members)
    else:
        # Se não conseguimos listar membros (apenas uma base para construir URL),
        # não temos como garantir que um nome é da guild -> aborta para manter exclusividade.
        print("[ERRO] Não consegui confirmar a lista de membros da guild. Abortando para manter foco exclusivo.")
        return

    if not focus:
        print("[INFO] Nenhum dos jogadores em data/players.json está atualmente listado como membro da guild True Knife.")
        return

    # 4) Para cada jogador focado, acessar o perfil via link da própria página da guild
    rows = []
    for player in focus:
        profile_url = member_links.get(player)
        if not profile_url:
            # Em teoria não deve acontecer se member_links veio; se acontecer, pula.
            print(f"[WARN] Perfil não encontrado via link para '{player}'. Pulando.")
            continue

        last_raw, last_iso = None, None
        try:
            last_raw, last_iso = fetch_last_login(profile_url)
        except Exception as e:
            print(f"[WARN] Falha ao ler perfil de '{player}': {e}")

        rows.append({
            "player": player,
            "profile_url": profile_url,
            "last_login_raw": last_raw,
            "last_login_iso": last_iso,
        })

    # 5) Persistência
    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    save_snapshot(ts, rows)
    append_history(ts, rows)

    ok = sum(1 for r in rows if r["last_login_raw"])
    print(f"[OK] Coleta (True Knife) feita {ok}/{len(rows)} com 'Last login' · {ts}")


if __name__ == "__main__":
    main()
