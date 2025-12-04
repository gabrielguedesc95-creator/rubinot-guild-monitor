
# src/collector.py
# -*- coding: utf-8 -*-
"""
Coleta o 'Last login' dos jogadores a partir dos links de perfil encontrados
na página da guild RubinOT informada pelo usuário.

Fluxo:
1) Lê data/players.json.
2) Entra na página da guild (GUILD_URL).
3) Mapeia nome -> URL do perfil.
4) Para cada jogador da lista, acessa o perfil e pega 'Last login'.
5) Salva snapshot (JSON) e histórico (CSV).

Observações:
- A página de personagem do RubinOT exibe 'Last login:' e segue o padrão
  '?subtopic=characters&name=<nome>' (base + query). (Vide exemplo público.)
- A página de guild segue '?subtopic=guilds&page=view&GuildName=<nome>' e contém
  hyperlinks para os perfis dos membros.

"""

import os
import re
import csv
import json
from datetime import datetime
from urllib.parse import urlparse, urljoin, quote_plus

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dtparser

BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # raiz do projeto
DATA_DIR = os.path.join(BASE_DIR, "data")
SNAP_DIR = os.path.join(DATA_DIR, "snapshots")
HISTORY_FILE = os.path.join(DATA_DIR, "history.csv")
PLAYERS_FILE = os.path.join(DATA_DIR, "players.json")

# 👉 Cole aqui a URL da guild que você me passou
GUILD_URL = os.environ.get(
    "GUILD_URL",
    "https://rubinot.com.br/?subtopic=guilds&page=view&GuildName=True+Knife"
).strip()

# User-Agent simples para evitar bloqueios por default UA
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; RubinotGuildMonitor/1.0; +https://github.com/)",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml"
}

def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(SNAP_DIR, exist_ok=True)

def load_target_players():
    """Lê 'data/players.json' (lista de jogadores) e retorna como set normalizado."""
    path = PLAYERS_FILE
    if not os.path.isfile(path):
        raise FileNotFoundError(
            f"Não encontrei {path}. Crie a pasta 'data/' e o arquivo 'players.json' com os nomes."
        )
    with open(path, "r", encoding="utf-8") as f:
        players = json.load(f)
    # Normaliza espaços e mantém o caso original (os nomes precisam bater com o site)
    players = [p.strip() for p in players if isinstance(p, str) and p.strip()]
    return set(players)

def get_guild_member_links(guild_url):
    """
    Acessa a página da guild e extrai um dict: nome -> URL absoluta do perfil.
    Procura por <a> cujo href contenha 'subtopic=characters' e usa o texto do link como nome.
    """
    resp = requests.get(guild_url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    member_links = {}

    # Procura anchors que levem ao perfil do personagem
    for a in soup.select("a[href*='subtopic=characters']"):
        name = a.get_text(strip=True)
        href = a.get("href", "")
        if not name or not href:
            continue
        # Torna a URL absoluta com base na URL da guild
        abs_url = urljoin(guild_url, href)
        member_links[name] = abs_url

    # Se por algum motivo não achar links, ainda dá para montar a URL do perfil:
    # base = scheme://netloc/?subtopic=characters&name=<NOME>
    if not member_links:
        parsed = urlparse(guild_url)
        base = f"{parsed.scheme}://{parsed.netloc}/?subtopic=characters&name="
        # Vamos retornar só a base; o consumidor montará com quote_plus(nome)
        return {}, base

    return member_links, None  # quando há links, não precisa de base

def fetch_last_login(profile_url):
    """
    Acessa a página de perfil do personagem e retorna (last_login_str, last_login_iso_opcional).
    Busca por texto 'Last login' de forma case-insensitive e extrai após ':'.
    """
    resp = requests.get(profile_url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    # Procura qualquer nó de texto contendo 'Last login'
    candidate = soup.find(string=re.compile(r"last\\s*login", re.IGNORECASE))
    last_str = None
    if candidate:
        # Tenta extrair o valor após ':'
        text = candidate.strip()
        # Em muitos casos está no formato "Last login: 24/04/2024, 15:28:07"
        parts = re.split(r":\\s*", text, maxsplit=1)
        if len(parts) == 2:
            last_str = parts[1].strip()

    # Caso a linha esteja em outro elemento, tenta percorrer irmãos/pai
    if not last_str:
        # Procura por elementos em que a label esteja e o valor em um <td>/span ao lado
        for el in soup.find_all(text=re.compile(r"last\\s*login", re.IGNORECASE)):
            parent = el.parent
            # tenta próximos irmãos
            if parent:
                sib_texts = []
                for sib in parent.find_all_next(string=True, limit=3):
                    t = sib.strip()
                    if t and t != el.strip():
                        sib_texts.append(t)
                if sib_texts:
                    # pega o primeiro que pareça uma data
                    for t in sib_texts:
                        if re.search(r"\\d{1,2}/\\d{1,2}/\\d{2,4}", t) or re.search(r"\\d{4}-\\d{2}-\\d{2}", t):
                            last_str = t.strip()
                            break
            if last_str:
                break

    # Tenta converter para ISO (se possível). RubinOT costuma usar dd/mm/yyyy HH:MM:SS.
    last_iso = None
    if last_str:
        try:
            dt = dtparser.parse(last_str, dayfirst=True)
            last_iso = dt.isoformat()
        except Exception:
            # mantém só o raw
            pass

    return last_str, last_iso

def append_history(collection_ts_iso, rows):
    """
    rows: lista de dicts com {'player', 'profile_url', 'last_login_raw', 'last_login_iso'}
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
                r.get("last_login_iso")
            ])

def save_snapshot(collection_ts_iso, rows):
    """Salva um snapshot JSON com os dados coletados na execução."""
    fname = os.path.join(SNAP_DIR, f"snapshot_{collection_ts_iso.replace(':','-')}.json")
    with open(fname, "w", encoding="utf-8") as f:
        json.dump({
            "timestamp": collection_ts_iso,
            "guild_url": GUILD_URL,
            "players": rows
        }, f, ensure_ascii=False, indent=2)

def main():
    ensure_dirs()
    target = load_target_players()
    if not target:
        print("[ERRO] Lista de jogadores vazia em data/players.json.")
        return

    member_links, profile_base = get_guild_member_links(GUILD_URL)

    rows = []
    for player in sorted(target):
        # Descobre a URL do perfil
        if player in member_links:
            profile_url = member_links[player]
        else:
            # monta pela base (se disponível) ou pelo domínio da guild
            if profile_base:
                profile_url = profile_base + quote_plus(player)
            else:
                parsed = urlparse(GUILD_URL)
                profile_url = f"{parsed.scheme}://{parsed.netloc}/?subtopic=characters&name={quote_plus(player)}"

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

    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    save_snapshot(ts, rows)
    append_history(ts, rows)

    ok = sum(1 for r in rows if r["last_login_raw"])
    print(f"[OK] Coleta feita {ok}/{len(rows)} com 'Last login' · {ts}")

if __name__ == "__main__":
    main()
