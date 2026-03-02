import { Room, Client } from "@colyseus/core";
import { Schema, type, MapSchema } from "@colyseus/schema";

/* =========================
   PlayFab config (ENV)
   ========================= */
const PF_TITLE_ID = process.env.PLAYFAB_TITLE_ID || "";
const PF_SECRET   = process.env.PLAYFAB_SECRET_KEY || "";
const PF_HOST     = PF_TITLE_ID ? `https://${PF_TITLE_ID}.playfabapi.com` : "";

const VC_PRIMARY = "CO";
const VC_SECOND  = "GE";

/* =========================
   RANKING constants
   ========================= */
const RANK_STAT = "RankRating";
const RANK_WIN_DELTA = 25;
const RANK_LOSE_DELTA = -5;
const RANK_MIN = 0; // non si può andare sotto

/* =========================
   Parametri pista
   ========================= */
const TRACK_X_START = 0.0;
const STEP_X = 42.85;

/* =========================
   Tuning bot (PIÙ LENTI)
   ========================= */
const BOT_BASE_MS_MIN = 4500;
const BOT_BASE_MS_MAX = 9000;

// più 1, poche 2, rarissime 4
const BOT_WEIGHTS = [
  { s: 1, w: 92 },
  { s: 2, w: 7 },
  { s: 4, w: 1 },
];

const BOT_POS_UPDATES = true;
const BOT_START_DELAY_MS = 4500;

/* =========================
   Helpers safety
   ========================= */
function safeNum(v: any, fallback = 0): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(n: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, n));
}

/* =========================
   State
   ========================= */
class PlayerState extends Schema {
  @type("number") numero_giocatore: number = 0;
  @type("number") x: number = 0;
  @type("number") y: number = 0;
  @type("number") z: number = 0;
  @type("number") punti: number = 0;
  @type("string") nickname: string = "";

  // ✅ cosmetica (0..23)
  @type("number") skin_id: number = 0;
}

class DerbyState extends Schema {
  @type({ map: PlayerState }) players = new MapSchema<PlayerState>();
}

type BotInfo = { sid: string; numero: number; timer?: NodeJS.Timeout };

export class DerbyRoom extends Room<DerbyState> {
  maxClients = 6;

  countdownSeconds = 10;
  minimoGiocatori = 1;
  puntiVittoria = 21;

  countdownStarted = false;
  matchLanciato = false;
  matchTerminato = false;
  tempoRimanente = this.countdownSeconds;

  interval: NodeJS.Timeout | null = null;
  autostartTimeout: NodeJS.Timeout | null = null;

  bots: BotInfo[] = [];

  leaderboardTimer: NodeJS.Timeout | null = null;
  leaderboardDirty = false;

  matchId: string | null = null;

  private sid2pf = new Map<string, string>();
  private lastPosTs = new Map<string, number>();
  private readonly POS_MIN_INTERVAL_MS = 50;

  private lastActivity = new Map<string, number>();
  private readonly INACTIVITY_TIMEOUT = 60000;

  // ✅ CO guadagnati in questa partita (somma buche 1/2/4)
  private matchEarnedCO = new Map<string, number>(); // sid -> earned

  onCreate() {
    this.setState(new DerbyState());
    this.autoDispose = true;

    // kick inattivi
    this.clock.setInterval(() => {
      const now = Date.now();
      this.clients.forEach(client => {
        const lastTime = this.lastActivity.get(client.sessionId) || 0;
        if (now - lastTime > this.INACTIVITY_TIMEOUT) {
          client.leave(4001);
        }
      });
    }, 5000);

    /* ========== MESSAGGI CLIENT -> SERVER ========== */

    this.onMessage("heartbeat", (client) => {
      this.lastActivity.set(client.sessionId, Date.now());
    });

    this.onMessage("set_skin", (client, msg: { skin_id: number }) => {
      try {
        this.lastActivity.set(client.sessionId, Date.now());
        const p = this.state.players.get(client.sessionId);
        if (!p) return;

        const skin = clamp((safeNum(msg?.skin_id, 0) | 0), 0, 23);
        p.skin_id = skin;

        this.broadcast("skin_update", {
          sessionId: client.sessionId,
          numero_giocatore: p.numero_giocatore,
          skin_id: p.skin_id
        });

        this._markLeaderboardDirty();
      } catch (e) {
        console.error("[set_skin] error:", e);
      }
    });

    this.onMessage("posizione", (client, msg: { x: number; y: number; z: number }) => {
      try {
        this.lastActivity.set(client.sessionId, Date.now());
        if (this.matchTerminato) return;

        const now = Date.now();
        const last = this.lastPosTs.get(client.sessionId) || 0;
        if (now - last < this.POS_MIN_INTERVAL_MS) return;
        this.lastPosTs.set(client.sessionId, now);

        const p = this.state.players.get(client.sessionId);
        if (!p) return;

        const maxX = TRACK_X_START + STEP_X * this.puntiVittoria + 1;
        p.x = clamp(safeNum(msg?.x, p.x), TRACK_X_START - 1, maxX);
        p.y = safeNum(msg?.y, p.y);
        p.z = safeNum(msg?.z, p.z);

        this.broadcast(
          "pos_update",
          { sessionId: client.sessionId, numero_giocatore: p.numero_giocatore, x: p.x, y: p.y, z: p.z },
          { except: client }
        );

        this._markLeaderboardDirty();
      } catch (e) {
        console.error("[posizione] error:", e);
      }
    });

    /**
     * ✅ CO dalle buche (anti-cheat):
     * - client manda "totale punti"
     * - server accetta SOLO incrementi 0..4 per singolo update (1/2/4)
     * - accumula CO = delta (1/2/4)
     */
    this.onMessage("aggiorna_punti", (client, nuovi_punti: number) => {
      try {
        this.lastActivity.set(client.sessionId, Date.now());
        if (this.matchTerminato || !this.matchLanciato) return;

        const p = this.state.players.get(client.sessionId);
        if (!p) return;

        const old = p.punti;

        const requested = Math.min(Math.max((nuovi_punti | 0), 0), this.puntiVittoria);

        // delta richiesto (solo +)
        let delta = (requested - old) | 0;

        // accetta SOLO 0..4 (una buca)
        delta = clamp(delta, 0, 4);

        const target = Math.min(old + delta, this.puntiVittoria);

        if (target === old) return;

        p.punti = target;
        p.x = TRACK_X_START + (STEP_X * p.punti);

        // accumula CO solo per umani (i bot non mandano aggiorna_punti)
        const prevCO = this.matchEarnedCO.get(client.sessionId) ?? 0;
        this.matchEarnedCO.set(client.sessionId, prevCO + delta);

        this.broadcast("punteggio_aggiornato", {
          sessionId: client.sessionId,
          numero_giocatore: p.numero_giocatore,
          punti: p.punti
        });

        this.broadcast("pos_update", {
          sessionId: client.sessionId,
          numero_giocatore: p.numero_giocatore,
          x: p.x, y: p.y, z: p.z
        });

        this._markLeaderboardDirty();

        if (p.punti >= this.puntiVittoria && old < this.puntiVittoria) {
          this._fineGara(client.sessionId, p.numero_giocatore, null);
        }
      } catch (e) {
        console.error("[aggiorna_punti] error:", e);
      }
    });

    this.onMessage("set_nickname", (client, nick: string) => {
      try {
        this.lastActivity.set(client.sessionId, Date.now());
        const p = this.state.players.get(client.sessionId);
        if (p) p.nickname = (nick ?? "").toString().slice(0, 24);
        this._markLeaderboardDirty();
      } catch (e) {
        console.error("[set_nickname] error:", e);
      }
    });

    this.onMessage("start_matchmaking", () => {
      try {
        this._tryStartCountdown("MSG");
      } catch (e) {
        console.error("[start_matchmaking] error:", e);
      }
    });

    this.onMessage("lancio", (client, msg: any) => {
      try {
        this.lastActivity.set(client.sessionId, Date.now());
        if (this.matchTerminato) return;
        if (!this.matchLanciato) return;

        const p = this.state.players.get(client.sessionId);
        if (!p) return;

        const fx = safeNum(msg?.fx, 0);
        const fz = safeNum(msg?.fz, 0);
        const x  = safeNum(msg?.x, p.x);
        const z  = safeNum(msg?.z, p.z);

        const fxC = clamp(fx, -200, 200);
        const fzC = clamp(fz, -200, 200);

        this.broadcast(
          "lancio_update",
          { sessionId: client.sessionId, numero_giocatore: p.numero_giocatore, fx: fxC, fz: fzC, x, z },
          { except: client }
        );
      } catch (e) {
        console.error("[lancio] error:", e);
      }
    });

    this.onMessage("wallet_spend", (client, msg) => {
      try {
        this.lastActivity.set(client.sessionId, Date.now());
        const maybePromise = this._handleWalletSpend(client, msg);
        if (maybePromise && typeof (maybePromise as any).catch === "function") {
          (maybePromise as any).catch((err: any) => console.error("[wallet_spend] error:", err));
        }
      } catch (e) {
        console.error("[wallet_spend] error:", e);
      }
    });
  }
func _on_wallet_sync(d) -> void:
	if d == null:
		return

	# Il server manda: { totalCO, totalGE, reason? }
	var new_balance := int(d.get("totalCO", 0))
	var reason := String(d.get("reason", "sync"))
	var delta := int(d.get("delta", 0)) # se non c'è, resta 0
	var match_id := String(d.get("matchId", ""))

	# ✅ aggiorna Wallet Autoload (il tuo Wallet.gd ha questa funzione perfetta)
	var wallet: Node = get_node_or_null("/root/Wallet")
	if wallet:
		if wallet.has_method("apply_server_wallet_update"):
			wallet.call("apply_server_wallet_update", new_balance, delta, reason, match_id)
		elif wallet.has_method("set_coins"):
			wallet.call("set_coins", new_balance)

	# ✅ aggiorna subito UI Home se visibile
	var home_nodes := get_tree().get_nodes_in_group("home_ui")
	if home_nodes.size() > 0:
		var home := home_nodes[0]
		if home:
			var co_label := home.get_node_or_null("CountBar/Legnetto2/COCount")
			if co_label:
				co_label.text = str(new_balance)


func _on_coins_awarded(d) -> void:
	# opzionale: toast in UI, per debug
	if d == null:
		return
	var delta := int(d.get("delta", 0))
	_log("coins_awarded delta=%d" % delta)


func _on_rank_sync(d) -> void:
	if d == null:
		return

	var val := int(d.get("value", 0))
	var delta := int(d.get("delta", 0))

	var home_nodes := get_tree().get_nodes_in_group("home_ui")
	if home_nodes.size() > 0:
		var home := home_nodes[0]
		if home and home.has_method("on_rank_changed"):
			home.call("on_rank_changed", delta, val)
		elif home:
			var rk := home.get_node_or_null("CountBar/Legnetto1/RKCount")
			if rk:
				rk.text = str(val)
  /* =========================
     Countdown / Start match
     ========================= */
  private _tryStartCountdown(_reason: string) {
    if (this.countdownStarted || this.matchLanciato || this.matchTerminato) return;
    if (this.clients.length < this.minimoGiocatori) return;

    this.countdownStarted = true;
    this.tempoRimanente = this.countdownSeconds;
    this.broadcast("countdown_update", this.tempoRimanente);

    this.interval = setInterval(() => {
      this.tempoRimanente -= 1;
      this.broadcast("countdown_update", Math.max(0, this.tempoRimanente));
      if (this.tempoRimanente <= 0 || this.state.players.size >= this.maxClients) {
        this.lanciaMatch();
      }
    }, 1000);
  }

  private lanciaMatch() {
    if (this.matchLanciato || this.matchTerminato) return;

    this.matchLanciato = true;
    this.countdownStarted = false;

    if (this.interval) { clearInterval(this.interval); this.interval = null; }

    this.lock();
    this._spawnBotsIfNeeded();
    this.matchId = `${this.roomId}-${Date.now()}`;

    // ✅ reset CO partita
    this.matchEarnedCO.clear();

    this.broadcast("match_started", { matchId: this.matchId });
    this.broadcast("inizia_match");

    this._startLeaderboardTicker(300);

    setTimeout(() => {
      if (!this.matchTerminato) this._runBots(true);
    }, BOT_START_DELAY_MS);
  }

  /* =========================
     Join / Leave
     ========================= */
  onJoin(client: Client, options: any) {
    this.lastActivity.set(client.sessionId, Date.now());

    if (this.matchTerminato || this.matchLanciato) {
      client.send("match_in_corso");
      client.leave();
      return;
    }

    const numero = this._assignNumeroGiocatore();

    const p = new PlayerState();
    p.numero_giocatore = numero;
    p.nickname = String(options?.nickname || `Player ${numero}`).slice(0, 24);
    this.state.players.set(client.sessionId, p);

    const pfid = String(options?.playfabId || "");
    if (pfid) {
      this.sid2pf.set(client.sessionId, pfid);

      this._pfGetBalances(pfid)
        .then(vc => {
          if (vc) client.send("wallet_sync", {
            totalCO: vc[VC_PRIMARY] ?? 0,
            totalGE: vc[VC_SECOND] ?? 0
          });
        })
        .catch(err => console.error("[_pfGetBalances] error:", err));
    }

    client.send("numero_giocatore", numero);

    // skins snapshot
    const skins: any[] = [];
    this.state.players.forEach((ps, sid) => {
      skins.push({
        sessionId: sid,
        numero_giocatore: ps.numero_giocatore,
        skin_id: ps.skin_id ?? 0
      });
    });
    client.send("skins_snapshot", skins);

    this._markLeaderboardDirty();

    if (!this.countdownStarted && this.clients.length >= this.minimoGiocatori) {
      this._tryStartCountdown("AUTO");
    }
  }

  onLeave(client: Client) {
    try {
      this.lastActivity.delete(client.sessionId);
      this.lastPosTs.delete(client.sessionId);
      this.sid2pf.delete(client.sessionId);
      this.matchEarnedCO.delete(client.sessionId);

      if (this.state.players.has(client.sessionId)) {
        this.state.players.delete(client.sessionId);
        this._markLeaderboardDirty();
      }

      const humansLeft = this.clients.length;
      if (humansLeft <= 0 && !this.matchTerminato) {
        this.matchTerminato = true;
        this._clearAllTimers();
        this.disconnect();
      }
    } catch (e) {
      console.error("[onLeave] error:", e);
    }
  }

  onDispose() {
    this._clearAllTimers();
  }

  /* =========================
     Fine gara
     ========================= */
  private _getNicknameBySid(sid: string): string {
    const ps = this.state.players.get(sid);
    return (ps?.nickname ?? "").toString().slice(0, 24);
  }

  private async _fineGara(winnerSid: string, numero: number, tempo: number | null) {
    if (this.matchTerminato) return;

    this.matchTerminato = true;
    this._clearAllTimers();

    const matchId = this.matchId ?? `${this.roomId}-${Date.now()}`;
    const finalBoard = this._buildLeaderboardPayload();

    const winnerNick = this._getNicknameBySid(winnerSid);
    const winnerPoints = this.puntiVittoria;

    this.broadcast("gara_finita", {
      matchId,
      winner: winnerSid,
      numero_giocatore: numero,
      winner_points: winnerPoints,
      winner_nick: winnerNick,
      tempo,
      classifica: finalBoard
    });

    // ====== CO: accredito in base alle buche (somma 1/2/4) ======
    try {
      await Promise.all(this.clients.map(async (cli) => {
        const sid = cli.sessionId;
        if (sid.startsWith("BOT_")) return;

        const pfid = this.sid2pf.get(sid);
        if (!pfid || !PF_HOST || !PF_SECRET) return;

        const earned = this.matchEarnedCO.get(sid) ?? 0;
        if (earned <= 0) return;

        const newTotals = await this._pfAddCurrency(pfid, VC_PRIMARY, earned);
        if (newTotals) {
          cli.send("wallet_sync", {
            totalCO: newTotals[VC_PRIMARY] ?? 0,
            totalGE: newTotals[VC_SECOND] ?? 0,
            reason: "holes"
          });
          cli.send("coins_awarded", { matchId, delta: earned });
        }
      }));
    } catch (e) {
      console.error("[CO] holes payout error:", e);
    }

    // ====== RANKING: winner +25, altri -5 (min 0) ======
    try {
      await this._pfApplyRankAfterMatch(matchId, winnerSid);
    } catch (e) {
      console.error("[RANK] apply error:", e);
    }

    // chiudi stanza dopo 10s
    setTimeout(() => {
      this.disconnect();
    }, 10000);
  }

  /* =========================
     Bots
     ========================= */
  private _runBots(_startImmediate = false) {
    const totalW = BOT_WEIGHTS.reduce((a, b) => a + b.w, 0);

    for (const bot of this.bots) {
      const baseMs = BOT_BASE_MS_MIN + Math.floor(Math.random() * (BOT_BASE_MS_MAX - BOT_BASE_MS_MIN));

      const tick = () => {
        if (this.matchTerminato) return;

        const ps = this.state.players.get(bot.sid);
        if (!ps) return;

        let r = Math.floor(Math.random() * totalW);
        let pick = 1;
        for (const k of BOT_WEIGHTS) {
          if (r < k.w) { pick = k.s; break; }
          r -= k.w;
        }

        const old = ps.punti;
        ps.punti = Math.min(ps.punti + pick, this.puntiVittoria);
        ps.x = TRACK_X_START + (STEP_X * ps.punti);

        this.broadcast("punteggio_aggiornato", {
          sessionId: bot.sid,
          numero_giocatore: bot.numero,
          punti: ps.punti
        });

        if (BOT_POS_UPDATES) {
          this.broadcast("pos_update", {
            sessionId: bot.sid,
            numero_giocatore: bot.numero,
            x: ps.x, y: ps.y, z: ps.z
          });
        }

        this._markLeaderboardDirty();

        if (ps.punti >= this.puntiVittoria && old < this.puntiVittoria) {
          this._fineGara(bot.sid, bot.numero, null);
        }
      };

      bot.timer = setInterval(tick, baseMs);
    }
  }

  /* =========================
     Timers cleanup
     ========================= */
  private _clearAllTimers() {
    if (this.interval) { clearInterval(this.interval); this.interval = null; }
    if (this.autostartTimeout) { clearTimeout(this.autostartTimeout); this.autostartTimeout = null; }
    if (this.leaderboardTimer) { clearInterval(this.leaderboardTimer); this.leaderboardTimer = null; }
    this.bots.forEach(b => { if (b.timer) clearInterval(b.timer); b.timer = undefined; });
  }

  /* =========================
     PlayFab helpers
     ========================= */

  private async _pfAddCurrency(pfid: string, code: string, amount: number) {
    if (!PF_HOST || !PF_SECRET) return null;

    const res = await fetch(`${PF_HOST}/Server/AddUserVirtualCurrency`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-SecretKey": PF_SECRET },
      body: JSON.stringify({ PlayFabId: pfid, VirtualCurrency: code, Amount: amount }),
    });

    const json = await res.json().catch(() => null);
    return json?.code === 200 ? this._pfGetBalances(pfid) : null;
  }

  private async _pfGetBalances(pfid: string) {
    if (!PF_HOST || !PF_SECRET) return null;

    const res = await fetch(`${PF_HOST}/Server/GetUserInventory`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-SecretKey": PF_SECRET },
      body: JSON.stringify({ PlayFabId: pfid }),
    });

    const json = await res.json().catch(() => null);
    return json?.code === 200 ? json.data.VirtualCurrency : null;
  }

  private async _pfGetRank(pfid: string): Promise<number> {
    if (!PF_HOST || !PF_SECRET) return 0;
    try {
      const res = await fetch(`${PF_HOST}/Server/GetPlayerStatistics`, {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-SecretKey": PF_SECRET },
        body: JSON.stringify({
          PlayFabId: pfid,
          StatisticNames: [RANK_STAT],
        }),
      });
      const json = await res.json().catch(() => null);
      if (json?.code !== 200) return 0;
      const stats = json?.data?.Statistics ?? [];
      const found = stats.find((s: any) => s?.StatisticName === RANK_STAT);
      return found && Number.isFinite(Number(found.Value)) ? Number(found.Value) : 0;
    } catch (e) {
      console.error("[PF][getRank] error:", e);
      return 0;
    }
  }

  private async _pfSetRank(pfid: string, newValue: number): Promise<boolean> {
    if (!PF_HOST || !PF_SECRET) return false;
    try {
      const res = await fetch(`${PF_HOST}/Server/UpdatePlayerStatistics`, {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-SecretKey": PF_SECRET },
        body: JSON.stringify({
          PlayFabId: pfid,
          Statistics: [{ StatisticName: RANK_STAT, Value: newValue | 0 }],
        }),
      });
      const json = await res.json().catch(() => null);
      return json?.code === 200;
    } catch (e) {
      console.error("[PF][setRank] error:", e);
      return false;
    }
  }

  private async _pfWasRankAlreadyApplied(pfid: string, matchId: string): Promise<boolean> {
    if (!PF_HOST || !PF_SECRET) return false;
    try {
      const res = await fetch(`${PF_HOST}/Server/GetUserReadOnlyData`, {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-SecretKey": PF_SECRET },
        body: JSON.stringify({ PlayFabId: pfid, Keys: ["rank_last_match_id"] }),
      });
      const json = await res.json().catch(() => null);
      if (json?.code !== 200) return false;
      const last = json?.data?.Data?.rank_last_match_id?.Value ?? "";
      return String(last) === String(matchId);
    } catch (e) {
      console.error("[PF][wasRankApplied] error:", e);
      return false;
    }
  }

  private async _pfMarkRankApplied(pfid: string, matchId: string, delta: number, value: number): Promise<void> {
    if (!PF_HOST || !PF_SECRET) return;
    try {
      await fetch(`${PF_HOST}/Server/UpdateUserReadOnlyData`, {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-SecretKey": PF_SECRET },
        body: JSON.stringify({
          PlayFabId: pfid,
          Data: {
            rank_last_match_id: String(matchId),
            rank_last_delta: String(delta),
            rank_last_value: String(value),
          },
        }),
      }).catch(() => null);
    } catch {
      // ignore
    }
  }

  private async _pfApplyRankAfterMatch(matchId: string, winnerSid: string) {
    if (!PF_HOST || !PF_SECRET) return;

    const entries: Array<{ sid: string; pfid: string }> = [];
    for (const c of this.clients) {
      const pfid = this.sid2pf.get(c.sessionId);
      if (pfid) entries.push({ sid: c.sessionId, pfid });
    }

    if (entries.length === 0) return;

    await Promise.all(entries.map(async ({ sid, pfid }) => {
      const delta = (sid === winnerSid) ? RANK_WIN_DELTA : RANK_LOSE_DELTA;

      try {
        const already = await this._pfWasRankAlreadyApplied(pfid, matchId);
        if (already) return;

        const cur = await this._pfGetRank(pfid);
        const next = Math.max(RANK_MIN, (cur + delta) | 0);

        const ok = await this._pfSetRank(pfid, next);
        if (ok) {
          await this._pfMarkRankApplied(pfid, matchId, delta, next);
        }

        const cli = this.clients.find(x => x.sessionId === sid);
        if (cli && ok) {
          cli.send("rank_sync", { matchId, value: next, delta });
        }
      } catch (e) {
        console.error("[RANK] update error:", pfid, e);
      }
    }));
  }

  /* =========================
     Player numbering / bots spawn
     ========================= */
  private _assignNumeroGiocatore(): number {
    const used = new Set<number>();
    this.state.players.forEach(ps => used.add(ps.numero_giocatore));
    for (let i = 1; i <= this.maxClients; i++) if (!used.has(i)) return i;
    return 1;
  }

  private _spawnBotsIfNeeded() {
    const used = new Set<number>();
    this.state.players.forEach(ps => used.add(ps.numero_giocatore));

    for (let i = 1; i <= this.maxClients; i++) {
      if (!used.has(i)) {
        const sid = `BOT_${this.roomId}_${i}`;
        const ps = new PlayerState();
        ps.numero_giocatore = i;
        ps.nickname = `BOT ${i}`;
        this.state.players.set(sid, ps);
        this.bots.push({ sid, numero: i });
      }
    }
  }

  /* =========================
     Leaderboard
     ========================= */
  private _buildLeaderboardPayload() {
    const list: any[] = [];
    this.state.players.forEach((ps, sid) => list.push({
      sessionId: sid,
      numero_giocatore: ps.numero_giocatore,
      nickname: ps.nickname,
      punti: ps.punti,
      x: ps.x
    }));
    return list.sort((a, b) => b.punti - a.punti || b.x - a.x);
  }

  private _startLeaderboardTicker(ms: number) {
    this.leaderboardTimer = setInterval(() => {
      if (this.leaderboardDirty) {
        this.leaderboardDirty = false;
        this.broadcast("classifica_update", this._buildLeaderboardPayload());
      }
    }, ms);
  }

  private _markLeaderboardDirty() {
    this.leaderboardDirty = true;
  }

  async _handleWalletSpend(_client: Client, _msg: any) {
    return;
  }
}
