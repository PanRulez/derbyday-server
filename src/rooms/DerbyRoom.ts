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
   Parametri pista
   ========================= */
const TRACK_X_START = 0.0;
const STEP_X = 42.85; 

/* =========================
   Tuning bot
   ========================= */
const BOT_BASE_MS_MIN = 3000; 
const BOT_BASE_MS_MAX = 6000;

const BOT_WEIGHTS = [
  { s: 1, w: 85 },
  { s: 2, w: 14 },
  { s: 4, w: 1 },
];

const BOT_POS_UPDATES = true; 
// Portiamo il delay a 4500ms (3s di countdown client + 1.5s di margine)
const BOT_START_DELAY_MS = 4500; 

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
  private awardedTokens = new Set<string>();
  private sid2pf = new Map<string, string>();
  private lastPosTs = new Map<string, number>();
  private readonly POS_MIN_INTERVAL_MS = 50;

  private lastActivity = new Map<string, number>();
  private readonly INACTIVITY_TIMEOUT = 60000;

  onCreate() {
    this.setState(new DerbyState());
    this.autoDispose = true;

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

    this.onMessage("posizione", (client, msg: { x: number; y: number; z: number }) => {
      this.lastActivity.set(client.sessionId, Date.now()); 
      if (this.matchTerminato) return;
      
      const now = Date.now();
      const last = this.lastPosTs.get(client.sessionId) || 0;
      if (now - last < this.POS_MIN_INTERVAL_MS) return;
      this.lastPosTs.set(client.sessionId, now);

      const p = this.state.players.get(client.sessionId);
      if (!p) return;

      const safeNum = (v: any, fallback: number) => Number.isFinite(v) ? Number(v) : fallback;
      p.x = Math.max(TRACK_X_START - 1, Math.min(TRACK_X_START + STEP_X * this.puntiVittoria + 1, safeNum(msg.x, p.x)));
      p.y = safeNum(msg.y, p.y);
      p.z = safeNum(msg.z, p.z);

      this.broadcast("pos_update", { sessionId: client.sessionId, numero_giocatore: p.numero_giocatore, x: p.x, y: p.y, z: p.z }, { except: client });
      this._markLeaderboardDirty();
    });

    this.onMessage("aggiorna_punti", (client, nuovi_punti: number) => {
      this.lastActivity.set(client.sessionId, Date.now());
      if (this.matchTerminato || !this.matchLanciato) return;

      const p = this.state.players.get(client.sessionId);
      if (!p) return;

      const target = Math.min(Math.max(nuovi_punti | 0, 0), this.puntiVittoria);
      const old = p.punti;
      p.punti = target;
      p.x = TRACK_X_START + (STEP_X * p.punti);

      this.broadcast("punteggio_aggiornato", { sessionId: client.sessionId, numero_giocatore: p.numero_giocatore, punti: p.punti });
      this.broadcast("pos_update", { sessionId: client.sessionId, numero_giocatore: p.numero_giocatore, x: p.x, y: p.y, z: p.z });

      this._markLeaderboardDirty();

      if (p.punti >= this.puntiVittoria && old < this.puntiVittoria) {
        this._fineGara(client.sessionId, p.numero_giocatore, null);
      }
    });

    this.onMessage("set_nickname", (client, nick: string) => {
      const p = this.state.players.get(client.sessionId);
      if (p) p.nickname = (nick ?? "").toString().slice(0, 24);
      this._markLeaderboardDirty();
    });

    this.onMessage("start_matchmaking", () => {
      this._tryStartCountdown("MSG");
    });

    this.onMessage("wallet_spend", (client, msg) => {
      this.lastActivity.set(client.sessionId, Date.now());
      this._handleWalletSpend(client, msg).catch(err => console.error(err));
    });
  }

  private _tryStartCountdown(reason: string) {
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
    if (this.matchLanciato) return;
    this.matchLanciato = true;
    this.countdownStarted = false;
    if (this.interval) clearInterval(this.interval);

    this.lock();
    this._spawnBotsIfNeeded();
    this.matchId = `${this.roomId}-${Date.now()}`;
    
    this.broadcast("match_started", { matchId: this.matchId });
    this.broadcast("inizia_match"); // Triggera il countdown 3-2-1 sui client

    this._startLeaderboardTicker(300);

    // I bot partono dopo che i client finiscono il 3-2-1 visivo
    setTimeout(() => {
      if (!this.matchTerminato) this._runBots(true);
    }, BOT_START_DELAY_MS);
  }

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
      this._pfGetBalances(pfid).then(vc => {
        if (vc) client.send("wallet_sync", { totalCO: vc[VC_PRIMARY] ?? 0, totalGE: vc[VC_SECOND] ?? 0 });
      });
    }

    client.send("numero_giocatore", numero);
    this._markLeaderboardDirty();

    if (!this.countdownStarted && this.clients.length >= this.minimoGiocatori) {
      this._tryStartCountdown("AUTO");
    }
  }

  private async _fineGara(winnerSid: string, numero: number, tempo: number | null) {
    if (this.matchTerminato) return;
    this.matchTerminato = true;
    this._clearAllTimers();

    const matchId = this.matchId ?? `${this.roomId}-${Date.now()}`;
    const finalBoard = this._buildLeaderboardPayload();

    // 1. Notifica fine gara immediata
    this.broadcast("gara_finita", { matchId, winner: winnerSid, numero_giocatore: numero, tempo, classifica: finalBoard });

    // 2. Accredito PlayFab
    if (!winnerSid.startsWith("BOT_")) {
        const pfid = this.sid2pf.get(winnerSid);
        if (pfid && PF_HOST && PF_SECRET) {
            try {
                const newTotals = await this._pfAddCurrency(pfid, VC_PRIMARY, 20);
                const cli = this.clients.find(c => c.sessionId === winnerSid);
                if (cli && newTotals) {
                    cli.send("wallet_sync", { totalCO: newTotals[VC_PRIMARY] ?? 0, totalGE: newTotals[VC_SECOND] ?? 0, reason: "win" });
                    cli.send("coins_awarded", { matchId, delta: 20 });
                }
            } catch (e) { console.error("PF Error:", e); }
        }
    }

    // 3. Pausa di 10 secondi per mostrare il podio prima di chiudere la stanza
    setTimeout(() => {
        this.disconnect();
    }, 10000);
  }

  private _runBots(startImmediate = false) {
    const totalW = BOT_WEIGHTS.reduce((a, b) => a + b.w, 0);
    for (const bot of this.bots) {
      const baseMs = BOT_BASE_MS_MIN + Math.floor(Math.random() * (BOT_BASE_MS_MAX - BOT_BASE_MS_MIN));
      const tick = () => {
        if (this.matchTerminato) return;
        const ps = this.state.players.get(bot.sid);
        if (!ps) return;

        let r = Math.floor(Math.random() * totalW);
        let pick = 1;
        for (const k of BOT_WEIGHTS) { if (r < k.w) { pick = k.s; break; } r -= k.w; }

        const old = ps.punti;
        ps.punti = Math.min(ps.punti + pick, this.puntiVittoria);
        ps.x = TRACK_X_START + (STEP_X * ps.punti);

        this.broadcast("punteggio_aggiornato", { sessionId: bot.sid, numero_giocatore: bot.numero, punti: ps.punti });
        if (BOT_POS_UPDATES) this.broadcast("pos_update", { sessionId: bot.sid, numero_giocatore: bot.numero, x: ps.x, y: ps.y, z: ps.z });

        this._markLeaderboardDirty();
        if (ps.punti >= this.puntiVittoria && old < this.puntiVittoria) this._fineGara(bot.sid, bot.numero, null);
      };
      bot.timer = setInterval(tick, baseMs);
    }
  }

  private _clearAllTimers() {
    if (this.interval) clearInterval(this.interval);
    if (this.autostartTimeout) clearTimeout(this.autostartTimeout);
    if (this.leaderboardTimer) clearInterval(this.leaderboardTimer);
    this.bots.forEach(b => { if (b.timer) clearInterval(b.timer); });
  }

  // ... (Restanti helper PlayFab rimangono invariati)
  private async _pfAddCurrency(pfid: string, code: string, amount: number) {
    const res = await fetch(`${PF_HOST}/Server/AddUserVirtualCurrency`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-SecretKey": PF_SECRET },
      body: JSON.stringify({ PlayFabId: pfid, VirtualCurrency: code, Amount: amount }),
    });
    return (await res.json()).code === 200 ? this._pfGetBalances(pfid) : null;
  }

  private async _pfGetBalances(pfid: string) {
    const res = await fetch(`${PF_HOST}/Server/GetUserInventory`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-SecretKey": PF_SECRET },
      body: JSON.stringify({ PlayFabId: pfid }),
    });
    const json = await res.json();
    return json.code === 200 ? json.data.VirtualCurrency : null;
  }

  private _assignNumeroGiocatore(): number {
    const used = new Set();
    this.state.players.forEach(ps => used.add(ps.numero_giocatore));
    for (let i = 1; i <= this.maxClients; i++) if (!used.has(i)) return i;
    return 1;
  }

  private _spawnBotsIfNeeded() {
    const used = new Set();
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

  private _buildLeaderboardPayload() {
    const list: any[] = [];
    this.state.players.forEach((ps, sid) => list.push({ sessionId: sid, numero_giocatore: ps.numero_giocatore, nickname: ps.nickname, punti: ps.punti, x: ps.x }));
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

  private _markLeaderboardDirty() { this.leaderboardDirty = true; }

  async _handleWalletSpend(client: Client, msg: any) { /* Logica esistente */ }
}
