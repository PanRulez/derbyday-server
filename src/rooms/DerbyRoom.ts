// server/DerbyRoom.ts
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
   Parametri pista (coerenti col client)
   ========================= */
// Allinea con Godot (Cavallino.gd):
// - TRACK_X_START = posizione X iniziale dei cavalli
// - STEP_X        = passo_lunghezza (default 0.20)
const TRACK_X_START = 0.0;
const STEP_X = 0.20;

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
  minimoGiocatori = 1;     // ↑ a 2 se vuoi 2 player minimi
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

  // Anti-flood posizioni
  private lastPosTs = new Map<string, number>();
  private readonly POS_MIN_INTERVAL_MS = 50; // max ~20 msg/s

  // Soft-authoritative scoring
  private readonly ALLOWED_STEPS = new Set([1, 2, 4]);

  onCreate() {
    this.setState(new DerbyState());
    this.autoDispose = true;
    console.log("🏁 Room creata:", this.roomId);

    /* ========== MESSAGGI CLIENT -> SERVER ========== */

    // POSIZIONE (x,y,z) aggiornata dal client umano
    this.onMessage("posizione", (client, msg: { x: number; y: number; z: number }) => {
      if (this.matchTerminato) return;

      // throttle anti-flood
      const now = Date.now();
      const last = this.lastPosTs.get(client.sessionId) || 0;
      if (now - last < this.POS_MIN_INTERVAL_MS) return;
      this.lastPosTs.set(client.sessionId, now);

      const p = this.state.players.get(client.sessionId);
      if (!p) return;

      // sanificazione input
      const safeNum = (v: any, fallback: number) =>
        Number.isFinite(v) ? Number(v) : fallback;

      // clamp opzionali (se vuoi limitare lo spazio pista)
      const maxX = TRACK_X_START + STEP_X * this.puntiVittoria + 1.0;
      const minX = TRACK_X_START - 1.0;

      p.x = Math.max(minX, Math.min(maxX, safeNum(msg.x, p.x)));
      p.y = safeNum(msg.y, p.y);
      p.z = safeNum(msg.z, p.z);

      // Broadcast delta posizione (i client animano gli avversari)
      this.broadcast(
        "pos_update",
        { sessionId: client.sessionId, numero_giocatore: p.numero_giocatore, x: p.x, y: p.y, z: p.z },
        { except: client }
      );

      this._markLeaderboardDirty();
    });

    // PUNTI aggiornati dal client umano (soft-authoritative: consente solo +1/+2/+4)
    this.onMessage("aggiorna_punti", (client, nuovi_punti: number) => {
      if (this.matchTerminato) return;
      const p = this.state.players.get(client.sessionId);
      if (!p) return;

      const safeInt = (v: any) => (Number.isFinite(v) ? (v | 0) : p.punti);
      const target = Math.min(Math.max(safeInt(nuovi_punti), 0), this.puntiVittoria);
      const delta = target - p.punti;

      // accetta solo incrementi positivi di 1/2/4
      if (delta <= 0 || !this.ALLOWED_STEPS.has(delta)) return;

      const old = p.punti;
      p.punti = target;

      // X autoritativa dai punti (coerente con i bot)
      p.x = TRACK_X_START + STEP_X * p.punti;

      // Notifiche
      this.broadcast("punteggio_aggiornato", {
        sessionId: client.sessionId,
        numero_giocatore: p.numero_giocatore,
        punti: p.punti,
      });

      // anche pos_update per farlo avanzare lato avversari
      this.broadcast("pos_update", {
        sessionId: client.sessionId,
        numero_giocatore: p.numero_giocatore,
        x: p.x, y: p.y, z: p.z,
      });

      this._markLeaderboardDirty();

      if (!this.matchTerminato && p.punti >= this.puntiVittoria && old < this.puntiVittoria) {
        this._fineGara(client.sessionId, p.numero_giocatore, null);
      }
    });

    // Nickname facoltativo
    this.onMessage("set_nickname", (client, nick: string) => {
      const p = this.state.players.get(client.sessionId);
      if (!p) return;
      p.nickname = (nick ?? "").toString().slice(0, 24);
      this._markLeaderboardDirty();
    });

    // Snapshot (mapping sessionId->numero)
    this.onMessage("richiedi_snapshot", (client) => {
      const snap: Array<{ sessionId: string; numero_giocatore: number }> = [];
      this.state.players.forEach((ps, sid) =>
        snap.push({ sessionId: sid, numero_giocatore: ps.numero_giocatore })
      );
      client.send("mappa_iniziale", snap);
      console.log("📦 Snapshot inviata a", client.sessionId, snap);
    });

    // Start manuale countdown
    this.onMessage("start_matchmaking", (client) => {
      console.log("📨 start_matchmaking da", client.sessionId);
      this._tryStartCountdown("MSG");
    });
  }

  /* =========================
     Countdown & Match
     ========================= */
  private _tryStartCountdown(reason: "MSG" | "AUTO_JOIN" | "AUTO_TIMER") {
    if (this.countdownStarted || this.matchLanciato || this.matchTerminato) return;
    if (this.clients.length < this.minimoGiocatori) return;

    this.countdownStarted = true;
    this.tempoRimanente = this.countdownSeconds;

    console.log(`⏳ Countdown (${reason}) — clients:${this.clients.length}`);
    this.broadcast("countdown_update", this.tempoRimanente);

    if (this.interval) clearInterval(this.interval);
    this.interval = setInterval(() => {
      if (this.state.players.size >= this.maxClients) {
        this.lanciaMatch();
        return;
      }

      this.tempoRimanente -= 1;

      if (this.tempoRimanente >= 0) {
        this.broadcast("countdown_update", this.tempoRimanente);
        // console.log(`⏳ Countdown: ${this.tempoRimanente}s`);
      }

      if (this.tempoRimanente <= 0) this.lanciaMatch();
    }, 1000);
  }

  private lanciaMatch() {
    if (this.matchLanciato) return;
    this.matchLanciato = true;
    this.countdownStarted = false;

    if (this.interval) { clearInterval(this.interval); this.interval = null; }
    if (this.autostartTimeout) { clearTimeout(this.autostartTimeout); this.autostartTimeout = null; }

    this.lock();
    this._spawnBotsIfNeeded();

    this.matchId = `${this.roomId}-${Date.now()}`;
    this.broadcast("match_started", { matchId: this.matchId });

    const snap: Array<{ sessionId: string; numero_giocatore: number }> = [];
    this.state.players.forEach((ps, sid) =>
      snap.push({ sessionId: sid, numero_giocatore: ps.numero_giocatore })
    );
    this.broadcast("mappa_iniziale", snap);
    console.log("🗺️ Snapshot mapping:", snap);

    this.broadcast("countdown_update", 0);
    this.broadcast("inizia_match");
    console.log(`🚦 MATCH INIZIATO (umani:${this.clients.length} bot:${this.bots.length})`);

    // classifica periodica
    this._startLeaderboardTicker(300);

    setTimeout(() => this._runBots(), 800);
  }

  /* =========================
     Join / Leave
     ========================= */
  onJoin(client: Client, options: any) {
    console.log(`👤 Join: ${client.sessionId} opts:`, options ?? {});
    if (this.matchTerminato || this.matchLanciato) {
      client.send("match_in_corso");
      client.leave();
      return;
    }

    const numero = this._assignNumeroGiocatore();
    const p = new PlayerState();
    p.numero_giocatore = numero;
    p.x = TRACK_X_START;
    p.y = 0;
    p.z = 0;

    if (options?.nickname) {
      p.nickname = String(options.nickname).slice(0, 24);
    }
    this.state.players.set(client.sessionId, p);

    const pfid = String(options?.playfabId || "");
    if (pfid) {
      this.sid2pf.set(client.sessionId, pfid);
      this._pfGetBalances(pfid).then(vc => {
        if (vc) {
          client.send("wallet_sync", {
            totalCO: vc[VC_PRIMARY] ?? 0,
            totalGE: vc[VC_SECOND] ?? 0,
          });
        }
      }).catch(() => {});
    }

    const snap: Array<{ sessionId: string; numero_giocatore: number }> = [];
    this.state.players.forEach((ps, sid) => snap.push({ sessionId: sid, numero_giocatore: ps.numero_giocatore }));
    this.broadcast("mappa_iniziale", snap);
    this.broadcast("giocatore_mappato", { sessionId: client.sessionId, numero_giocatore: numero });
    client.send("numero_giocatore", numero);

    if (this.countdownStarted) {
      client.send("countdown_update", this.tempoRimanente);
    } else {
      if (this.clients.length >= this.minimoGiocatori) {
        this._tryStartCountdown("AUTO_JOIN");
      } else if (!this.autostartTimeout) {
        this.autostartTimeout = setTimeout(() => this._tryStartCountdown("AUTO_TIMER"), 2000);
      }
    }
  }

  onLeave(client: Client) {
    // stop eventuale timer bot con stesso SID
    const botIdx = this.bots.findIndex(b => b.sid === client.sessionId);
    if (botIdx >= 0) {
      const bot = this.bots[botIdx];
      if (bot.timer) clearInterval(bot.timer);
      this.bots.splice(botIdx, 1);
    }

    this.state.players.delete(client.sessionId);
    this.sid2pf.delete(client.sessionId);
    this.lastPosTs.delete(client.sessionId);

    this._markLeaderboardDirty();

    if (this.clients.length === 0 && !this.matchTerminato) {
      this._clearAllTimers();
      console.log("🧹 Room vuota, chiudo.");
      this.disconnect();
    }
  }

  onDispose() {
    console.log("🧽 onDispose room:", this.roomId);
    this._clearAllTimers();
  }

  private _assignNumeroGiocatore(): number {
    const used = new Set<number>();
    this.state.players.forEach(ps => used.add(ps.numero_giocatore));
    for (let i = 1; i <= this.maxClients; i++) if (!used.has(i)) return i;
    return Math.min(used.size + 1, this.maxClients);
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
        ps.x = TRACK_X_START; ps.y = 0; ps.z = 0;

        this.state.players.set(sid, ps);
        this.bots.push({ sid, numero: i });
        this.broadcast("giocatore_mappato", { sessionId: sid, numero_giocatore: i });
        console.log(`🤖 BOT creato: ${sid} → Player ${i}`);
      }
    }
    this._markLeaderboardDirty();
  }

  private _runBots() {
    const WEIGHTS = [{ s: 1, w: 85 }, { s: 2, w: 14 }, { s: 4, w: 1 }];
    const totalW = WEIGHTS.reduce((a, b) => a + b.w, 0);

    for (const bot of this.bots) {
      const baseMs = 9000 + Math.floor(Math.random() * 3000);
      const jitter = 0.4;
      const minMs = Math.floor(baseMs * (1 - jitter));
      const maxMs = Math.floor(baseMs * (1 + jitter));

      const tick = () => {
        if (this.matchTerminato) return;
        const ps = this.state.players.get(bot.sid);
        if (!ps) return;

        let r = Math.floor(Math.random() * totalW);
        let pick = 1;
        for (const k of WEIGHTS) { if (r < k.w) { pick = k.s; break; } r -= k.w; }

        const prev = ps.punti;
        ps.punti = Math.min(ps.punti + pick, this.puntiVittoria);

        // Autoritative X basata sui punti
        ps.x = TRACK_X_START + STEP_X * ps.punti;

        // Notifiche score + posizione
        this.broadcast("punteggio_aggiornato", { sessionId: bot.sid, numero_giocatore: bot.numero, punti: ps.punti });
        this.broadcast("pos_update", { sessionId: bot.sid, numero_giocatore: bot.numero, x: ps.x, y: ps.y, z: ps.z });

        this._markLeaderboardDirty();

        if (!this.matchTerminato && ps.punti >= this.puntiVittoria && prev < this.puntiVittoria) {
          this._fineGara(bot.sid, bot.numero, null);
        }
      };

      const firstDelay = minMs + Math.floor(Math.random() * (maxMs - minMs));
      setTimeout(() => {
        tick();
        bot.timer = setInterval(() => tick(), minMs + Math.floor(Math.random() * (maxMs - minMs)));
      }, firstDelay);
    }
  }

  /* =========================
     Classifica
     ========================= */
  private _startLeaderboardTicker(ms: number) {
    if (this.leaderboardTimer) clearInterval(this.leaderboardTimer);
    this.leaderboardTimer = setInterval(() => {
      if (!this.matchLanciato || this.matchTerminato) return;
      if (this.leaderboardDirty) {
        this.leaderboardDirty = false;
        const payload = this._buildLeaderboardPayload();
        this.broadcast("classifica_update", payload);
      }
    }, ms);
  }

  private _markLeaderboardDirty() {
    this.leaderboardDirty = true;
  }

  private _buildLeaderboardPayload(): Array<{
    sessionId: string;
    numero_giocatore: number;
    nickname: string;
    punti: number;
    x: number;
  }> {
    const list: Array<{ sessionId: string; numero_giocatore: number; nickname: string; punti: number; x: number }> = [];
    this.state.players.forEach((ps, sid) => {
      list.push({
        sessionId: sid,
        numero_giocatore: ps.numero_giocatore,
        nickname: ps.nickname || `Player ${ps.numero_giocatore}`,
        punti: ps.punti,
        x: ps.x,
      });
    });

    // Ordina: punti DESC, poi x DESC
    list.sort((a, b) => (a.punti === b.punti ? b.x - a.x : b.punti - a.punti));
    return list;
  }

  /* =========================
     Fine gara + premi PlayFab
     ========================= */
  private _fineGara(winnerSid: string, numero: number, tempo: number | null) {
    if (this.matchTerminato) return;
    this.matchTerminato = true;

    const matchId = this.matchId ?? `${this.roomId}-${Date.now()}`;
    console.log("🏁 Fine gara. Winner:", winnerSid, "Player", numero, "matchId:", matchId);

    const finalBoard = this._buildLeaderboardPayload();
    this.broadcast("classifica_update", finalBoard);

    this.broadcast("gara_finita", { matchId, winner: winnerSid, numero_giocatore: numero, tempo });

    // Premi PlayFab (solo umani)
    if (!winnerSid.startsWith("BOT_")) {
      const token = `${winnerSid}:${matchId}`;
      if (!this.awardedTokens.has(token)) {
        this.awardedTokens.add(token);

        const pfid = this.sid2pf.get(winnerSid);
        if (pfid && PF_HOST && PF_SECRET) {
          const delta = 20;
          this._pfAddCurrency(pfid, VC_PRIMARY, delta)
            .then(newTotals => {
              const cli = this.clients.find(c => c.sessionId === winnerSid);
              if (cli) {
                cli.send("coins_awarded", { matchId, delta, reason: "win" });
                if (newTotals) {
                  cli.send("wallet_sync", {
                    totalCO: newTotals[VC_PRIMARY] ?? 0,
                    totalGE: newTotals[VC_SECOND] ?? 0,
                  });
                }
              }
              console.log(`💰 PlayFab +${delta}${VC_PRIMARY} a ${pfid} (saldo: ${newTotals?.[VC_PRIMARY]})`);
            })
            .catch(err => {
              console.error("PlayFab award error", err);
              const cli = this.clients.find(c => c.sessionId === winnerSid);
              if (cli) cli.send("coins_awarded", { matchId, delta: 20, reason: "win" });
            });
        } else {
          const cli = this.clients.find(c => c.sessionId === winnerSid);
          if (cli) cli.send("coins_awarded", { matchId, delta: 20, reason: "win" });
          console.warn("⚠️ PlayFab non configurato: accredito simulato lato server.");
        }
      }
    }

    this._clearAllTimers();
    setTimeout(() => this.disconnect(), 1500);
  }

  private _clearAllTimers() {
    if (this.interval) { clearInterval(this.interval); this.interval = null; }
    if (this.autostartTimeout) { clearTimeout(this.autostartTimeout); this.autostartTimeout = null; }
    for (const b of this.bots) if (b.timer) clearInterval(b.timer);
    this.bots = [];

    if (this.leaderboardTimer) { clearInterval(this.leaderboardTimer); this.leaderboardTimer = null; }
    this.leaderboardDirty = false;

    this.awardedTokens.clear();
    this.matchId = null;
  }

  /* =========================
     PlayFab helpers
     ========================= */
  private async _pfGetBalances(pfid: string): Promise<Record<string, number> | null> {
    if (!PF_HOST || !PF_SECRET) return null;
    const res = await fetch(`${PF_HOST}/Server/GetUserInventory`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-SecretKey": PF_SECRET },
      body: JSON.stringify({ PlayFabId: pfid }),
    });
    const json = await res.json();
    if (json.code !== 200) {
      console.error("GetUserInventory error:", json);
      return null;
    }
    const vc = json.data?.VirtualCurrency || {};
    return vc as Record<string, number>;
  }

  private async _pfAddCurrency(pfid: string, code: string, amount: number): Promise<Record<string, number> | null> {
    if (!PF_HOST || !PF_SECRET) return null;
    const res = await fetch(`${PF_HOST}/Server/AddUserVirtualCurrency`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-SecretKey": PF_SECRET },
      body: JSON.stringify({ PlayFabId: pfid, VirtualCurrency: code, Amount: amount }),
    });
    const json = await res.json();
    if (json.code !== 200) {
      console.error("AddUserVirtualCurrency error:", json);
      return null;
    }
    return await this._pfGetBalances(pfid);
  }
}
