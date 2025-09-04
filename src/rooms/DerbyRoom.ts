import { Room, Client } from "@colyseus/core";
import { Schema, type, MapSchema } from "@colyseus/schema";

/* =========================
   PlayFab config (ENV)
   ========================= */
const PF_TITLE_ID = process.env.PLAYFAB_TITLE_ID || "";      // es. "142828"
const PF_SECRET   = process.env.PLAYFAB_SECRET_KEY || "";    // Title Secret Key
const PF_HOST     = PF_TITLE_ID ? `https://${PF_TITLE_ID}.playfabapi.com` : "";

// Valute (primaria = CO, secondaria opzionale = GE)
const VC_PRIMARY = "CO";
const VC_SECOND  = "GE";

/* =========================
   State
   ========================= */
class PlayerState extends Schema {
  @type("number") numero_giocatore: number = 0;
  @type("number") x: number = 0;
  @type("number") y: number = 0;
  @type("number") z: number = 0;
  @type("number") punti: number = 0;
  @type("string") nickname: string = "";   // opzionale: settato dal client
}

class DerbyState extends Schema {
  @type({ map: PlayerState }) players = new MapSchema<PlayerState>();
}

type BotInfo = { sid: string; numero: number; timer?: NodeJS.Timeout };

export class DerbyRoom extends Room<DerbyState> {
  /* =========================
     Parametri room
     ========================= */
  maxClients = 6;
  countdownSeconds = 10;
  minimoGiocatori = 1;         // cambia a 2 se vuoi avvio minimo 2 player
  puntiVittoria = 21;

  /* =========================
     Flag stato runtime
     ========================= */
  countdownStarted = false;
  matchLanciato = false;
  matchTerminato = false;
  tempoRimanente = this.countdownSeconds;

  /* =========================
     Timer & runtime refs
     ========================= */
  interval: NodeJS.Timeout | null = null;          // countdown
  autostartTimeout: NodeJS.Timeout | null = null;  // avvio automatico dopo join
  bots: BotInfo[] = [];

  leaderboardTimer: NodeJS.Timeout | null = null;  // invio classifica periodico
  leaderboardDirty = false;                        // debounce per on-change

  /** ID del match corrente (usato per idempotenza e log) */
  matchId: string | null = null;
  /** Evita invii doppi di premi: chiave = `${sessionId}:${matchId}` */
  private awardedTokens = new Set<string>();
  /** Mappa sessionId -> PlayFabId */
  private sid2pf = new Map<string, string>();

  onCreate() {
    this.setState(new DerbyState());
    this.autoDispose = true;
    console.log("🏁 Room creata:", this.roomId);

    /* ========== MESSAGGI CLIENT -> SERVER ========== */

    // POSIZIONE (x,y,z) aggiornate dal client
    this.onMessage("posizione", (client, msg: { x: number; y: number; z: number }) => {
      if (this.matchTerminato) return;
      const p = this.state.players.get(client.sessionId);
      if (!p) return;

      p.x = msg.x; p.y = msg.y; p.z = msg.z;

      // opzionale: broadcast delta posizione (utile per client legacy)
      this.broadcast(
        "pos_update",
        { sessionId: client.sessionId, numero_giocatore: p.numero_giocatore, x: p.x, y: p.y, z: p.z },
        { except: client }
      );

      // segna leaderboard “sporca” per invio rapido
      this._markLeaderboardDirty();
    });

    // PUNTI aggiornati dal client
    this.onMessage("aggiorna_punti", (client, nuovi_punti: number) => {
      if (this.matchTerminato) return;
      const p = this.state.players.get(client.sessionId);
      if (!p) return;

      const vecchi = p.punti;
      p.punti = nuovi_punti;
      console.log(`🏅 PUNTI ${client.sessionId} (P${p.numero_giocatore}) ${vecchi}→${p.punti}`);

      this.broadcast("punteggio_aggiornato", {
        sessionId: client.sessionId,
        numero_giocatore: p.numero_giocatore,
        punti: p.punti,
      });

      this._markLeaderboardDirty();

      if (!this.matchTerminato && p.punti >= this.puntiVittoria) {
        this._fineGara(client.sessionId, p.numero_giocatore, null);
      }
    });

    // Nickname facoltativo (per visual)
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
      // se la room è piena, avvia subito
      if (this.state.players.size >= this.maxClients) {
        this.lanciaMatch();
        return;
      }

      this.tempoRimanente -= 1;

      if (this.tempoRimanente >= 0) {
        this.broadcast("countdown_update", this.tempoRimanente);
        console.log(`⏳ Countdown: ${this.tempoRimanente}s`);
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

    this.lock(); // chiudi room a nuovi ingressi
    this._spawnBotsIfNeeded();

    // matchId per idempotenza/log
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

    // Timer classifica periodico (ogni 300 ms)
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
    // nickname suggerito dal client (opzionale)
    if (options?.nickname) {
      p.nickname = String(options.nickname).slice(0, 24);
    }
    this.state.players.set(client.sessionId, p);

    // PlayFabId dal client (per accrediti e sync)
    const pfid = String(options?.playfabId || "");
    if (pfid) {
      this.sid2pf.set(client.sessionId, pfid);
      // Wallet sync all’ingresso: manda CO e GE
      this._pfGetBalances(pfid).then(vc => {
        if (vc) {
          client.send("wallet_sync", {
            totalCO: vc[VC_PRIMARY] ?? 0,
            totalGE: vc[VC_SECOND] ?? 0,
          });
        }
      }).catch(() => {});
    }

    // broadcast mapping + numero al newcomer
    const snap: Array<{ sessionId: string; numero_giocatore: number }> = [];
    this.state.players.forEach((ps, sid) => snap.push({ sessionId: sid, numero_giocatore: ps.numero_giocatore }));
    this.broadcast("mappa_iniziale", snap);
    this.broadcast("giocatore_mappato", { sessionId: client.sessionId, numero_giocatore: numero });
    client.send("numero_giocatore", numero);

    // auto start?
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
    const botIdx = this.bots.findIndex(b => b.sid === client.sessionId);
    if (botIdx >= 0) {
      const bot = this.bots[botIdx];
      if (bot.timer) clearInterval(bot.timer);
      this.bots.splice(botIdx, 1);
    }
    this.state.players.delete(client.sessionId);
    this.sid2pf.delete(client.sessionId);

    // forza un refresh classifica se in countdown (ordine numeri può cambiare)
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

        ps.punti = Math.min(ps.punti + pick, this.puntiVittoria);
        this.broadcast("punteggio_aggiornato", { sessionId: bot.sid, numero_giocatore: bot.numero, punti: ps.punti });
        this._markLeaderboardDirty();

        if (!this.matchTerminato && ps.punti >= this.puntiVittoria) {
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
    list.sort((a, b) => {
      if (a.punti === b.punti) return b.x - a.x;
      return b.punti - a.punti;
    });

    return list;
  }

  /* =========================
     Fine gara + premi PlayFab
     ========================= */
  private _fineGara(winnerSid: string, numero: number, tempo: number | null) {
    if (this.matchTerminato) return;
    this.matchTerminato = true;

    const matchId = this.matchId ?? `${this.roomId}-${Date.now()}`; // fallback prudente
    console.log("🏁 Fine gara. Winner:", winnerSid, "Player", numero, "matchId:", matchId);

    // invia classifica finale
    const finalBoard = this._buildLeaderboardPayload();
    this.broadcast("classifica_update", finalBoard);

    // Notifica fine gara a tutti
    this.broadcast("gara_finita", { matchId, winner: winnerSid, numero_giocatore: numero, tempo });

    // Premio solo giocatore umano, una volta sola (in CO)
    if (!winnerSid.startsWith("BOT_")) {
      const token = `${winnerSid}:${matchId}`;
      if (!this.awardedTokens.has(token)) {
        this.awardedTokens.add(token);

        const pfid = this.sid2pf.get(winnerSid);
        if (pfid && PF_HOST && PF_SECRET) {
          const delta = 20; // premio 1º posto in CO
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
          // Env non configurato: fallback notifica locale, senza PlayFab
          const cli = this.clients.find(c => c.sessionId === winnerSid);
          if (cli) cli.send("coins_awarded", { matchId, delta: 20, reason: "win" });
          console.warn("⚠️ PlayFab non configurato: nessun accredito server-side.");
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

    // reset contesto match/premi
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
    // rileggi tutte le valute dopo l’accredito
    return await this._pfGetBalances(pfid);
  }
}
