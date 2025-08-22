import { Room, Client } from "@colyseus/core";
import { Schema, type, MapSchema } from "@colyseus/schema";

class PlayerState extends Schema {
  @type("number") numero_giocatore: number = 0;
  @type("number") x: number = 0;
  @type("number") y: number = 0;
  @type("number") z: number = 0;
  @type("number") punti: number = 0;
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

  /** ID del match corrente (usato per idempotenza e log) */
  matchId: string | null = null;
  /** Evita invii doppi di premi: chiave = `${sessionId}:${matchId}` */
  private awardedTokens = new Set<string>();

  onCreate() {
    this.setState(new DerbyState());
    this.autoDispose = true;
    console.log("🏁 Room creata:", this.roomId);

    // POSIZIONE
    this.onMessage("posizione", (client, msg: { x: number; y: number; z: number }) => {
      if (this.matchTerminato) return;
      const p = this.state.players.get(client.sessionId);
      if (!p) return;
      p.x = msg.x; p.y = msg.y; p.z = msg.z;
      this.broadcast(
        "pos_update",
        { sessionId: client.sessionId, numero_giocatore: p.numero_giocatore, x: p.x, y: p.y, z: p.z },
        { except: client }
      );
    });

    // PUNTI
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
      if (!this.matchTerminato && p.punti >= this.puntiVittoria) {
        this._fineGara(client.sessionId, p.numero_giocatore, null);
      }
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

    // Start manuale
    this.onMessage("start_matchmaking", (client) => {
      console.log("📨 start_matchmaking da", client.sessionId);
      this._tryStartCountdown("MSG");
    });
  }

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

    // Genera e annuncia il matchId per idempotenza/log
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

    setTimeout(() => this._runBots(), 800);
  }

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
    this.state.players.set(client.sessionId, p);

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
    if (this.clients.length === 0 && !this.matchTerminato) {
      this._clearAllTimers();
      console.log("🧹 Room vuota, chiudo.");
      this.disconnect();
    }
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
        this.state.players.set(sid, ps);
        this.bots.push({ sid, numero: i });
        this.broadcast("giocatore_mappato", { sessionId: sid, numero_giocatore: i });
        console.log(`🤖 BOT creato: ${sid} → Player ${i}`);
      }
    }
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

  private _fineGara(winnerSid: string, numero: number, tempo: number | null) {
    if (this.matchTerminato) return;
    this.matchTerminato = true;

    const matchId = this.matchId ?? `${this.roomId}-${Date.now()}`; // fallback prudente
    console.log("🏁 Fine gara. Winner:", winnerSid, "Player", numero, "matchId:", matchId);

    // Broadcast fine gara con matchId
    this.broadcast("gara_finita", { matchId, winner: winnerSid, numero_giocatore: numero, tempo });

    // Premio solo giocatore umano, una volta sola
    if (!winnerSid.startsWith("BOT_")) {
      const token = `${winnerSid}:${matchId}`;
      if (!this.awardedTokens.has(token)) {
        this.awardedTokens.add(token);
        const winnerClient = this.clients.find(c => c.sessionId === winnerSid);
        if (winnerClient) {
          const delta = 20; // premio 1° posto confermato
          winnerClient.send("coins_awarded", { matchId, delta, reason: "win" });
          console.log(`💰 coins_awarded → ${winnerSid} +${delta} (${matchId})`);
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

    // reset contesto match/premi
    this.awardedTokens.clear();
    this.matchId = null;
  }
}
