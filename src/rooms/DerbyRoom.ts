import { Room, Client } from "@colyseus/core";
import { Schema, type, MapSchema } from "@colyseus/schema";

/* =========================
   PlayFab config (ENV)
   ========================= */
const PF_TITLE_ID = process.env.PLAYFAB_TITLE_ID || "";
const PF_SECRET = process.env.PLAYFAB_SECRET_KEY || "";
const PF_HOST = PF_TITLE_ID ? `https://${PF_TITLE_ID}.playfabapi.com` : "";

const VC_PRIMARY = "CO";
const VC_SECOND = "GE";

/* =========================
   RANKING
   ========================= */
const RANK_STAT = "RankRating";
const RANK_WIN_DELTA = 25;
const RANK_LOSE_DELTA = -5;
const RANK_MIN = 0;

/* =========================
   Track
   ========================= */
const TRACK_X_START = 0.0;
const STEP_X = 42.85;

/* =========================
   Bots (più lenti)
   ========================= */
const BOT_BASE_MS_MIN = 4500;
const BOT_BASE_MS_MAX = 9000;

const BOT_WEIGHTS = [
  { s: 1, w: 92 },
  { s: 2, w: 7 },
  { s: 4, w: 1 },
];

const BOT_POS_UPDATES = true;
const BOT_START_DELAY_MS = 4500;

/* =========================
   Bot cosmetics / names
   ========================= */
const BOT_MOUNT_SKINS_COUNT = 6;
const BOT_JOCKEY_SKINS_COUNT = 12;

const BOT_NAMES = [
  "Alberto",
  "Matteo",
  "Michele",
  "Lisa",
  "Ermes",
  "Marco",
  "Cristina",
  "Monica",
  "Alessandro",
  "Lillo",
];

/* =========================
   Badge offsets
   ========================= */
const AVATAR_ID_OFFSET = 4000;
const AVATAR_BG_ID_OFFSET = 5000;
const PLATE_ID_OFFSET = 6000;

/* =========================
   Helpers
   ========================= */
function safeNum(v: any, fallback = 0): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(n: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, n));
}

function randomInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function shuffleArray<T>(arr: T[]): T[] {
  const copy = [...arr];
  for (let i = copy.length - 1; i > 0; i--) {
    const j = randomInt(0, i);
    [copy[i], copy[j]] = [copy[j], copy[i]];
  }
  return copy;
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

  @type("number") mount_skin_id: number = 0;
  @type("number") jockey_skin_id: number = 0;

  @type("number") avatar_id: number = AVATAR_ID_OFFSET;
  @type("number") avatar_bg_id: number = AVATAR_BG_ID_OFFSET;
  @type("number") plate_id: number = PLATE_ID_OFFSET;
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
  leaderboardTimer: NodeJS.Timeout | null = null;
  leaderboardDirty = false;

  bots: BotInfo[] = [];
  matchId: string | null = null;

  private sid2pf = new Map<string, string>();
  private lastPosTs = new Map<string, number>();
  private readonly POS_MIN_INTERVAL_MS = 50;

  private lastActivity = new Map<string, number>();
  private readonly INACTIVITY_TIMEOUT = 60000;

  private matchEarnedCO = new Map<string, number>();

  onCreate() {
    this.setState(new DerbyState());
    this.autoDispose = true;

    this.clock.setInterval(() => {
      const now = Date.now();
      this.clients.forEach((client) => {
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

    this.onMessage("set_mount_skin", (client, msg: { skin_id: number }) => {
      try {
        this.lastActivity.set(client.sessionId, Date.now());
        const p = this.state.players.get(client.sessionId);
        if (!p) return;

        const skin = clamp((safeNum(msg?.skin_id, 0) | 0), 0, 23);
        p.mount_skin_id = skin;

        this.broadcast("mount_skin_update", {
          sessionId: client.sessionId,
          numero_giocatore: p.numero_giocatore,
          skin_id: p.mount_skin_id,
        });

        this._markLeaderboardDirty();
      } catch (e) {
        console.error("[set_mount_skin] error:", e);
      }
    });

    this.onMessage("set_jockey_skin", (client, msg: { skin_id: number }) => {
      try {
        this.lastActivity.set(client.sessionId, Date.now());
        const p = this.state.players.get(client.sessionId);
        if (!p) return;

        const skin = clamp((safeNum(msg?.skin_id, 0) | 0), 0, 23);
        p.jockey_skin_id = skin;

        this.broadcast("jockey_skin_update", {
          sessionId: client.sessionId,
          numero_giocatore: p.numero_giocatore,
          skin_id: p.jockey_skin_id,
        });

        this._markLeaderboardDirty();
      } catch (e) {
        console.error("[set_jockey_skin] error:", e);
      }
    });

    this.onMessage(
      "set_badge_cosmetics",
      (client, msg: { avatar_id: number; avatar_bg_id: number; plate_id: number }) => {
        try {
          this.lastActivity.set(client.sessionId, Date.now());
          const p = this.state.players.get(client.sessionId);
          if (!p) return;

          p.avatar_id = Math.max(AVATAR_ID_OFFSET, safeNum(msg?.avatar_id, AVATAR_ID_OFFSET) | 0);
          p.avatar_bg_id = Math.max(AVATAR_BG_ID_OFFSET, safeNum(msg?.avatar_bg_id, AVATAR_BG_ID_OFFSET) | 0);
          p.plate_id = Math.max(PLATE_ID_OFFSET, safeNum(msg?.plate_id, PLATE_ID_OFFSET) | 0);
        } catch (e) {
          console.error("[set_badge_cosmetics] error:", e);
        }
      }
    );

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

    this.onMessage("aggiorna_punti", (client, nuovi_punti: number) => {
      try {
        this.lastActivity.set(client.sessionId, Date.now());
        if (this.matchTerminato || !this.matchLanciato) return;

        const p = this.state.players.get(client.sessionId);
        if (!p) return;

        const old = p.punti;
        const requested = Math.min(Math.max((nuovi_punti | 0), 0), this.puntiVittoria);

        let delta = (requested - old) | 0;
        delta = clamp(delta, 0, 4);

        const target = Math.min(old + delta, this.puntiVittoria);
        if (target === old) return;

        p.punti = target;
        p.x = TRACK_X_START + STEP_X * p.punti;

        const prevCO = this.matchEarnedCO.get(client.sessionId) ?? 0;
        this.matchEarnedCO.set(client.sessionId, prevCO + delta);

        this.broadcast("punteggio_aggiornato", {
          sessionId: client.sessionId,
          numero_giocatore: p.numero_giocatore,
          punti: p.punti,
        });

        this.broadcast("pos_update", {
          sessionId: client.sessionId,
          numero_giocatore: p.numero_giocatore,
          x: p.x,
          y: p.y,
          z: p.z,
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
        const x = safeNum(msg?.x, p.x);
        const z = safeNum(msg?.z, p.z);

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
  }

  /* =========================
     Countdown / Start
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

    if (this.interval) {
      clearInterval(this.interval);
      this.interval = null;
    }

    this.lock();
    this._spawnBotsIfNeeded();
    this.matchId = `${this.roomId}-${Date.now()}`;
    this.matchEarnedCO.clear();

    this.broadcast("match_started", { matchId: this.matchId });
    this.broadcast("inizia_match");

    this._startLeaderboardTicker(300);

    setTimeout(() => {
      if (!this.matchTerminato) this._runBots();
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
        .then((vc) => {
          if (vc) {
            client.send("wallet_sync", {
              totalCO: vc[VC_PRIMARY] ?? 0,
              totalGE: vc[VC_SECOND] ?? 0,
              reason: "join",
            });
          }
        })
        .catch((err) => console.error("[_pfGetBalances] error:", err));

      this._pfGetRank(pfid)
        .then((curRank) => {
          client.send("rank_sync", { matchId: "", value: (curRank ?? 0) | 0, delta: 0 });
        })
        .catch((err) => console.error("[_pfGetRank][onJoin] error:", err));
    }

    client.send("numero_giocatore", numero);

    const skins: any[] = [];
    this.state.players.forEach((ps, sid) => {
      skins.push({
        sessionId: sid,
        numero_giocatore: ps.numero_giocatore,
        mount_skin_id: ps.mount_skin_id ?? 0,
        jockey_skin_id: ps.jockey_skin_id ?? 0,
        avatar_id: ps.avatar_id ?? AVATAR_ID_OFFSET,
        avatar_bg_id: ps.avatar_bg_id ?? AVATAR_BG_ID_OFFSET,
        plate_id: ps.plate_id ?? PLATE_ID_OFFSET,
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

  private async _buildWinnerBadgePayload(winnerSid: string) {
    const ps = this.state.players.get(winnerSid);

    if (!ps) {
      return {
        winner_avatar_id: AVATAR_ID_OFFSET,
        winner_avatar_bg_id: AVATAR_BG_ID_OFFSET,
        winner_plate_id: PLATE_ID_OFFSET,
        winner_rank: 0,
      };
    }

    if (winnerSid.startsWith("BOT_")) {
      return {
        winner_avatar_id: AVATAR_ID_OFFSET + (ps.jockey_skin_id | 0),
        winner_avatar_bg_id: AVATAR_BG_ID_OFFSET,
        winner_plate_id: PLATE_ID_OFFSET,
        winner_rank: 0,
      };
    }

    let rank = 0;
    const pfid = this.sid2pf.get(winnerSid);
    if (pfid) {
      rank = await this._pfGetRank(pfid);
    }

    return {
      winner_avatar_id: ps.avatar_id ?? AVATAR_ID_OFFSET,
      winner_avatar_bg_id: ps.avatar_bg_id ?? AVATAR_BG_ID_OFFSET,
      winner_plate_id: ps.plate_id ?? PLATE_ID_OFFSET,
      winner_rank: rank | 0,
    };
  }

  private async _fineGara(winnerSid: string, numero: number, tempo: number | null) {
    if (this.matchTerminato) return;

    this.matchTerminato = true;
    this._clearAllTimers();

    const matchId = this.matchId ?? `${this.roomId}-${Date.now()}`;
    const finalBoard = this._buildLeaderboardPayload();
    const winnerNick = this._getNicknameBySid(winnerSid);
    const winnerBadge = await this._buildWinnerBadgePayload(winnerSid);

    this.broadcast("gara_finita", {
      matchId,
      winner: winnerSid,
      numero_giocatore: numero,
      winner_points: this.puntiVittoria,
      winner_nick: winnerNick,
      tempo,
      classifica: finalBoard,
      ...winnerBadge,
    });

    try {
      await Promise.all(
        this.clients.map(async (cli) => {
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
              reason: "holes",
              delta: earned,
              matchId,
            });
            cli.send("coins_awarded", { matchId, delta: earned });
          }
        })
      );
    } catch (e) {
      console.error("[CO] payout error:", e);
    }

    try {
      await this._pfApplyRankAfterMatch(matchId, winnerSid);
    } catch (e) {
      console.error("[RANK] apply error:", e);
    }

    setTimeout(() => this.disconnect(), 10000);
  }

  /* =========================
     Bots
     ========================= */
  private _runBots() {
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
          if (r < k.w) {
            pick = k.s;
            break;
          }
          r -= k.w;
        }

        const old = ps.punti;
        ps.punti = Math.min(ps.punti + pick, this.puntiVittoria);
        ps.x = TRACK_X_START + STEP_X * ps.punti;

        this.broadcast("punteggio_aggiornato", {
          sessionId: bot.sid,
          numero_giocatore: bot.numero,
          punti: ps.punti,
        });

        if (BOT_POS_UPDATES) {
          this.broadcast("pos_update", {
            sessionId: bot.sid,
            numero_giocatore: bot.numero,
            x: ps.x,
            y: ps.y,
            z: ps.z,
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
    if (this.interval) {
      clearInterval(this.interval);
      this.interval = null;
    }

    if (this.leaderboardTimer) {
      clearInterval(this.leaderboardTimer);
      this.leaderboardTimer = null;
    }

    this.bots.forEach((b) => {
      if (b.timer) clearInterval(b.timer);
      b.timer = undefined;
    });
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
        body: JSON.stringify({ PlayFabId: pfid, StatisticNames: [RANK_STAT] }),
      });
      const json = await res.json().catch(() => null);
      if (json?.code !== 200) {
        console.error("[PF][getRank] FAILED:", {
          http: res.status,
          code: json?.code,
          error: json?.error,
          errorMessage: json?.errorMessage,
          pfid,
        });
        return 0;
      }
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

      if (json?.code !== 200) {
        console.error("[PF][setRank] FAILED:", {
          http: res.status,
          code: json?.code,
          error: json?.error,
          errorMessage: json?.errorMessage,
          pfid,
          newValue,
        });
        return false;
      }

      return true;
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
    } catch (e) {
      console.error("[PF][markRankApplied] error:", e);
    }
  }

  private async _pfApplyRankAfterMatch(matchId: string, winnerSid: string) {
    if (!PF_HOST || !PF_SECRET) {
      console.error("[RANK] PF env missing (PF_HOST/PF_SECRET).");
      return;
    }

    const entries: Array<{ sid: string; pfid: string }> = [];
    for (const c of this.clients) {
      const pfid = this.sid2pf.get(c.sessionId);
      if (pfid) entries.push({ sid: c.sessionId, pfid });
    }

    if (entries.length === 0) {
      console.error("[RANK] No human entries with playfabId.");
      return;
    }

    await Promise.all(
      entries.map(async ({ sid, pfid }) => {
        const delta = sid === winnerSid ? RANK_WIN_DELTA : RANK_LOSE_DELTA;

        try {
          const already = await this._pfWasRankAlreadyApplied(pfid, matchId);
          if (already) {
            const cur = await this._pfGetRank(pfid);
            const cli = this.clients.find((x) => x.sessionId === sid);
            if (cli) cli.send("rank_sync", { matchId, value: cur | 0, delta: 0 });
            return;
          }

          const cur = await this._pfGetRank(pfid);
          const next = Math.max(RANK_MIN, (cur + delta) | 0);

          const ok = await this._pfSetRank(pfid, next);
          if (ok) {
            await this._pfMarkRankApplied(pfid, matchId, delta, next);
          }

          const cli = this.clients.find((x) => x.sessionId === sid);
          if (cli) {
            cli.send("rank_sync", {
              matchId,
              value: (ok ? next : cur) | 0,
              delta: ok ? delta : 0,
            });
          }
        } catch (e) {
          console.error("[RANK] update error:", { pfid, sid, e });
        }
      })
    );
  }

  /* =========================
     Leaderboard helpers
     ========================= */
  private _buildLeaderboardPayload() {
    const list: any[] = [];
    this.state.players.forEach((ps, sid) =>
      list.push({
        sessionId: sid,
        numero_giocatore: ps.numero_giocatore,
        nickname: ps.nickname,
        punti: ps.punti,
        x: ps.x,
      })
    );
    return list.sort((a, b) => b.punti - a.punti || b.x - a.x);
  }

  private _startLeaderboardTicker(ms: number) {
    if (this.leaderboardTimer) {
      clearInterval(this.leaderboardTimer);
      this.leaderboardTimer = null;
    }

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

  private _assignNumeroGiocatore(): number {
    const used = new Set<number>();
    this.state.players.forEach((ps) => used.add(ps.numero_giocatore));
    for (let i = 1; i <= this.maxClients; i++) {
      if (!used.has(i)) return i;
    }
    return 1;
  }

  private _spawnBotsIfNeeded() {
    const usedNumbers = new Set<number>();
    this.state.players.forEach((ps) => usedNumbers.add(ps.numero_giocatore));

    const availableBotNames = shuffleArray(BOT_NAMES);

    const usedHumanJockeySkins = new Set<number>();
    this.state.players.forEach((ps, sid) => {
      if (!sid.startsWith("BOT_")) {
        const skin = safeNum(ps.jockey_skin_id, -1) | 0;
        if (skin >= 0 && skin < BOT_JOCKEY_SKINS_COUNT) {
          usedHumanJockeySkins.add(skin);
        }
      }
    });

    const availableBotJockeySkins: number[] = [];
    for (let i = 0; i < BOT_JOCKEY_SKINS_COUNT; i++) {
      if (!usedHumanJockeySkins.has(i)) {
        availableBotJockeySkins.push(i);
      }
    }

    const shuffledJockeyPool = shuffleArray(availableBotJockeySkins);

    for (let i = 1; i <= this.maxClients; i++) {
      if (!usedNumbers.has(i)) {
        const sid = `BOT_${this.roomId}_${i}`;
        const ps = new PlayerState();

        ps.numero_giocatore = i;
        ps.nickname = availableBotNames.length > 0 ? availableBotNames.shift()! : `BOT ${i}`;

        ps.mount_skin_id = randomInt(0, BOT_MOUNT_SKINS_COUNT - 1);

        if (shuffledJockeyPool.length > 0) {
          ps.jockey_skin_id = shuffledJockeyPool.shift()!;
        } else {
          ps.jockey_skin_id = randomInt(0, BOT_JOCKEY_SKINS_COUNT - 1);
        }

        ps.avatar_id = AVATAR_ID_OFFSET + (ps.jockey_skin_id | 0);
        ps.avatar_bg_id = AVATAR_BG_ID_OFFSET;
        ps.plate_id = PLATE_ID_OFFSET;

        this.state.players.set(sid, ps);
        this.bots.push({ sid, numero: i });

        this.broadcast("mount_skin_update", {
          sessionId: sid,
          numero_giocatore: ps.numero_giocatore,
          skin_id: ps.mount_skin_id,
        });

        this.broadcast("jockey_skin_update", {
          sessionId: sid,
          numero_giocatore: ps.numero_giocatore,
          skin_id: ps.jockey_skin_id,
        });
      }
    }
  }
}
