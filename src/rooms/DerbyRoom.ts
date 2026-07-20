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
const RANK_MIN = 0;

type RankLeagueRule = {
  minRating: number;
  deltas: number[];
  forfeitPenalty: number;
};

// Deve restare allineato al catalogo RankManager del client. Il calcolo vero
// vive qui sul server: il client riceve soltanto valore e delta risultanti.
const RANK_LEAGUE_RULES: RankLeagueRule[] = [
  { minRating: 0, deltas: [40, 25, 15, -5, -5, -5], forfeitPenalty: 5 },
  { minRating: 150, deltas: [35, 22, 12, -10, -10, -10], forfeitPenalty: 10 },
  { minRating: 400, deltas: [30, 18, 10, -15, -15, -15], forfeitPenalty: 15 },
  { minRating: 800, deltas: [25, 15, 8, -20, -20, -20], forfeitPenalty: 20 },
  { minRating: 1400, deltas: [20, 12, 5, -25, -25, -25], forfeitPenalty: 25 },
];

type RankedAuth = {
  playfabId: string;
};

type ShotProof = {
  id: number;
  createdAt: number;
  consumed: boolean;
};

type RankSyncPayload = {
  matchId: string;
  previous_value: number;
  value: number;
  delta: number;
  placement: number;
  reason: "match" | "forfeit" | "sync";
};

const VALID_HOLE_SCORES = new Set([1, 2, 4]);
const MIN_SCORE_AFTER_SHOT_MS = 100;
const MAX_SCORE_AFTER_SHOT_MS = 45000;
const MIN_ACCEPTED_SCORE_INTERVAL_MS = 500;

// Spareggio pari-merito: se nessuno dei contendenti fa buca entro questo
// tempo, gli slot restanti vengono assegnati nell'ordine congelato all'avvio.
const SPAREGGIO_TIMEOUT_MS = 30000;

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
const BOT_MOUNT_SKINS_COUNT = 12;
const BOT_JOCKEY_SKINS_COUNT = 6;

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
   Badge composition defaults
   ========================= */
const HUMAN_ID_OFFSET = 1000;
const HAIR_ID_OFFSET = 2000;
const HAIR_COLOR_DEFAULT = 1;
const JOCKEY_ID_OFFSET = 4000;
const AVATAR_ID_OFFSET = JOCKEY_ID_OFFSET; // legacy alias for jockey_id
const AVATAR_BG_ID_OFFSET = 7000;
const FRAME_ID_OFFSET = 8000;
const PLATE_ID_OFFSET = 9000;

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

function getRankRule(rating: number): RankLeagueRule {
  const safeRating = Math.max(RANK_MIN, rating | 0);
  let selected = RANK_LEAGUE_RULES[0];

  for (const rule of RANK_LEAGUE_RULES) {
    if (rule.minRating > safeRating) break;
    selected = rule;
  }

  return selected;
}

function getRankDeltaForPlacement(rating: number, placement: number): number {
  const rule = getRankRule(rating);
  const safePlacement = clamp(placement | 0, 1, rule.deltas.length);
  return rule.deltas[safePlacement - 1] ?? 0;
}

async function authenticatePlayFabSessionTicket(sessionTicket: string): Promise<RankedAuth> {
  if (!PF_HOST || !PF_SECRET) {
    throw new Error("RANK_AUTH_SERVER_NOT_CONFIGURED");
  }

  const ticket = String(sessionTicket || "").trim();
  if (!ticket) {
    throw new Error("PLAYFAB_SESSION_TICKET_MISSING");
  }

  const response = await fetch(`${PF_HOST}/Server/AuthenticateSessionTicket`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-SecretKey": PF_SECRET,
    },
    body: JSON.stringify({ SessionTicket: ticket }),
  });

  const json = await response.json().catch(() => null);
  const expired = json?.data?.IsSessionTicketExpired === true;
  const playfabId = String(json?.data?.UserInfo?.PlayFabId || "").trim();

  if (!response.ok || json?.code !== 200 || expired || !playfabId) {
    throw new Error(expired ? "PLAYFAB_SESSION_EXPIRED" : "PLAYFAB_SESSION_INVALID");
  }

  return { playfabId };
}

function botJockeyId(jockeySkinId: number): number {
  return JOCKEY_ID_OFFSET + Math.max(0, jockeySkinId | 0);
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
  @type("number") frame_id: number = FRAME_ID_OFFSET;

  @type("number") human_id: number = HUMAN_ID_OFFSET;
  @type("number") hair_id: number = HAIR_ID_OFFSET;
  @type("number") hair_color: number = HAIR_COLOR_DEFAULT;
  @type("number") jockey_id: number = JOCKEY_ID_OFFSET;
}

class DerbyState extends Schema {
  @type({ map: PlayerState }) players = new MapSchema<PlayerState>();
}

type BotInfo = {
  sid: string;
  numero: number;
  timer?: NodeJS.Timeout;
};

type FinishEntry = {
  sid: string;
  numero: number;
  position: number;
  nickname: string;
  punti: number;
  x: number;
  isBot: boolean;
};

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
  finishedOrder: FinishEntry[] = [];
  matchId: string | null = null;

  // Regole podio: il primo arrivo a 21 chiude la gara; 2° e 3° vanno ai piu'
  // vicini al traguardo in quel momento. I pari merito sul podio si risolvono
  // con lo spareggio "prima buca vince" tra i soli contendenti.
  spareggioActive = false;
  spareggioPool: string[] = [];
  spareggioOpenPositions: number[] = [];
  spareggioTimer: NodeJS.Timeout | null = null;

  // Finisher usciti prima di fine gara (es. il vincitore che non aspetta lo
  // spareggio): snapshot per applicare comunque rank e CO a fine match.
  private departedFinishers = new Map<
    string,
    { pfid: string; earnedCO: number; placement: number }
  >();

  // Cosmetici salvati al momento del piazzamento: se il vincitore esce prima
  // della fine, il suo badge nella schermata finale resta corretto.
  private finishCosmeticsBySid = new Map<
    string,
    {
      human_id: number;
      hair_id: number;
      hair_color: number;
      jockey_id: number;
      avatar_id: number;
      avatar_bg_id: number;
      plate_id: number;
      frame_id: number;
    }
  >();

  private sid2pf = new Map<string, string>();
  private lastPosTs = new Map<string, number>();
  private readonly POS_MIN_INTERVAL_MS = 50;

  private lastShotBySid = new Map<string, ShotProof>();
  private lastScoreEventBySid = new Map<string, number>();
  private lastAcceptedScoreAtBySid = new Map<string, number>();
  private forfeitSids = new Set<string>();
  private forfeitRequests = new Map<string, Promise<RankSyncPayload>>();

  private lastActivity = new Map<string, number>();
  private readonly INACTIVITY_TIMEOUT = 60000;

  private matchEarnedCO = new Map<string, number>();
  private chestGrantInFlight = new Set<string>();
  private chestGrantDone = new Set<string>();

  onCreate(): void {
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

    this.onMessage("forfeit", async (client, msg: any) => {
      this.lastActivity.set(client.sessionId, Date.now());

      if (!this._isForfeitEligible(client.sessionId)) {
        client.send("forfeit_ack", {
          ok: false,
          reason: "FORFEIT_NOT_ELIGIBLE",
        });
        return;
      }

      try {
        const result = await this._applyForfeitPenalty(client.sessionId);
        client.send("forfeit_ack", {
          ok: true,
          event_id: String(msg?.event_id || ""),
          rank_sync: result,
        });
      } catch (e) {
        console.error("[FORFEIT] apply error:", e);
        client.send("forfeit_ack", {
          ok: false,
          reason: "FORFEIT_APPLY_FAILED",
        });
      }
    });

    this.onMessage("claim_ranked_chest", (client) => {
      try {
        this.lastActivity.set(client.sessionId, Date.now());

        const entry = this.finishedOrder.find(
          (item) => item.sid === client.sessionId && item.position >= 1 && item.position <= 3
        );
        if (!entry) {
          client.send("chest_award_failed", {
            matchId: this.matchId ?? "",
            position: 0,
            error: "NOT_TOP3_FINISHED",
          });
          return;
        }

        this._grantChestForFinishEntry(entry).catch((e) =>
          console.error("[CHEST] claim_ranked_chest error:", e)
        );
      } catch (e) {
        console.error("[claim_ranked_chest] error:", e);
      }
    });

    this.onMessage("set_mount_skin", (client, msg: { skin_id: number }) => {
      try {
        this.lastActivity.set(client.sessionId, Date.now());

        const p = this.state.players.get(client.sessionId);
        if (!p) return;

        const skin = Math.max(0, safeNum(msg?.skin_id, 0) | 0);
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

        const skin = Math.max(0, safeNum(msg?.skin_id, 0) | 0);
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
      (
        client,
        msg: {
          human_id?: number;
          hair_id?: number;
          hair_color?: number;
          jockey_id?: number;
          avatar_id?: number;
          badge_bg_id?: number;
          badge_plate_id?: number;
          badge_frame_id?: number;
          avatar_bg_id?: number;
          plate_id?: number;
          frame_id?: number;
        }
      ) => {
        try {
          this.lastActivity.set(client.sessionId, Date.now());

          const p = this.state.players.get(client.sessionId);
          if (!p) return;

          p.human_id = Math.max(
            HUMAN_ID_OFFSET,
            safeNum(msg?.human_id, HUMAN_ID_OFFSET) | 0
          );
          p.hair_id = Math.max(
            HAIR_ID_OFFSET,
            safeNum(msg?.hair_id, HAIR_ID_OFFSET) | 0
          );
          p.hair_color = clamp(
            safeNum(msg?.hair_color, HAIR_COLOR_DEFAULT) | 0,
            0,
            4
          );
          p.jockey_id = Math.max(
            JOCKEY_ID_OFFSET,
            safeNum(msg?.jockey_id, JOCKEY_ID_OFFSET) | 0
          );
          p.avatar_id = Math.max(
            AVATAR_ID_OFFSET,
            safeNum(msg?.avatar_id, p.jockey_id) | 0
          );
          p.avatar_bg_id = Math.max(
            AVATAR_BG_ID_OFFSET,
            safeNum(msg?.badge_bg_id ?? msg?.avatar_bg_id, AVATAR_BG_ID_OFFSET) | 0
          );
          p.plate_id = Math.max(
            PLATE_ID_OFFSET,
            safeNum(msg?.badge_plate_id ?? msg?.plate_id, PLATE_ID_OFFSET) | 0
          );
          p.frame_id = Math.max(
            FRAME_ID_OFFSET,
            safeNum(msg?.badge_frame_id ?? msg?.frame_id, FRAME_ID_OFFSET) | 0
          );

          this.broadcast("badge_cosmetics_update", {
            sessionId: client.sessionId,
            numero_giocatore: p.numero_giocatore,
            human_id: p.human_id,
            hair_id: p.hair_id,
            hair_color: p.hair_color,
            jockey_id: p.jockey_id,
            avatar_id: p.avatar_id,
            badge_bg_id: p.avatar_bg_id,
            badge_plate_id: p.plate_id,
            badge_frame_id: p.frame_id,
            avatar_bg_id: p.avatar_bg_id,
            plate_id: p.plate_id,
            frame_id: p.frame_id,
          });

          this._markLeaderboardDirty();
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
          {
            sessionId: client.sessionId,
            numero_giocatore: p.numero_giocatore,
            x: p.x,
            y: p.y,
            z: p.z,
          },
          { except: client }
        );

        this._markLeaderboardDirty();
      } catch (e) {
        console.error("[posizione] error:", e);
      }
    });

    // IMPORTANTE:
    // qui il payload è interpretato come punti della singola buca: 1 / 2 / 4
    this.onMessage("aggiorna_punti", (client, msg: any) => {
      try {
        this.lastActivity.set(client.sessionId, Date.now());
        if (this.matchTerminato || !this.matchLanciato) {
          this._rejectScore(client, "MATCH_NOT_ACTIVE");
          return;
        }

        const p = this.state.players.get(client.sessionId);
        if (!p) {
          this._rejectScore(client, "PLAYER_NOT_FOUND");
          return;
        }

        if (!msg || typeof msg !== "object") {
          this._rejectScore(client, "INVALID_SCORE_PAYLOAD");
          return;
        }

        const delta = safeNum(msg.points, 0) | 0;
        const shotId = safeNum(msg.shot_id, 0) | 0;
        const eventId = safeNum(msg.event_id, 0) | 0;
        const lastEventId = this.lastScoreEventBySid.get(client.sessionId) ?? 0;

        if (!VALID_HOLE_SCORES.has(delta)) {
          this._rejectScore(client, "INVALID_SCORE_VALUE");
          return;
        }

        if (eventId <= lastEventId) {
          this._rejectScore(client, "SCORE_EVENT_REPLAY");
          return;
        }
        this.lastScoreEventBySid.set(client.sessionId, eventId);

        const shot = this.lastShotBySid.get(client.sessionId);
        if (!shot || shot.id !== shotId || shot.consumed) {
          this._rejectScore(client, "SHOT_PROOF_MISSING");
          return;
        }

        const now = Date.now();
        const flightTime = now - shot.createdAt;
        if (flightTime < MIN_SCORE_AFTER_SHOT_MS || flightTime > MAX_SCORE_AFTER_SHOT_MS) {
          this._rejectScore(client, "SHOT_PROOF_EXPIRED");
          return;
        }

        const lastAcceptedAt = this.lastAcceptedScoreAtBySid.get(client.sessionId) ?? 0;
        if (now - lastAcceptedAt < MIN_ACCEPTED_SCORE_INTERVAL_MS) {
          this._rejectScore(client, "SCORE_RATE_LIMITED");
          return;
        }

        shot.consumed = true;
        this.lastShotBySid.set(client.sessionId, shot);
        this.lastAcceptedScoreAtBySid.set(client.sessionId, now);

        const old = p.punti;
        const target = Math.min(old + delta, this.puntiVittoria);
        if (target === old) return;

        const appliedDelta = target - old;

        p.punti = target;
        p.x = TRACK_X_START + STEP_X * p.punti;

        const prevCO = this.matchEarnedCO.get(client.sessionId) ?? 0;
        this.matchEarnedCO.set(client.sessionId, prevCO + appliedDelta);

        this.broadcast("punteggio_aggiornato", {
          sessionId: client.sessionId,
          numero_giocatore: p.numero_giocatore,
          punti: p.punti,
          delta: appliedDelta,
          shot_id: shotId,
          event_id: eventId,
        });

        this.broadcast("pos_update", {
          sessionId: client.sessionId,
          numero_giocatore: p.numero_giocatore,
          x: p.x,
          y: p.y,
          z: p.z,
        });

        this._markLeaderboardDirty();

        if (this.spareggioActive) {
          this._onSpareggioScore(client.sessionId);
        } else if (
          this.finishedOrder.length === 0 &&
          p.punti >= this.puntiVittoria &&
          old < this.puntiVittoria
        ) {
          this._registerFinish(client.sessionId);
        }
      } catch (e) {
        console.error("[aggiorna_punti] error:", e);
        this._rejectScore(client, "SCORE_INTERNAL_ERROR");
      }
    });

    this.onMessage("set_nickname", (client, nick: string) => {
      try {
        this.lastActivity.set(client.sessionId, Date.now());
        const p = this.state.players.get(client.sessionId);
        if (p) {
          p.nickname = (nick ?? "").toString().slice(0, 24);
        }
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
        const shotId = safeNum(msg?.shot_id, 0) | 0;

        const previousShot = this.lastShotBySid.get(client.sessionId);
        if (shotId <= 0 || (previousShot && shotId <= previousShot.id)) return;

        const forceLength = Math.hypot(fx, fz);
        if (!Number.isFinite(forceLength) || forceLength < 0.25 || forceLength > 250) return;

        const fxC = clamp(fx, -200, 200);
        const fzC = clamp(fz, -200, 200);

        this.lastShotBySid.set(client.sessionId, {
          id: shotId,
          createdAt: Date.now(),
          consumed: false,
        });

        this.broadcast(
          "lancio_update",
          {
            sessionId: client.sessionId,
            numero_giocatore: p.numero_giocatore,
            fx: fxC,
            fz: fzC,
            x,
            z,
            shot_id: shotId,
          },
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
  private _tryStartCountdown(_reason: string): void {
    if (this.countdownStarted || this.matchLanciato || this.matchTerminato) return;
    if (this.clients.length < this.minimoGiocatori) return;

    this.countdownStarted = true;
    this.tempoRimanente = this.countdownSeconds;
    this.broadcast("countdown_update", this.tempoRimanente);

    this.interval = setInterval(() => {
      this.tempoRimanente -= 1;
      this.broadcast("countdown_update", Math.max(0, this.tempoRimanente));

      if (
        this.tempoRimanente <= 0 ||
        this.state.players.size >= this.maxClients
      ) {
        this.lanciaMatch();
      }
    }, 1000);
  }

  private lanciaMatch(): void {
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
    this.chestGrantInFlight.clear();
    this.chestGrantDone.clear();
    this.finishedOrder = [];
    this.lastShotBySid.clear();
    this.lastScoreEventBySid.clear();
    this.lastAcceptedScoreAtBySid.clear();
    this.forfeitSids.clear();
    this.forfeitRequests.clear();
    this.spareggioActive = false;
    this.spareggioPool = [];
    this.spareggioOpenPositions = [];
    this.departedFinishers.clear();
    this.finishCosmeticsBySid.clear();

    this.broadcast("match_started", { matchId: this.matchId });
    this.broadcast("inizia_match", { matchId: this.matchId });

    this._startLeaderboardTicker(300);

    setTimeout(() => {
      if (!this.matchTerminato) {
        this._runBots();
      }
    }, BOT_START_DELAY_MS);
  }

  /* =========================
     Join / Leave
     ========================= */
  async onAuth(_client: Client, options: any): Promise<RankedAuth> {
    const auth = await authenticatePlayFabSessionTicket(
      String(options?.sessionTicket || options?.session_ticket || "")
    );

    const claimedPlayFabId = String(options?.playfabId || "").trim();
    if (claimedPlayFabId && claimedPlayFabId !== auth.playfabId) {
      throw new Error("PLAYFAB_IDENTITY_MISMATCH");
    }

    if (Array.from(this.sid2pf.values()).includes(auth.playfabId)) {
      throw new Error("PLAYFAB_ACCOUNT_ALREADY_IN_ROOM");
    }

    return auth;
  }

  onJoin(client: Client, options: any, auth: RankedAuth): void {
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

    const pfid = String(auth?.playfabId || "").trim();
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
          client.send("rank_sync", {
            matchId: "",
            previous_value: (curRank ?? 0) | 0,
            value: (curRank ?? 0) | 0,
            delta: 0,
            placement: 0,
            reason: "sync",
          });
        })
        .catch((err) => console.error("[_pfGetRank][onJoin] error:", err));
    }

    client.send("numero_giocatore", numero);

    const skins: any[] = [];
    this.state.players.forEach((ps, sid) => {
      skins.push({
        sessionId: sid,
        numero_giocatore: ps.numero_giocatore,
        is_bot: sid.startsWith("BOT_"),
        mount_skin_id: ps.mount_skin_id ?? 0,
        jockey_skin_id: ps.jockey_skin_id ?? 0,
        human_id: ps.human_id ?? HUMAN_ID_OFFSET,
        hair_id: ps.hair_id ?? HAIR_ID_OFFSET,
        hair_color: ps.hair_color ?? HAIR_COLOR_DEFAULT,
        jockey_id: ps.jockey_id ?? JOCKEY_ID_OFFSET,
        avatar_id: ps.avatar_id ?? AVATAR_ID_OFFSET,
        badge_bg_id: ps.avatar_bg_id ?? AVATAR_BG_ID_OFFSET,
        badge_plate_id: ps.plate_id ?? PLATE_ID_OFFSET,
        badge_frame_id: ps.frame_id ?? FRAME_ID_OFFSET,
        avatar_bg_id: ps.avatar_bg_id ?? AVATAR_BG_ID_OFFSET,
        plate_id: ps.plate_id ?? PLATE_ID_OFFSET,
        frame_id: ps.frame_id ?? FRAME_ID_OFFSET,
      });
    });
    client.send("skins_snapshot", skins);

    this._markLeaderboardDirty();

    if (!this.countdownStarted && this.clients.length >= this.minimoGiocatori) {
      this._tryStartCountdown("AUTO");
    }
  }

  async onLeave(client: Client): Promise<void> {
    try {
      if (this._isForfeitEligible(client.sessionId)) {
        await this._applyForfeitPenalty(client.sessionId).catch((e) =>
          console.error("[FORFEIT][onLeave] error:", e)
        );
      }

      // Chi ha gia' un piazzamento puo' uscire liberamente (es. il vincitore
      // che non aspetta lo spareggio): salviamo pfid/CO/posizione per
      // applicare comunque rank e monete a fine gara.
      const finishedEntry = this.finishedOrder.find(
        (entry) => entry.sid === client.sessionId
      );
      if (finishedEntry) {
        const departedPfid = this.sid2pf.get(client.sessionId);
        if (departedPfid) {
          this.departedFinishers.set(client.sessionId, {
            pfid: departedPfid,
            earnedCO: this.matchEarnedCO.get(client.sessionId) ?? 0,
            placement: finishedEntry.position,
          });
        }
      }

      this._removeFromSpareggio(client.sessionId);

      this.lastActivity.delete(client.sessionId);
      this.lastPosTs.delete(client.sessionId);
      this.lastShotBySid.delete(client.sessionId);
      this.lastScoreEventBySid.delete(client.sessionId);
      this.lastAcceptedScoreAtBySid.delete(client.sessionId);
      this.sid2pf.delete(client.sessionId);
      this.matchEarnedCO.delete(client.sessionId);

      if (this.state.players.has(client.sessionId)) {
        this.state.players.delete(client.sessionId);
        this._markLeaderboardDirty();
      }

      const humansLeft = this.clients.length;
      if (humansLeft <= 0 && !this.matchTerminato) {
        if (this.spareggioActive) {
          // Ultimo umano uscito con spareggio in corso: risolvi subito con
          // l'ordine congelato e chiudi la gara (applica rank/CO ai partiti).
          this._resolveSpareggioByTimeout();
        } else if (this.matchLanciato && this.finishedOrder.length > 0) {
          // Gara decisa ma non chiusa: chiudila comunque, cosi' il vincitore
          // uscito in anticipo riceve rank e monete.
          await this._fineGara(null).catch((e) =>
            console.error("[_fineGara][onLeave] error:", e)
          );
        } else {
          this.matchTerminato = true;
          this._clearAllTimers();
          this.disconnect();
        }
      }
    } catch (e) {
      console.error("[onLeave] error:", e);
    }
  }

  onDispose(): void {
    this._clearAllTimers();
  }

  private _rejectScore(client: Client, reason: string): void {
    const player = this.state.players.get(client.sessionId);
    client.send("score_rejected", {
      reason,
      numero_giocatore: player?.numero_giocatore ?? 0,
      authoritative_score: player?.punti ?? 0,
    });
  }

  private _isForfeitEligible(sid: string): boolean {
    return (
      this.matchLanciato &&
      !this.matchTerminato &&
      this.state.players.has(sid) &&
      !this.finishedOrder.some((entry) => entry.sid === sid)
    );
  }

  private _applyForfeitPenalty(sid: string): Promise<RankSyncPayload> {
    const existing = this.forfeitRequests.get(sid);
    if (existing) return existing;

    this.forfeitSids.add(sid);

    const request = (async () => {
      const pfid = this.sid2pf.get(sid);
      if (!pfid) throw new Error("FORFEIT_PLAYFAB_ID_MISSING");

      const current = await this._pfGetRank(pfid);
      const penalty = getRankRule(current).forfeitPenalty;
      const matchId = this.matchId ?? `${this.roomId}-forfeit`;
      const eventId = `${matchId}:forfeit:${pfid}`;

      return this._pfApplyRankDelta(
        pfid,
        eventId,
        -penalty,
        0,
        "forfeit",
        matchId,
        current
      );
    })();

    this.forfeitRequests.set(sid, request);
    request.catch(() => {
      if (this.forfeitRequests.get(sid) === request) {
        this.forfeitRequests.delete(sid);
      }
    });
    return request;
  }

  /* =========================
     Fine gara
     ========================= */
  private _getNicknameBySid(sid: string): string {
    const ps = this.state.players.get(sid);
    return (ps?.nickname ?? "").toString().slice(0, 24);
  }

  private _getFinishedPosition(sid: string): number {
    const found = this.finishedOrder.find((x) => x.sid === sid);
    return found ? found.position : 0;
  }

  private _buildChestWinnersPayload(): any[] {
    return this.finishedOrder.slice(0, 3).map((entry) => ({
      position: entry.position,
      sessionId: entry.sid,
      numero_giocatore: entry.numero,
      nickname: entry.nickname,
      is_bot: entry.isBot,
      grant_chest: !entry.isBot,
      chest_type: "rotating",
    }));
  }

  private _registerFinish(sid: string): void {
    if (this.matchTerminato) return;

    const ps = this.state.players.get(sid);
    if (!ps) return;
    if (ps.punti < this.puntiVittoria) return;

    if (this.finishedOrder.some((entry) => entry.sid === sid)) return;

    const position = this.finishedOrder.length + 1;
    this._pushFinishEntry(sid, position, ps);

    // Nuove regole: il primo arrivo chiude la gara. 2° e 3° vengono assegnati
    // subito ai piu' vicini al traguardo (spareggio sui pari merito).
    if (position === 1) {
      this._assignPodiumAfterFirstArrival();
    }
  }

  private _pushFinishEntry(sid: string, position: number, ps: PlayerState): void {
    const entry: FinishEntry = {
      sid,
      numero: ps.numero_giocatore,
      position,
      nickname: (ps.nickname ?? "").toString().slice(0, 24),
      punti: ps.punti,
      x: ps.x,
      isBot: sid.startsWith("BOT_"),
    };

    this.finishedOrder.push(entry);

    this.finishCosmeticsBySid.set(sid, {
      human_id: ps.human_id ?? HUMAN_ID_OFFSET,
      hair_id: ps.hair_id ?? HAIR_ID_OFFSET,
      hair_color: ps.hair_color ?? HAIR_COLOR_DEFAULT,
      jockey_id: ps.jockey_id ?? JOCKEY_ID_OFFSET,
      avatar_id: ps.avatar_id ?? AVATAR_ID_OFFSET,
      avatar_bg_id: ps.avatar_bg_id ?? AVATAR_BG_ID_OFFSET,
      plate_id: ps.plate_id ?? PLATE_ID_OFFSET,
      frame_id: ps.frame_id ?? FRAME_ID_OFFSET,
    });

    this.broadcast("player_finished", {
      sessionId: sid,
      numero_giocatore: entry.numero,
      posizione: entry.position,
      position: entry.position,
      nickname: entry.nickname,
      punti: entry.punti,
      is_bot: entry.isBot,
      human_id: ps.human_id ?? HUMAN_ID_OFFSET,
      hair_id: ps.hair_id ?? HAIR_ID_OFFSET,
      hair_color: ps.hair_color ?? HAIR_COLOR_DEFAULT,
      jockey_id: ps.jockey_id ?? JOCKEY_ID_OFFSET,
      avatar_id: ps.avatar_id ?? AVATAR_ID_OFFSET,
      badge_bg_id: ps.avatar_bg_id ?? AVATAR_BG_ID_OFFSET,
      badge_plate_id: ps.plate_id ?? PLATE_ID_OFFSET,
      badge_frame_id: ps.frame_id ?? FRAME_ID_OFFSET,
      avatar_bg_id: ps.avatar_bg_id ?? AVATAR_BG_ID_OFFSET,
      plate_id: ps.plate_id ?? PLATE_ID_OFFSET,
      frame_id: ps.frame_id ?? FRAME_ID_OFFSET,
    });

    this._markLeaderboardDirty();
    this._grantChestForFinishEntry(entry).catch((e) =>
      console.error("[CHEST] immediate grant error:", e)
    );
  }

  // Registra un piazzamento per prossimita'/spareggio: con le nuove regole
  // solo il primo taglia davvero il traguardo, 2° e 3° chiudono con i loro
  // punti reali.
  private _registerPlacementEntry(sid: string, position: number): void {
    if (this.matchTerminato) return;
    if (this.finishedOrder.some((entry) => entry.sid === sid)) return;

    const ps = this.state.players.get(sid);
    if (!ps) return;

    this._pushFinishEntry(sid, position, ps);
  }

  private _assignPodiumAfterFirstArrival(): void {
    if (this.matchTerminato || this.spareggioActive) return;

    // Snapshot congelato al momento dell'arrivo del primo.
    const candidates: Array<{
      sid: string;
      punti: number;
      x: number;
      numero: number;
    }> = [];
    this.state.players.forEach((ps, sid) => {
      if (this.finishedOrder.some((entry) => entry.sid === sid)) return;
      candidates.push({
        sid,
        punti: ps.punti,
        x: ps.x,
        numero: ps.numero_giocatore,
      });
    });
    candidates.sort(
      (a, b) => b.punti - a.punti || b.x - a.x || a.numero - b.numero
    );

    let nextPosition = 2;
    let idx = 0;

    while (nextPosition <= 3 && idx < candidates.length) {
      const tieGroup = candidates.filter(
        (c, i) => i >= idx && c.punti === candidates[idx].punti
      );

      if (tieGroup.length === 1) {
        this._registerPlacementEntry(tieGroup[0].sid, nextPosition);
        nextPosition += 1;
        idx += 1;
        continue;
      }

      // Pari merito sul podio: spareggio "prima buca vince" tra i soli pari.
      const openPositions: number[] = [];
      for (let pos = nextPosition; pos <= 3; pos++) openPositions.push(pos);
      this._startSpareggio(
        tieGroup.map((c) => c.sid),
        openPositions
      );
      return;
    }

    this._fineGara(null).catch((e) => console.error("[_fineGara] error:", e));
  }

  private _startSpareggio(pool: string[], openPositions: number[]): void {
    this.spareggioActive = true;
    this.spareggioPool = [...pool];
    this.spareggioOpenPositions = [...openPositions];

    const contenders = this.spareggioPool.map((sid) => {
      const ps = this.state.players.get(sid);
      return {
        sessionId: sid,
        numero_giocatore: ps?.numero_giocatore ?? 0,
        nickname: (ps?.nickname ?? "").toString().slice(0, 24),
        punti: ps?.punti ?? 0,
        is_bot: sid.startsWith("BOT_"),
      };
    });

    this.broadcast("spareggio_start", {
      positions: [...this.spareggioOpenPositions],
      contenders,
      timeout_ms: SPAREGGIO_TIMEOUT_MS,
    });

    this.spareggioTimer = setTimeout(() => {
      this._resolveSpareggioByTimeout();
    }, SPAREGGIO_TIMEOUT_MS);

    this._trySettleSpareggio();
  }

  private _onSpareggioScore(sid: string): void {
    if (!this.spareggioActive) return;
    if (!this.spareggioPool.includes(sid)) return;
    if (this.spareggioOpenPositions.length === 0) return;

    const position = this.spareggioOpenPositions.shift()!;
    this.spareggioPool = this.spareggioPool.filter((s) => s !== sid);
    this._registerPlacementEntry(sid, position);
    this._trySettleSpareggio();
  }

  private _trySettleSpareggio(): void {
    if (!this.spareggioActive) return;

    // L'ultimo rimasto non ha piu' rivali: prende lo slot senza attese.
    while (
      this.spareggioPool.length === 1 &&
      this.spareggioOpenPositions.length > 0
    ) {
      const position = this.spareggioOpenPositions.shift()!;
      const sid = this.spareggioPool.pop()!;
      this._registerPlacementEntry(sid, position);
    }

    if (
      this.spareggioOpenPositions.length === 0 ||
      this.spareggioPool.length === 0
    ) {
      this._endSpareggio();
    }
  }

  private _resolveSpareggioByTimeout(): void {
    if (!this.spareggioActive) return;

    // Nessuna buca entro il tempo: assegna gli slot rimasti nell'ordine
    // congelato all'avvio dello spareggio.
    while (
      this.spareggioOpenPositions.length > 0 &&
      this.spareggioPool.length > 0
    ) {
      const position = this.spareggioOpenPositions.shift()!;
      const sid = this.spareggioPool.shift()!;
      this._registerPlacementEntry(sid, position);
    }

    this._endSpareggio();
  }

  private _endSpareggio(): void {
    if (this.spareggioTimer) {
      clearTimeout(this.spareggioTimer);
      this.spareggioTimer = null;
    }

    this.spareggioActive = false;
    this.spareggioPool = [];
    this.spareggioOpenPositions = [];

    this._fineGara(null).catch((e) => console.error("[_fineGara] error:", e));
  }

  private _removeFromSpareggio(sid: string): void {
    if (!this.spareggioActive) return;
    if (!this.spareggioPool.includes(sid)) return;

    this.spareggioPool = this.spareggioPool.filter((s) => s !== sid);
    this._trySettleSpareggio();
  }

  private async _buildWinnerBadgePayload(
    winnerSid: string
  ): Promise<{
    winner_human_id: number;
    winner_hair_id: number;
    winner_hair_color: number;
    winner_jockey_id: number;
    winner_avatar_id: number;
    winner_badge_bg_id: number;
    winner_badge_plate_id: number;
    winner_badge_frame_id: number;
    winner_avatar_bg_id: number;
    winner_plate_id: number;
    winner_frame_id: number;
    winner_rank: number;
  }> {
    const ps = this.state.players.get(winnerSid);

    if (!ps) {
      // Vincitore gia' uscito dalla room: usa lo snapshot dei cosmetici
      // salvato al piazzamento, e il rank dal pfid conservato.
      const snap = this.finishCosmeticsBySid.get(winnerSid);
      if (snap) {
        let rank = 0;
        const pfid =
          this.departedFinishers.get(winnerSid)?.pfid ??
          this.sid2pf.get(winnerSid);
        if (pfid && !winnerSid.startsWith("BOT_")) {
          try {
            rank = await this._pfGetRank(pfid);
          } catch {
            rank = 0;
          }
        }

        return {
          winner_human_id: snap.human_id,
          winner_hair_id: snap.hair_id,
          winner_hair_color: snap.hair_color,
          winner_jockey_id: snap.jockey_id,
          winner_avatar_id: snap.avatar_id,
          winner_badge_bg_id: snap.avatar_bg_id,
          winner_badge_plate_id: snap.plate_id,
          winner_badge_frame_id: snap.frame_id,
          winner_avatar_bg_id: snap.avatar_bg_id,
          winner_plate_id: snap.plate_id,
          winner_frame_id: snap.frame_id,
          winner_rank: rank | 0,
        };
      }

      return {
        winner_human_id: HUMAN_ID_OFFSET,
        winner_hair_id: HAIR_ID_OFFSET,
        winner_hair_color: HAIR_COLOR_DEFAULT,
        winner_jockey_id: JOCKEY_ID_OFFSET,
        winner_avatar_id: AVATAR_ID_OFFSET,
        winner_badge_bg_id: AVATAR_BG_ID_OFFSET,
        winner_badge_plate_id: PLATE_ID_OFFSET,
        winner_badge_frame_id: FRAME_ID_OFFSET,
        winner_avatar_bg_id: AVATAR_BG_ID_OFFSET,
        winner_plate_id: PLATE_ID_OFFSET,
        winner_frame_id: FRAME_ID_OFFSET,
        winner_rank: 0,
      };
    }

    if (winnerSid.startsWith("BOT_")) {
      const botJockey = botJockeyId(ps.jockey_skin_id);
      return {
        winner_human_id: 0,
        winner_hair_id: 0,
        winner_hair_color: 0,
        winner_jockey_id: botJockey,
        winner_avatar_id: botJockey,
        winner_badge_bg_id: 0,
        winner_badge_plate_id: 0,
        winner_badge_frame_id: 0,
        winner_avatar_bg_id: 0,
        winner_plate_id: 0,
        winner_frame_id: 0,
        winner_rank: 0,
      };
    }

    let rank = 0;
    const pfid = this.sid2pf.get(winnerSid);
    if (pfid) {
      rank = await this._pfGetRank(pfid);
    }

    return {
      winner_human_id: ps.human_id ?? HUMAN_ID_OFFSET,
      winner_hair_id: ps.hair_id ?? HAIR_ID_OFFSET,
      winner_hair_color: ps.hair_color ?? HAIR_COLOR_DEFAULT,
      winner_jockey_id: ps.jockey_id ?? JOCKEY_ID_OFFSET,
      winner_avatar_id: ps.avatar_id ?? AVATAR_ID_OFFSET,
      winner_badge_bg_id: ps.avatar_bg_id ?? AVATAR_BG_ID_OFFSET,
      winner_badge_plate_id: ps.plate_id ?? PLATE_ID_OFFSET,
      winner_badge_frame_id: ps.frame_id ?? FRAME_ID_OFFSET,
      winner_avatar_bg_id: ps.avatar_bg_id ?? AVATAR_BG_ID_OFFSET,
      winner_plate_id: ps.plate_id ?? PLATE_ID_OFFSET,
      winner_frame_id: ps.frame_id ?? FRAME_ID_OFFSET,
      winner_rank: rank | 0,
    };
  }

  private async _fineGara(tempo: number | null): Promise<void> {
    if (this.matchTerminato) return;

    this.matchTerminato = true;
    this._clearAllTimers();

    const matchId = this.matchId ?? `${this.roomId}-${Date.now()}`;
    const primo = this.finishedOrder[0] ?? null;
    const secondo = this.finishedOrder[1] ?? null;
    const terzo = this.finishedOrder[2] ?? null;

    const winnerSid = primo?.sid ?? "";
    const winnerNumero = primo?.numero ?? 0;
    const winnerNick = winnerSid ? this._getNicknameBySid(winnerSid) : "";
    const winnerBadge = winnerSid
      ? await this._buildWinnerBadgePayload(winnerSid)
      : await this._buildWinnerBadgePayload("");

    const chestWinners = this._buildChestWinnersPayload();
    const finalBoard = this._buildLeaderboardPayload();

    this.broadcast("gara_finita", {
      matchId,

      winner: winnerSid,
      numero_giocatore: winnerNumero,
      winner_points: this.puntiVittoria,
      winner_nick: winnerNick,
      tempo,

      primo,
      secondo,
      terzo,
      finished_order: this.finishedOrder,
      chest_winners: chestWinners,
      classifica: finalBoard,

      ...winnerBadge,
    });

    try {
      if (winnerSid) {
        await this._pfApplyRankAfterMatch(matchId);
      }
    } catch (e) {
      console.error("[RANK] apply error:", e);
    }

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

            cli.send("coins_awarded", {
              matchId,
              delta: earned,
            });
          }
        })
      );
    } catch (e) {
      console.error("[CO] payout error:", e);
    }

    // CO delle buche per i finisher usciti prima di fine gara: PlayFab viene
    // comunque accreditato, il wallet si riallinea al prossimo login.
    try {
      const departedPayouts: Array<{ pfid: string; earnedCO: number }> = [];
      this.departedFinishers.forEach((info) => {
        if (info.earnedCO > 0) departedPayouts.push(info);
      });

      await Promise.all(
        departedPayouts.map(async (info) => {
          await this._pfAddCurrency(info.pfid, VC_PRIMARY, info.earnedCO);
        })
      );
    } catch (e) {
      console.error("[CO] departed payout error:", e);
    }

    try {
      await this._pfGrantChestsToTop3(matchId);
    } catch (e) {
      console.error("[CHEST] top3 grant error:", e);
    }

    setTimeout(() => this.disconnect(), 10000);
  }

  /* =========================
     Bots
     ========================= */
  private _runBots(): void {
    const totalW = BOT_WEIGHTS.reduce((a, b) => a + b.w, 0);

    for (const bot of this.bots) {
      const baseMs =
        BOT_BASE_MS_MIN +
        Math.floor(Math.random() * (BOT_BASE_MS_MAX - BOT_BASE_MS_MIN));

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

        if (this.spareggioActive) {
          this._onSpareggioScore(bot.sid);
        } else if (
          this.finishedOrder.length === 0 &&
          ps.punti >= this.puntiVittoria &&
          old < this.puntiVittoria
        ) {
          this._registerFinish(bot.sid);
        }
      };

      bot.timer = setInterval(tick, baseMs);
    }
  }

  /* =========================
     Timers cleanup
     ========================= */
  private _clearAllTimers(): void {
    if (this.interval) {
      clearInterval(this.interval);
      this.interval = null;
    }

    if (this.leaderboardTimer) {
      clearInterval(this.leaderboardTimer);
      this.leaderboardTimer = null;
    }

    if (this.spareggioTimer) {
      clearTimeout(this.spareggioTimer);
      this.spareggioTimer = null;
    }

    this.bots.forEach((b) => {
      if (b.timer) {
        clearInterval(b.timer);
      }
      b.timer = undefined;
    });
  }

  /* =========================
     PlayFab helpers
     ========================= */
  private async _pfGrantRotatingChest(
    pfid: string,
    matchId: string,
    position: number
  ): Promise<any | null> {
    if (!PF_HOST || !PF_SECRET) return null;

    const res = await fetch(`${PF_HOST}/Server/ExecuteCloudScript`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-SecretKey": PF_SECRET,
      },
      body: JSON.stringify({
        PlayFabId: pfid,
        FunctionName: "AddRotatingChest",
        FunctionParameter: {
          source: "ranked_top3",
          matchId,
          position,
        },
        GeneratePlayStreamEvent: true,
      }),
    });

    const json = await res.json().catch(() => null);

    if (json?.code !== 200) {
      console.error("[PF][AddRotatingChest] FAILED:", {
        http: res.status,
        code: json?.code,
        error: json?.error,
        errorMessage: json?.errorMessage,
        pfid,
        matchId,
        position,
      });
      return null;
    }

    return json?.data?.FunctionResult ?? null;
  }

  private async _pfGrantChestsToTop3(matchId: string): Promise<void> {
    const top3 = this.finishedOrder.slice(0, 3);

    await Promise.all(
      top3.map(async (entry) => {
        await this._grantChestForFinishEntry(entry, matchId);
      })
    );
  }

  private _chestGrantKey(matchId: string, entry: FinishEntry): string {
    return `${matchId}:${entry.sid}:${entry.position}`;
  }

  private async _grantChestForFinishEntry(
    entry: FinishEntry,
    matchIdOverride: string | null = null
  ): Promise<void> {
    if (entry.isBot) return;
    if (entry.position < 1 || entry.position > 3) return;

    const matchId = matchIdOverride ?? this.matchId ?? `${this.roomId}-${Date.now()}`;
    const key = this._chestGrantKey(matchId, entry);
    if (this.chestGrantDone.has(key) || this.chestGrantInFlight.has(key)) return;

    const pfid = this.sid2pf.get(entry.sid);
    const cli = this.clients.find((x) => x.sessionId === entry.sid);
    if (!pfid) return;

    this.chestGrantInFlight.add(key);

    try {
      const result = await this._pfGrantRotatingChest(
        pfid,
        matchId,
        entry.position
      );

      if (!result || result.ok === false) {
        throw new Error("EMPTY_CHEST_RESULT");
      }

      this.chestGrantDone.add(key);

      if (cli) {
        cli.send("chest_awarded", {
          matchId,
          position: entry.position,
          result,
        });
      }
    } catch (e) {
      console.error("[CHEST] grant error:", {
        sid: entry.sid,
        pfid,
        position: entry.position,
        e,
      });

      if (cli) {
        cli.send("chest_award_failed", {
          matchId,
          position: entry.position,
          error: "CHEST_GRANT_FAILED",
        });
      }
    } finally {
      this.chestGrantInFlight.delete(key);
    }
  }

  private async _pfAddCurrency(
    pfid: string,
    code: string,
    amount: number
  ): Promise<any | null> {
    if (!PF_HOST || !PF_SECRET) return null;

    const res = await fetch(`${PF_HOST}/Server/AddUserVirtualCurrency`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-SecretKey": PF_SECRET,
      },
      body: JSON.stringify({
        PlayFabId: pfid,
        VirtualCurrency: code,
        Amount: amount,
      }),
    });

    const json = await res.json().catch(() => null);
    return json?.code === 200 ? this._pfGetBalances(pfid) : null;
  }

  private async _pfGetBalances(pfid: string): Promise<any | null> {
    if (!PF_HOST || !PF_SECRET) return null;

    const res = await fetch(`${PF_HOST}/Server/GetUserInventory`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-SecretKey": PF_SECRET,
      },
      body: JSON.stringify({
        PlayFabId: pfid,
      }),
    });

    const json = await res.json().catch(() => null);
    return json?.code === 200 ? json.data.VirtualCurrency : null;
  }

  private async _pfGetRank(pfid: string): Promise<number> {
    if (!PF_HOST || !PF_SECRET) return 0;

    try {
      const res = await fetch(`${PF_HOST}/Server/GetPlayerStatistics`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-SecretKey": PF_SECRET,
        },
        body: JSON.stringify({
          PlayFabId: pfid,
          StatisticNames: [RANK_STAT],
        }),
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

      return found && Number.isFinite(Number(found.Value))
        ? Number(found.Value)
        : 0;
    } catch (e) {
      console.error("[PF][getRank] error:", e);
      return 0;
    }
  }

  private async _pfSetRank(
    pfid: string,
    newValue: number
  ): Promise<boolean> {
    if (!PF_HOST || !PF_SECRET) return false;

    try {
      const res = await fetch(`${PF_HOST}/Server/UpdatePlayerStatistics`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-SecretKey": PF_SECRET,
        },
        body: JSON.stringify({
          PlayFabId: pfid,
          Statistics: [
            {
              StatisticName: RANK_STAT,
              Value: newValue | 0,
            },
          ],
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

  private async _pfWasRankAlreadyApplied(
    pfid: string,
    matchId: string
  ): Promise<boolean> {
    if (!PF_HOST || !PF_SECRET) return false;

    try {
      const res = await fetch(`${PF_HOST}/Server/GetUserReadOnlyData`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-SecretKey": PF_SECRET,
        },
        body: JSON.stringify({
          PlayFabId: pfid,
          Keys: ["rank_last_match_id"],
        }),
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

  private async _pfMarkRankApplied(
    pfid: string,
    matchId: string,
    delta: number,
    value: number
  ): Promise<void> {
    if (!PF_HOST || !PF_SECRET) return;

    try {
      await fetch(`${PF_HOST}/Server/UpdateUserReadOnlyData`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-SecretKey": PF_SECRET,
        },
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

  private async _pfApplyRankDelta(
    pfid: string,
    eventId: string,
    rawDelta: number,
    placement: number,
    reason: "match" | "forfeit",
    matchId: string,
    knownCurrent?: number
  ): Promise<RankSyncPayload> {
    const current = Number.isFinite(knownCurrent)
      ? Math.max(RANK_MIN, Number(knownCurrent) | 0)
      : await this._pfGetRank(pfid);
    const alreadyApplied = await this._pfWasRankAlreadyApplied(pfid, eventId);

    if (alreadyApplied) {
      return {
        matchId,
        previous_value: current | 0,
        value: current | 0,
        delta: 0,
        placement,
        reason,
      };
    }

    const next = Math.max(RANK_MIN, (current + rawDelta) | 0);
    const appliedDelta = next - current;
    const ok = await this._pfSetRank(pfid, next);

    if (!ok) {
      throw new Error("RANK_STAT_UPDATE_FAILED");
    }

    await this._pfMarkRankApplied(pfid, eventId, appliedDelta, next);

    return {
      matchId,
      previous_value: current | 0,
      value: next | 0,
      delta: appliedDelta,
      placement,
      reason,
    };
  }

  private async _pfApplyRankAfterMatch(matchId: string): Promise<void> {
    if (!PF_HOST || !PF_SECRET) {
      console.error("[RANK] PF env missing (PF_HOST/PF_SECRET).");
      return;
    }

    const placementBySid = new Map<string, number>();
    this._buildLeaderboardPayload().forEach((entry, index) => {
      placementBySid.set(String(entry.sessionId || ""), index + 1);
    });

    const entries: Array<{ sid: string; pfid: string; placement: number }> = [];

    for (const c of this.clients) {
      const pfid = this.sid2pf.get(c.sessionId);
      if (pfid && !this.forfeitSids.has(c.sessionId)) {
        entries.push({
          sid: c.sessionId,
          pfid,
          placement: placementBySid.get(c.sessionId) ?? this.maxClients,
        });
      }
    }

    // Finisher usciti prima di fine gara: delta applicato con la posizione
    // registrata al momento del piazzamento.
    this.departedFinishers.forEach((info, sid) => {
      if (this.forfeitSids.has(sid)) return;
      if (entries.some((entry) => entry.sid === sid)) return;
      entries.push({ sid, pfid: info.pfid, placement: info.placement });
    });

    if (entries.length === 0) {
      console.error("[RANK] No human entries with playfabId.");
      return;
    }

    await Promise.all(
      entries.map(async ({ sid, pfid, placement }) => {
        try {
          const current = await this._pfGetRank(pfid);
          const rawDelta = getRankDeltaForPlacement(current, placement);
          const result = await this._pfApplyRankDelta(
            pfid,
            matchId,
            rawDelta,
            placement,
            "match",
            matchId,
            current
          );

          const cli = this.clients.find((x) => x.sessionId === sid);
          if (cli) {
            cli.send("rank_sync", result);
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
  private _buildLeaderboardPayload(): any[] {
    const list: any[] = [];

    this.state.players.forEach((ps, sid) => {
      list.push({
        sessionId: sid,
        numero_giocatore: ps.numero_giocatore,
        nickname: ps.nickname,
        punti: ps.punti,
        x: ps.x,
        finished_position: this._getFinishedPosition(sid),
        is_bot: sid.startsWith("BOT_"),
        jockey_skin_id: ps.jockey_skin_id ?? 0,
        human_id: ps.human_id ?? HUMAN_ID_OFFSET,
        hair_id: ps.hair_id ?? HAIR_ID_OFFSET,
        hair_color: ps.hair_color ?? HAIR_COLOR_DEFAULT,
        jockey_id: ps.jockey_id ?? JOCKEY_ID_OFFSET,
        avatar_id: ps.avatar_id ?? AVATAR_ID_OFFSET,
        badge_bg_id: ps.avatar_bg_id ?? AVATAR_BG_ID_OFFSET,
        badge_plate_id: ps.plate_id ?? PLATE_ID_OFFSET,
        badge_frame_id: ps.frame_id ?? FRAME_ID_OFFSET,
        avatar_bg_id: ps.avatar_bg_id ?? AVATAR_BG_ID_OFFSET,
        plate_id: ps.plate_id ?? PLATE_ID_OFFSET,
        frame_id: ps.frame_id ?? FRAME_ID_OFFSET,
      });
    });

    return list.sort((a, b) => {
      const af = a.finished_position | 0;
      const bf = b.finished_position | 0;

      if (af > 0 && bf > 0) return af - bf;
      if (af > 0) return -1;
      if (bf > 0) return 1;

      return b.punti - a.punti || b.x - a.x;
    });
  }

  private _startLeaderboardTicker(ms: number): void {
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

  private _markLeaderboardDirty(): void {
    this.leaderboardDirty = true;
  }

  private _assignNumeroGiocatore(): number {
    const used = new Set<number>();

    this.state.players.forEach((ps) => {
      used.add(ps.numero_giocatore);
    });

    for (let i = 1; i <= this.maxClients; i++) {
      if (!used.has(i)) return i;
    }

    return 1;
  }

  private _spawnBotsIfNeeded(): void {
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
      if (usedNumbers.has(i)) continue;

      const sid = `BOT_${this.roomId}_${i}`;
      const ps = new PlayerState();

      ps.numero_giocatore = i;
      ps.nickname =
        availableBotNames.length > 0
          ? availableBotNames.shift()!
          : `BOT ${i}`;

      ps.mount_skin_id = randomInt(0, BOT_MOUNT_SKINS_COUNT - 1);

      if (shuffledJockeyPool.length > 0) {
        ps.jockey_skin_id = shuffledJockeyPool.shift()!;
      } else {
        ps.jockey_skin_id = randomInt(0, BOT_JOCKEY_SKINS_COUNT - 1);
      }

      ps.human_id = 0;
      ps.hair_id = 0;
      ps.hair_color = 0;
      ps.jockey_id = botJockeyId(ps.jockey_skin_id);
      ps.avatar_id = ps.jockey_id;
      ps.avatar_bg_id = 0;
      ps.plate_id = 0;
      ps.frame_id = 0;

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

    this._markLeaderboardDirty();
  }
}
