// ===== Derby Day – CloudScript Classic =====
// ===== CORE / BASE =====
//
// RANGE UFFICIALI:
// human:       1000–1999
// hair:        2000–2999
// hat:         3000–3999
// jockey:      4000–4999
// mount:       5000–5999
// ball:        6000–6999
// badge_bg:    7000–7999
// badge_frame: 8000–8999
// badge_plate: 9000–9999
//
// Nessun legacy:
// niente SkinManager, SkinDef, avatar, avatar_bg, plate, frame.
// Tutto passa da cosmetics / equipped.
// Modalità ex single rinominata: journey.

// =====================================================
// HELPERS GENERALI
// =====================================================

function toInt(x, defVal) {
    var n = parseInt(x, 10);
    return isNaN(n) ? defVal : n;
}

function toFloat(x, defVal) {
    var n = Number(x);
    return isNaN(n) ? defVal : n;
}

function toBool(x, defVal) {
    if (x === true) return true;
    if (x === false) return false;
    if (x === "true") return true;
    if (x === "false") return false;
    if (x === 1) return true;
    if (x === 0) return false;
    return !!defVal;
}

function asString(x, defVal) {
    if (x === null || x === undefined) return defVal || "";
    return String(x);
}

function safeJsonParse(s, defVal) {
    if (!s) return defVal;

    try {
        return JSON.parse(s);
    } catch (e) {
        return defVal;
    }
}

function nowUnix() {
    return Math.floor(Date.now() / 1000);
}

function todayKeyUtc() {
    var d = new Date();
    var y = d.getUTCFullYear();
    var m = ("0" + (d.getUTCMonth() + 1)).slice(-2);
    var day = ("0" + d.getUTCDate()).slice(-2);
    return y + "-" + m + "-" + day;
}

function clone(obj) {
    if (obj === null || obj === undefined) return obj;
    return JSON.parse(JSON.stringify(obj));
}

function makeSafeInternalKey(prefix, rawId) {
    var s = asString(rawId, "").trim();
    s = s.replace(/[^a-zA-Z0-9_\-]/g, "_");
    return prefix + s;
}

function sanitizeStringArray(arr) {
    var out = [];
    if (!(arr instanceof Array)) return out;

    for (var i = 0; i < arr.length; i++) {
        var s = asString(arr[i], "").trim();
        if (!s) continue;
        if (out.indexOf(s) !== -1) continue;
        out.push(s);
    }

    return out;
}

function sanitizeLowerStringArray(arr) {
    var out = [];
    if (!(arr instanceof Array)) return out;

    for (var i = 0; i < arr.length; i++) {
        var s = asString(arr[i], "").trim().toLowerCase();
        if (!s) continue;
        if (out.indexOf(s) !== -1) continue;
        out.push(s);
    }

    return out;
}

function sanitizeUnlockIdArray(arr) {
    return sanitizeLowerStringArray(arr);
}

function getRecordValue(record) {
    if (record == null) return "";
    if (typeof record === "object" && record.Value != null) return String(record.Value);
    return asString(record, "");
}

var titleDataCache = {};
var TITLE_DATA_CACHE_TTL_MS = 2000;

function cloneJson(value, fallback) {
    try {
        return JSON.parse(JSON.stringify(value));
    } catch (e) {
        return fallback;
    }
}

function readTitleDataJson(key, defVal) {
    key = asString(key, "").trim();
    if (!key) return defVal;

    var nowMs = new Date().getTime();
    var cached = titleDataCache[key];

    if (cached && nowMs - cached.loaded_at_ms <= TITLE_DATA_CACHE_TTL_MS) {
        return cloneJson(cached.value, defVal);
    }

    var value = defVal;

    try {
        var td = server.GetTitleData({ Keys: [key] });

        if (td && td.Data && td.Data[key]) {
            value = safeJsonParse(td.Data[key], defVal);
        }
    } catch (e) {}

    titleDataCache[key] = {
        loaded_at_ms: nowMs,
        value: cloneJson(value, defVal)
    };

    return cloneJson(value, defVal);
}

function hashString32(str) {
    str = asString(str, "");

    var h = 2166136261;

    for (var i = 0; i < str.length; i++) {
        h ^= str.charCodeAt(i);
        h += (h << 1) + (h << 4) + (h << 7) + (h << 8) + (h << 24);
    }

    return h >>> 0;
}

function seededUnitInterval(seed) {
    var h = hashString32(seed);
    return (h % 1000000) / 1000000.0;
}

function sortStringsAsc(arr) {
    arr.sort(function (a, b) {
        a = asString(a, "");
        b = asString(b, "");

        if (a < b) return -1;
        if (a > b) return 1;
        return 0;
    });

    return arr;
}

function isTruthy(v) {
    return v === true || v === "true" || v === 1 || v === "1";
}

// =====================================================
// STORAGE HELPERS
// =====================================================

function getReadOnlyState(keys) {
    var out = {};
    if (!(keys instanceof Array) || keys.length === 0) return out;

    var res = server.GetUserReadOnlyData({
        PlayFabId: currentPlayerId,
        Keys: keys
    });

    var data = (res && res.Data) ? res.Data : {};

    for (var i = 0; i < keys.length; i++) {
        var k = keys[i];
        var raw = getRecordValue(data[k]);
        out[k] = safeJsonParse(raw, null);
    }

    return out;
}

function updateReadOnlyState(obj) {
    var data = {};

    for (var k in obj) {
        if (!obj.hasOwnProperty(k)) continue;
        data[k] = JSON.stringify(obj[k]);
    }

    if (Object.keys(data).length <= 0) return;

    server.UpdateUserReadOnlyData({
        PlayFabId: currentPlayerId,
        Data: data
    });
}

function getUserDataState(keys) {
    var out = {};
    if (!(keys instanceof Array) || keys.length === 0) return out;

    var res = server.GetUserData({
        PlayFabId: currentPlayerId,
        Keys: keys
    });

    var data = (res && res.Data) ? res.Data : {};

    for (var i = 0; i < keys.length; i++) {
        var k = keys[i];
        var raw = getRecordValue(data[k]);
        out[k] = safeJsonParse(raw, null);
    }

    return out;
}

function updateUserDataState(obj) {
    var data = {};

    for (var k in obj) {
        if (!obj.hasOwnProperty(k)) continue;
        data[k] = JSON.stringify(obj[k]);
    }

    if (Object.keys(data).length <= 0) return;

    server.UpdateUserData({
        PlayFabId: currentPlayerId,
        Data: data
    });
}

function getInternalDataRaw(keys) {
    if (!(keys instanceof Array) || keys.length === 0) return {};

    var res = server.GetUserInternalData({
        PlayFabId: currentPlayerId,
        Keys: keys
    });

    return (res && res.Data) ? res.Data : {};
}

function updateInternalStateRaw(obj) {
    var data = {};

    for (var k in obj) {
        if (!obj.hasOwnProperty(k)) continue;
        data[k] = JSON.stringify(obj[k]);
    }

    if (Object.keys(data).length <= 0) return;

    server.UpdateUserInternalData({
        PlayFabId: currentPlayerId,
        Data: data
    });
}

// =====================================================
// COSTANTI
// =====================================================

var ITEM_COSMETIC = "cosmetic";
var ITEM_CHEST = "chest";
var ITEM_CURRENCY = "currency";
var ITEM_BUNDLE = "bundle";
var ITEM_UNLOCK = "unlock";

var PRICE_FREE = "free";
var PRICE_CO = "co";
var PRICE_GE = "ge";
var PRICE_AD = "ad";
var PRICE_REAL = "real";

var RULE_REPEATABLE = "repeatable";
var RULE_DAILY_ONCE = "daily_once";
var RULE_ACCOUNT_ONCE = "account_once";

var CHEST_STATUS_IDLE = "idle";
var CHEST_STATUS_QUEUED = "queued";
var CHEST_STATUS_OPENING = "opening";
var CHEST_STATUS_READY = "ready";

var MAX_CHEST_SLOTS = 4;
var MAX_CHEST_QUEUE = 1;

var DAILY_ROT_FIXED = "fixed";
var DAILY_ROT_DETERMINISTIC = "deterministic";
var DAILY_ROT_RANDOM_WEIGHTED = "random_weighted";

var CP_NEED_NAME = 0;
var CP_NAME_DONE = 1;
var CP_HUMAN_DONE = 2;
var CP_HAIR_DONE = 3;
var CP_ONBOARDING_DONE = 4;
var CP_TUTORIAL_DONE = 5;

var CAT_HUMAN = "human";
var CAT_HAIR = "hair";
var CAT_HAT = "hat";
var CAT_JOCKEY = "jockey";
var CAT_MOUNT = "mount";
var CAT_BALL = "ball";
var CAT_BADGE_BG = "badge_bg";
var CAT_BADGE_FRAME = "badge_frame";
var CAT_BADGE_PLATE = "badge_plate";

var VALID_COSMETIC_CATEGORIES = [
    CAT_HUMAN,
    CAT_HAIR,
    CAT_HAT,
    CAT_JOCKEY,
    CAT_MOUNT,
    CAT_BALL,
    CAT_BADGE_BG,
    CAT_BADGE_FRAME,
    CAT_BADGE_PLATE
];

var DEFAULT_HUMAN_ID = 1000;
var DEFAULT_HAIR_ID = 2000;
var DEFAULT_HAIR_COLOR = 1;
var DEFAULT_HAT_ID = 3000;
var DEFAULT_JOCKEY_ID = 4000;
var DEFAULT_MOUNT_ID = 5000;
var DEFAULT_BALL_ID = 6000;
var DEFAULT_BADGE_BG_ID = 7000;
var DEFAULT_BADGE_FRAME_ID = 8000;
var DEFAULT_BADGE_PLATE_ID = 9000;

var DEFAULT_UNLOCKED_COSMETICS = {
    "1000": true,
    "2000": true,
    "3000": true,
    "4000": true,
    "5000": true,
    "6000": true,
    "7000": true,
    "8000": true,
    "9000": true
};

// =====================================================
// RANGE / CATEGORY HELPERS
// =====================================================

function sanitizeCosmeticCategory(category) {
    var s = asString(category, "").trim().toLowerCase();

    if (VALID_COSMETIC_CATEGORIES.indexOf(s) !== -1) {
        return s;
    }

    return "";
}

function getCategoryFromCosmeticId(cosmeticId) {
    var id = toInt(cosmeticId, 0);

    if (id >= 1000 && id <= 1999) return CAT_HUMAN;
    if (id >= 2000 && id <= 2999) return CAT_HAIR;
    if (id >= 3000 && id <= 3999) return CAT_HAT;
    if (id >= 4000 && id <= 4999) return CAT_JOCKEY;
    if (id >= 5000 && id <= 5999) return CAT_MOUNT;
    if (id >= 6000 && id <= 6999) return CAT_BALL;
    if (id >= 7000 && id <= 7999) return CAT_BADGE_BG;
    if (id >= 8000 && id <= 8999) return CAT_BADGE_FRAME;
    if (id >= 9000 && id <= 9999) return CAT_BADGE_PLATE;

    return "";
}

function getDefaultCosmeticIdForCategory(category) {
    category = sanitizeCosmeticCategory(category);

    if (category === CAT_HUMAN) return DEFAULT_HUMAN_ID;
    if (category === CAT_HAIR) return DEFAULT_HAIR_ID;
    if (category === CAT_HAT) return DEFAULT_HAT_ID;
    if (category === CAT_JOCKEY) return DEFAULT_JOCKEY_ID;
    if (category === CAT_MOUNT) return DEFAULT_MOUNT_ID;
    if (category === CAT_BALL) return DEFAULT_BALL_ID;
    if (category === CAT_BADGE_BG) return DEFAULT_BADGE_BG_ID;
    if (category === CAT_BADGE_FRAME) return DEFAULT_BADGE_FRAME_ID;
    if (category === CAT_BADGE_PLATE) return DEFAULT_BADGE_PLATE_ID;

    return 0;
}

function isCosmeticIdInCategory(cosmeticId, category) {
    var realCategory = getCategoryFromCosmeticId(cosmeticId);
    var safeCategory = sanitizeCosmeticCategory(category);

    return realCategory !== "" && realCategory === safeCategory;
}

function sanitizeHairColor(value) {
    var v = toInt(value, DEFAULT_HAIR_COLOR);

    if (v < 0) return 0;
    if (v > 4) return 4;

    return v;
}

function sanitizeCampaignProgress(v) {
    return Math.max(0, Math.min(999, toInt(v, 0)));
}

function mergeCampaignProgress(currentValue, incomingValue, allowRegress) {
    var current = sanitizeCampaignProgress(currentValue);
    var incoming = sanitizeCampaignProgress(incomingValue);

    if (allowRegress === true) {
        return incoming;
    }

    return Math.max(current, incoming);
}

// =====================================================
// ECONOMIA
// =====================================================

function mergeRules(base, over) {
    if (!over) return base;

    var keys = [
        "GrantOnWinCO",
        "DozerDropCostCO",
        "WelcomeBonusCO",
        "MaxGrantPerCall"
    ];

    for (var i = 0; i < keys.length; i++) {
        var k = keys[i];

        if (over[k] != null) {
            base[k] = toInt(over[k], base[k]);
        }
    }

    return base;
}

function readRules() {
    var rules = {
        GrantOnWinCO: 20,
        DozerDropCostCO: 1,
        WelcomeBonusCO: 0,
        MaxGrantPerCall: 100
    };

    var loaded = readTitleDataJson("CO_RULES", null);

    if (loaded && typeof loaded === "object") {
        rules = mergeRules(rules, loaded);
    }

    return rules;
}

function getInventory() {
    return server.GetUserInventory({
        PlayFabId: currentPlayerId
    });
}

function getBalanceCO() {
    var inv = getInventory();
    return (inv.VirtualCurrency && inv.VirtualCurrency.CO) || 0;
}

function getBalanceGE() {
    var inv = getInventory();
    return (inv.VirtualCurrency && inv.VirtualCurrency.GE) || 0;
}

function getBalances() {
    var inv = getInventory();

    return {
        CO: (inv.VirtualCurrency && inv.VirtualCurrency.CO) || 0,
        GE: (inv.VirtualCurrency && inv.VirtualCurrency.GE) || 0
    };
}

function spendCurrencyByPriceType(priceType, priceCo, priceGe) {
    priceType = asString(priceType, "").trim().toLowerCase();

    if (priceType === PRICE_FREE) {
        return { ok: true };
    }

    if (priceType === PRICE_CO) {
        var costCo = Math.max(0, toInt(priceCo, 0));

        if (costCo <= 0) {
            return { ok: false, error: "INVALID_PRICE_CO" };
        }

        var balCo = getBalanceCO();

        if (balCo < costCo) {
            return {
                ok: false,
                error: "NOT_ENOUGH_CO",
                needed: costCo,
                current: balCo
            };
        }

        server.SubtractUserVirtualCurrency({
            PlayFabId: currentPlayerId,
            VirtualCurrency: "CO",
            Amount: costCo
        });

        return { ok: true };
    }

    if (priceType === PRICE_GE) {
        var costGe = Math.max(0, toInt(priceGe, 0));

        if (costGe <= 0) {
            return { ok: false, error: "INVALID_PRICE_GE" };
        }

        var balGe = getBalanceGE();

        if (balGe < costGe) {
            return {
                ok: false,
                error: "NOT_ENOUGH_GE",
                needed: costGe,
                current: balGe
            };
        }

        server.SubtractUserVirtualCurrency({
            PlayFabId: currentPlayerId,
            VirtualCurrency: "GE",
            Amount: costGe
        });

        return { ok: true };
    }

    if (priceType === PRICE_AD) {
        return { ok: false, error: "AD_PRICE_NOT_SERVER_GRANTED" };
    }

    if (priceType === PRICE_REAL) {
        return { ok: false, error: "REAL_PRICE_NOT_SERVER_GRANTED" };
    }

    return { ok: false, error: "INVALID_PRICE_TYPE" };
}

// =====================================================
// CATALOGHI SERVER-SIDE
// =====================================================

function readShopCatalog() {
    var parsed = readTitleDataJson("SHOP_CATALOG", {});

    if (parsed && typeof parsed === "object") {
        return parsed;
    }

    return {};
}

function readCosmeticCatalog() {
    var parsed = readTitleDataJson("COSMETIC_CATALOG", {});

    if (parsed && typeof parsed === "object") {
        return parsed;
    }

    return {};
}

function readQuestCatalog() {
    var parsed = readTitleDataJson("QUEST_CATALOG", {});

    if (parsed && typeof parsed === "object") {
        return parsed;
    }

    return {};
}

function readChestCatalog() {
    var parsed = readTitleDataJson("CHEST_CATALOG", {});

    if (parsed && typeof parsed === "object") {
        return parsed;
    }

    return {};
}

function getCosmeticDefById(cosmeticId) {
    var catalog = readCosmeticCatalog();
    var key = asString(cosmeticId, "").trim();

    if (!key) return null;
    if (!catalog[key]) return null;

    return clone(catalog[key]);
}

function cosmeticExistsInCatalog(cosmeticId) {
    return getCosmeticDefById(cosmeticId) != null;
}

function getCosmeticCategoryFromCatalog(cosmeticId) {
    var def = getCosmeticDefById(cosmeticId);

    if (!def) {
        return getCategoryFromCosmeticId(cosmeticId);
    }

    var category = sanitizeCosmeticCategory(def.category);

    if (category !== "") {
        return category;
    }

    return getCategoryFromCosmeticId(cosmeticId);
}

function getCosmeticGroupFromCatalog(cosmeticId) {
    var def = getCosmeticDefById(cosmeticId);

    if (!def) return "";

    return asString(def.group, "").trim().toLowerCase();
}

function getChestDefById(defId) {
    var catalog = readChestCatalog();

    defId = asString(defId, "").trim().toLowerCase();

    if (!defId) return null;
    if (!catalog[defId]) return null;

    return clone(catalog[defId]);
}
// =====================================================
// DEFAULT PLAYER STATE
// =====================================================

function defaultPlayerProfile() {
    return {
        display_name: "",
        starter_character_id: "",
        rank_rating: 0,
        language: "en",
        campaign_progress: CP_NEED_NAME
    };
}

function defaultCosmetics() {
    return {
        equipped: {
            human: DEFAULT_HUMAN_ID,
            hair: DEFAULT_HAIR_ID,
            hair_color: DEFAULT_HAIR_COLOR,
            hat: DEFAULT_HAT_ID,
            jockey: DEFAULT_JOCKEY_ID,
            mount: DEFAULT_MOUNT_ID,
            ball: DEFAULT_BALL_ID,
            badge_bg: DEFAULT_BADGE_BG_ID,
            badge_frame: DEFAULT_BADGE_FRAME_ID,
            badge_plate: DEFAULT_BADGE_PLATE_ID
        }
    };
}

function defaultUnlocksState() {
    return {
        ids: []
    };
}

function defaultQuestsState() {
    return {
        data: {},
        last_daily_reset: ""
    };
}

function defaultChestsState() {
    return {
        slots: [],
        queue_slot_indexes: [],
        rotation_index: 0,
        last_server_time: nowUnix()
    };
}

function defaultShopState() {
    return {
        daily_key: "",
        daily_item_ids: [],
        claimed_today: {}
    };
}

function defaultJourneyState() {
    return {
        total_steps: 0,
        updated_at: nowUnix()
    };
}

function defaultMetaState() {
    return {
        profile_version: 3,
        last_sync_unix: nowUnix()
    };
}

// =====================================================
// SANITIZE PLAYER STATE BASE
// =====================================================

function sanitizePlayerProfile(p) {
    var d = defaultPlayerProfile();
    if (!p) return d;

    d.display_name = asString(p.display_name, "").trim();
    d.starter_character_id = asString(p.starter_character_id, "").trim().toLowerCase();
    d.rank_rating = Math.max(0, toInt(p.rank_rating, 0));

    d.language = asString(p.language, "en").trim().toLowerCase();
    if (d.language !== "it" && d.language !== "en") {
        d.language = "en";
    }

    d.campaign_progress = sanitizeCampaignProgress(p.campaign_progress);

    return d;
}

function sanitizeEquippedCosmeticForCategory(requestedId, category, defaultId) {
    category = sanitizeCosmeticCategory(category);

    var id = toInt(requestedId, defaultId);

    if (!isCosmeticIdInCategory(id, category)) {
        return defaultId;
    }

    var realCategory = getCosmeticCategoryFromCatalog(id);

    if (realCategory !== "" && realCategory !== category) {
        return defaultId;
    }

    return id;
}

function sanitizeCosmetics(c) {
    var d = defaultCosmetics();
    if (!c) return d;

    var eq = c.equipped || {};

    d.equipped.human = sanitizeEquippedCosmeticForCategory(
        eq.human,
        CAT_HUMAN,
        DEFAULT_HUMAN_ID
    );

    d.equipped.hair = sanitizeEquippedCosmeticForCategory(
        eq.hair,
        CAT_HAIR,
        DEFAULT_HAIR_ID
    );

    d.equipped.hair_color = sanitizeHairColor(eq.hair_color);

    d.equipped.hat = sanitizeEquippedCosmeticForCategory(
        eq.hat,
        CAT_HAT,
        DEFAULT_HAT_ID
    );

    d.equipped.jockey = sanitizeEquippedCosmeticForCategory(
        eq.jockey,
        CAT_JOCKEY,
        DEFAULT_JOCKEY_ID
    );

    d.equipped.mount = sanitizeEquippedCosmeticForCategory(
        eq.mount,
        CAT_MOUNT,
        DEFAULT_MOUNT_ID
    );

    d.equipped.ball = sanitizeEquippedCosmeticForCategory(
        eq.ball,
        CAT_BALL,
        DEFAULT_BALL_ID
    );

    d.equipped.badge_bg = sanitizeEquippedCosmeticForCategory(
        eq.badge_bg,
        CAT_BADGE_BG,
        DEFAULT_BADGE_BG_ID
    );

    d.equipped.badge_frame = sanitizeEquippedCosmeticForCategory(
        eq.badge_frame,
        CAT_BADGE_FRAME,
        DEFAULT_BADGE_FRAME_ID
    );

    d.equipped.badge_plate = sanitizeEquippedCosmeticForCategory(
        eq.badge_plate,
        CAT_BADGE_PLATE,
        DEFAULT_BADGE_PLATE_ID
    );

    return d;
}

function sanitizeUnlocksState(u) {
    var d = defaultUnlocksState();
    if (!u || typeof u !== "object") return d;

    d.ids = sanitizeUnlockIdArray((u.ids instanceof Array) ? u.ids : []);
    return d;
}

function hasUnlockId(unlocksState, unlockId) {
    unlocksState = sanitizeUnlocksState(unlocksState);
    unlockId = asString(unlockId, "").trim().toLowerCase();

    if (!unlockId) return false;

    return unlocksState.ids.indexOf(unlockId) !== -1;
}

function hasAllUnlockIds(unlocksState, unlockIds) {
    unlocksState = sanitizeUnlocksState(unlocksState);
    unlockIds = sanitizeUnlockIdArray((unlockIds instanceof Array) ? unlockIds : []);

    if (unlockIds.length <= 0) return false;

    for (var i = 0; i < unlockIds.length; i++) {
        if (!hasUnlockId(unlocksState, unlockIds[i])) {
            return false;
        }
    }

    return true;
}

function grantUnlockIdsToState(unlocksState, unlockIds) {
    unlocksState = sanitizeUnlocksState(unlocksState);
    unlockIds = sanitizeUnlockIdArray((unlockIds instanceof Array) ? unlockIds : []);

    var changed = false;

    for (var i = 0; i < unlockIds.length; i++) {
        var uid = unlockIds[i];

        if (unlocksState.ids.indexOf(uid) !== -1) continue;

        unlocksState.ids.push(uid);
        changed = true;
    }

    unlocksState.ids = sanitizeUnlockIdArray(unlocksState.ids);

    return {
        changed: changed,
        unlocks_state: unlocksState
    };
}

function sanitizeJourneyState(s) {
    var d = defaultJourneyState();
    if (!s) return d;

    d.total_steps = Math.max(0, toInt(s.total_steps, 0));
    d.updated_at = nowUnix();

    return d;
}

function applyJourneyStepsProgress(journeyState, amount) {
    journeyState = sanitizeJourneyState(journeyState);
    amount = Math.max(0, toInt(amount, 0));

    var current = Math.max(0, toInt(journeyState.total_steps, 0));
    var next = current + amount;

    journeyState.total_steps = next;
    journeyState.updated_at = nowUnix();

    return {
        journey_state: journeyState
    };
}

function sanitizeMetaState(m) {
    var d = defaultMetaState();
    if (!m) return d;

    d.profile_version = Math.max(3, toInt(m.profile_version, 3));
    d.last_sync_unix = nowUnix();

    return d;
}

// =====================================================
// QUESTS SANITIZE / HELPERS
// =====================================================

function sanitizeQuestType(v) {
    var s = asString(v, "progress").trim().toLowerCase();

    if (s === "daily") return "daily";

    return "progress";
}

function sanitizeQuestDef(def) {
    if (!def || typeof def !== "object") return null;

    var id = asString(def.id, "").trim();
    if (!id) return null;

    var rewardChestId = asString(def.reward_chest_id, "none").trim().toLowerCase();

    if (
        rewardChestId !== "none" &&
        rewardChestId !== "wood" &&
        rewardChestId !== "silver" &&
        rewardChestId !== "gold" &&
        rewardChestId !== "magic" &&
        rewardChestId !== "giant"
    ) {
        rewardChestId = "none";
    }

    return {
        id: id,
        type: sanitizeQuestType(def.type),
        title: asString(def.title, "").trim(),
        description: asString(def.description, "").trim(),
        event_key: asString(def.event_key, "").trim(),
        target: Math.max(1, toInt(def.target, 1)),
        reward_co: Math.max(0, toInt(def.reward_co, 0)),
        reward_ge: Math.max(0, toInt(def.reward_ge, 0)),
        reward_cosmetic_id: Math.max(0, toInt(def.reward_cosmetic_id, 0)),
        reward_chest_id: rewardChestId,
        unlock_ids: sanitizeUnlockIdArray((def.unlock_ids instanceof Array) ? def.unlock_ids : []),
        icon: asString(def.icon, "").trim()
    };
}

function sanitizeQuestRuntimeState(def, saved) {
    var q = {
        id: def.id,
        type: def.type,
        title: def.title,
        description: def.description,
        event_key: def.event_key,
        target: Math.max(1, toInt(def.target, 1)),
        reward_co: Math.max(0, toInt(def.reward_co, 0)),
        reward_ge: Math.max(0, toInt(def.reward_ge, 0)),
        reward_cosmetic_id: Math.max(0, toInt(def.reward_cosmetic_id, 0)),
        reward_chest_id: asString(def.reward_chest_id, "none").trim().toLowerCase(),
        unlock_ids: sanitizeUnlockIdArray((def.unlock_ids instanceof Array) ? def.unlock_ids : []),
        icon: def.icon,
        progress: 0,
        completed: false,
        claimed: false
    };

    if (saved && typeof saved === "object") {
        q.progress = Math.max(0, toInt(saved.progress, 0));
        q.completed = toBool(saved.completed, false);
        q.claimed = toBool(saved.claimed, false);
    }

    if (q.progress >= q.target) {
        q.progress = q.target;
        q.completed = true;
    }

    return q;
}

function buildQuestStateFromCatalog(savedData, savedResetKey) {
    var catalog = readQuestCatalog();
    var data = {};
    var today = todayKeyUtc();

    savedData = savedData || {};
    savedResetKey = asString(savedResetKey, "").trim();

    for (var questId in catalog) {
        if (!catalog.hasOwnProperty(questId)) continue;

        var def = sanitizeQuestDef(catalog[questId]);
        if (!def) continue;

        var saved = savedData[def.id];
        var q = sanitizeQuestRuntimeState(def, saved);

        if (def.type === "daily" && savedResetKey !== today) {
            q.progress = 0;
            q.completed = false;
            q.claimed = false;
        }

        data[def.id] = q;
    }

    return {
        data: data,
        last_daily_reset: today
    };
}

function sanitizeQuestsState(q) {
    var savedData = (q && q.data && typeof q.data === "object") ? q.data : {};
    var savedReset = (q && q.last_daily_reset != null) ? q.last_daily_reset : "";

    return buildQuestStateFromCatalog(savedData, savedReset);
}

function addQuestProgressByEvent(questsState, eventKey, amount) {
    questsState = sanitizeQuestsState(questsState);

    eventKey = asString(eventKey, "").trim();
    amount = Math.max(0, toInt(amount, 0));

    if (!eventKey || amount <= 0) {
        return {
            changed: false,
            completed_now: [],
            quests_state: questsState
        };
    }

    var completedNow = [];
    var changed = false;

    for (var questId in questsState.data) {
        if (!questsState.data.hasOwnProperty(questId)) continue;

        var q = questsState.data[questId];
        if (!q) continue;
        if (asString(q.event_key, "").trim() !== eventKey) continue;
        if (toBool(q.claimed, false)) continue;
        if (toBool(q.completed, false)) continue;

        var oldCompleted = toBool(q.completed, false);
        var target = Math.max(1, toInt(q.target, 1));
        var newProgress = Math.max(0, toInt(q.progress, 0)) + amount;

        q.progress = Math.min(newProgress, target);

        if (q.progress >= target) {
            q.progress = target;
            q.completed = true;
        }

        questsState.data[questId] = q;
        changed = true;

        if (!oldCompleted && q.completed) {
            completedNow.push(questId);
        }
    }

    return {
        changed: changed,
        completed_now: completedNow,
        quests_state: questsState
    };
}

// =====================================================
// CHESTS SANITIZE / ROTATION
// =====================================================

function sanitizeChestStatus(v) {
    var s = asString(v, CHEST_STATUS_IDLE).trim().toLowerCase();

    if (s === CHEST_STATUS_IDLE) return CHEST_STATUS_IDLE;
    if (s === CHEST_STATUS_QUEUED) return CHEST_STATUS_QUEUED;
    if (s === CHEST_STATUS_OPENING) return CHEST_STATUS_OPENING;
    if (s === CHEST_STATUS_READY) return CHEST_STATUS_READY;

    return CHEST_STATUS_IDLE;
}

function sanitizeChestsState(c) {
    var d = defaultChestsState();
    if (!c) return d;

    var rawSlots = (c.slots instanceof Array) ? c.slots : [];
    d.slots = [];

    for (var i = 0; i < MAX_CHEST_SLOTS; i++) {
        var s = rawSlots[i];

        if (!s || typeof s !== "object") {
            d.slots.push({});
            continue;
        }

        var sid = asString(s.id, "").trim();
        var sdef = asString(s.def_id, "").trim();

        if (!sid && !sdef) {
            d.slots.push({});
            continue;
        }

        d.slots.push({
            id: sid,
            def_id: sdef,
            title: asString(s.title, "").trim(),
            description: asString(s.description, "").trim(),
            duration_sec: Math.max(0, toInt(s.duration_sec, 0)),
            status: sanitizeChestStatus(s.status),
            created_ts: Math.max(0, toInt(s.created_ts, 0)),
            start_ts: Math.max(0, toInt(s.start_ts, 0)),
            end_ts: Math.max(0, toInt(s.end_ts, 0)),
            reward_co_min: Math.max(0, toInt(s.reward_co_min, 0)),
            reward_co_max: Math.max(0, toInt(s.reward_co_max, 0)),
            reward_ge_min: Math.max(0, toInt(s.reward_ge_min, 0)),
            reward_ge_max: Math.max(0, toInt(s.reward_ge_max, 0)),
            cosmetic_drop_chance: Math.max(0, toFloat(s.cosmetic_drop_chance, 0)),
            possible_cosmetic_ids: (s.possible_cosmetic_ids instanceof Array) ? clone(s.possible_cosmetic_ids) : [],
            duplicate_cosmetic_co: Math.max(0, toInt(s.duplicate_cosmetic_co, 0))
        });
    }

    d.queue_slot_indexes = [];

    if (c.queue_slot_indexes instanceof Array) {
        for (var j = 0; j < c.queue_slot_indexes.length; j++) {
            var idx = toInt(c.queue_slot_indexes[j], -1);

            if (
                idx >= 0 &&
                idx < MAX_CHEST_SLOTS &&
                d.queue_slot_indexes.indexOf(idx) === -1
            ) {
                var queuedSlot = d.slots[idx];

                if (
                    !isEmptyChestSlot(queuedSlot) &&
                    sanitizeChestStatus(queuedSlot.status) === CHEST_STATUS_QUEUED
                ) {
                    d.queue_slot_indexes.push(idx);
                }
            }
        }
    }

    if (d.queue_slot_indexes.length > MAX_CHEST_QUEUE) {
        d.queue_slot_indexes = d.queue_slot_indexes.slice(0, MAX_CHEST_QUEUE);
    }

    d.rotation_index = Math.max(0, toInt(c.rotation_index, 0));
    d.last_server_time = nowUnix();

    while (d.slots.length < MAX_CHEST_SLOTS) {
        d.slots.push({});
    }

    return d;
}

var CHEST_ROTATION_PATTERN = [
    "silver", "silver", "silver", "gold", "silver", "silver", "gold", "silver", "silver", "silver", "silver", "magic",
    "silver", "silver", "gold", "silver", "silver", "silver", "gold", "silver", "silver", "silver", "silver", "gold",
    "silver", "gold", "silver", "silver", "silver", "silver", "gold", "silver", "silver", "silver", "silver", "gold",
    "silver", "silver", "gold", "silver", "silver", "silver", "silver", "magic", "silver", "silver", "gold", "silver",
    "silver", "silver", "silver", "gold", "silver", "silver", "gold", "silver", "silver", "silver", "silver", "giant"
];

function getNextRotatingChestDefId(rotationIndex) {
    if (!(CHEST_ROTATION_PATTERN instanceof Array) || CHEST_ROTATION_PATTERN.length <= 0) {
        return "";
    }

    rotationIndex = Math.max(0, toInt(rotationIndex, 0));
    rotationIndex = rotationIndex % CHEST_ROTATION_PATTERN.length;

    return asString(CHEST_ROTATION_PATTERN[rotationIndex], "").trim().toLowerCase();
}
// =====================================================
// QUEST REWARD HELPER
// =====================================================

function claimQuestReward(questsState, cosmetics, unlocksState, questId) {
    questsState = sanitizeQuestsState(questsState);
    cosmetics = sanitizeCosmetics(cosmetics);
    unlocksState = sanitizeUnlocksState(unlocksState);
    questId = asString(questId, "").trim();

    if (!questId) return { ok: false, error: "INVALID_QUEST_ID", quests_state: questsState, cosmetics: cosmetics, unlocks_state: unlocksState };

    var q = questsState.data[questId];
    if (!q) return { ok: false, error: "QUEST_NOT_FOUND", quests_state: questsState, cosmetics: cosmetics, unlocks_state: unlocksState };
    if (!toBool(q.completed, false)) return { ok: false, error: "QUEST_NOT_COMPLETED", quests_state: questsState, cosmetics: cosmetics, unlocks_state: unlocksState };
    if (toBool(q.claimed, false)) return { ok: false, error: "QUEST_ALREADY_CLAIMED", quests_state: questsState, cosmetics: cosmetics, unlocks_state: unlocksState };

    q.claimed = true;
    questsState.data[questId] = q;

    var rewardCo = Math.max(0, toInt(q.reward_co, 0));
    var rewardGe = Math.max(0, toInt(q.reward_ge, 0));
    var rewardCosmeticId = Math.max(0, toInt(q.reward_cosmetic_id, 0));
    var rewardChestId = asString(q.reward_chest_id, "none").trim().toLowerCase();
    var rewardUnlockIds = sanitizeUnlockIdArray((q.unlock_ids instanceof Array) ? q.unlock_ids : []);
    var openedChestRewards = null;

    if (rewardCo > 0) {
        server.AddUserVirtualCurrency({ PlayFabId: currentPlayerId, VirtualCurrency: "CO", Amount: rewardCo });
    }

    if (rewardGe > 0) {
        server.AddUserVirtualCurrency({ PlayFabId: currentPlayerId, VirtualCurrency: "GE", Amount: rewardGe });
    }

    if (rewardCosmeticId > 0 && cosmeticExistsInCatalog(rewardCosmeticId)) {
        cosmetics.equipped[getCosmeticCategoryFromCatalog(rewardCosmeticId)] = rewardCosmeticId;
        cosmetics = sanitizeCosmetics(cosmetics);
    }

    if (rewardChestId && rewardChestId !== "none") {
        var questChestDef = getChestDefById(rewardChestId);
        if (questChestDef) {
            var virtualChest = makeChestDataFromDef(questChestDef);
            openedChestRewards = generateChestRewards(virtualChest, cosmetics);

            if (openedChestRewards.co > 0) {
                server.AddUserVirtualCurrency({ PlayFabId: currentPlayerId, VirtualCurrency: "CO", Amount: openedChestRewards.co });
            }

            if (openedChestRewards.ge > 0) {
                server.AddUserVirtualCurrency({ PlayFabId: currentPlayerId, VirtualCurrency: "GE", Amount: openedChestRewards.ge });
            }

            if (openedChestRewards.cosmetic_id > 0 && cosmeticExistsInCatalog(openedChestRewards.cosmetic_id)) {
                cosmetics.equipped[getCosmeticCategoryFromCatalog(openedChestRewards.cosmetic_id)] = openedChestRewards.cosmetic_id;
                cosmetics = sanitizeCosmetics(cosmetics);
            }
        }
    }

    if (rewardUnlockIds.length > 0) {
        var grantRes = grantUnlockIdsToState(unlocksState, rewardUnlockIds);
        unlocksState = sanitizeUnlocksState(grantRes.unlocks_state);
    }

    return {
        ok: true,
        quest_id: questId,
        reward_co: rewardCo,
        reward_ge: rewardGe,
        reward_cosmetic_id: rewardCosmeticId,
        reward_chest_id: rewardChestId,
        unlock_ids: rewardUnlockIds,
        opened_chest_rewards: openedChestRewards,
        quests_state: questsState,
        cosmetics: cosmetics,
        unlocks_state: unlocksState
    };
}

// =====================================================
// CHEST HELPERS
// =====================================================

function isEmptyChestSlot(slot) {
    if (!slot || typeof slot !== "object") return true;

    var sid = asString(slot.id, "").trim();
    var sdef = asString(slot.def_id, "").trim();

    return !sid && !sdef;
}

function findFreeChestSlot(chestsState) {
    if (!chestsState || !(chestsState.slots instanceof Array)) return -1;

    for (var i = 0; i < MAX_CHEST_SLOTS; i++) {
        if (isEmptyChestSlot(chestsState.slots[i])) return i;
    }

    return -1;
}

function getOpeningChestSlotIndex(chestsState) {
    if (!chestsState || !(chestsState.slots instanceof Array)) return -1;

    for (var i = 0; i < MAX_CHEST_SLOTS; i++) {
        var slot = chestsState.slots[i];

        if (!isEmptyChestSlot(slot) && sanitizeChestStatus(slot.status) === CHEST_STATUS_OPENING) {
            return i;
        }
    }

    return -1;
}

function makeChestDataFromDef(chestDef) {
    var now = nowUnix();

    return {
        id: asString(chestDef.id, "chest").trim() + "_" + now + "_" + Math.floor(Math.random() * 100000),
        def_id: asString(chestDef.id, "").trim().toLowerCase(),
        title: asString(chestDef.title, "").trim(),
        description: asString(chestDef.description, "").trim(),
        duration_sec: Math.max(0, toInt(chestDef.duration_sec, 0)),
        status: CHEST_STATUS_IDLE,
        created_ts: now,
        start_ts: 0,
        end_ts: 0,
        reward_co_min: Math.max(0, toInt(chestDef.reward_co_min, 0)),
        reward_co_max: Math.max(0, toInt(chestDef.reward_co_max, 0)),
        reward_ge_min: Math.max(0, toInt(chestDef.reward_ge_min, 0)),
        reward_ge_max: Math.max(0, toInt(chestDef.reward_ge_max, 0)),
        cosmetic_drop_chance: Math.max(0, toFloat(chestDef.cosmetic_drop_chance, 0)),
        possible_cosmetic_ids: (chestDef.possible_cosmetic_ids instanceof Array) ? clone(chestDef.possible_cosmetic_ids) : [],
        duplicate_cosmetic_co: Math.max(0, toInt(chestDef.duplicate_cosmetic_co, 0))
    };
}

function addChestToState(chestsState, chestDef) {
    chestsState = sanitizeChestsState(chestsState);

    var freeSlot = findFreeChestSlot(chestsState);
    if (freeSlot === -1) {
        return { ok: false, error: "CHEST_SLOTS_FULL", chests_state: chestsState };
    }

    chestsState.slots[freeSlot] = makeChestDataFromDef(chestDef);
    chestsState.last_server_time = nowUnix();

    return {
        ok: true,
        slot_index: freeSlot,
        chests_state: chestsState
    };
}

function rollRange(minV, maxV) {
    minV = toInt(minV, 0);
    maxV = toInt(maxV, minV);

    if (maxV < minV) maxV = minV;

    return minV + Math.floor(Math.random() * (maxV - minV + 1));
}

function generateChestRewards(chest, cosmetics) {
    var rewards = {
        co: rollRange(chest.reward_co_min, chest.reward_co_max),
        ge: rollRange(chest.reward_ge_min, chest.reward_ge_max),
        cosmetic_id: 0,
        cosmetic_unlocked: false,
        duplicate_compensation_co: 0,
        title: asString(chest.title, ""),
        def_id: asString(chest.def_id, "")
    };

    var dropChance = Math.max(0, toFloat(chest.cosmetic_drop_chance, 0));
    var possible = (chest.possible_cosmetic_ids instanceof Array) ? chest.possible_cosmetic_ids : [];

    if (dropChance > 0 && possible.length > 0 && Math.random() <= dropChance) {
        var picked = toInt(possible[Math.floor(Math.random() * possible.length)], 0);

        if (picked > 0 && cosmeticExistsInCatalog(picked)) {
            rewards.cosmetic_id = picked;
            rewards.cosmetic_unlocked = true;
        }
    }

    return rewards;
}

function removeSlotIndexFromQueue(queueArr, slotIndex) {
    var out = [];
    if (!(queueArr instanceof Array)) return out;

    for (var i = 0; i < queueArr.length; i++) {
        var v = toInt(queueArr[i], -1);
        if (v !== slotIndex) out.push(v);
    }

    return out;
}

function isSlotIndexQueued(chestsState, slotIndex) {
    if (!chestsState || !(chestsState.queue_slot_indexes instanceof Array)) return false;
    return chestsState.queue_slot_indexes.indexOf(slotIndex) !== -1;
}

function startOpeningForSlot(chestsState, slotIndex) {
    chestsState = sanitizeChestsState(chestsState);

    if (slotIndex < 0 || slotIndex >= MAX_CHEST_SLOTS) return chestsState;

    var slot = chestsState.slots[slotIndex];
    if (isEmptyChestSlot(slot)) return chestsState;

    var now = nowUnix();

    slot.status = CHEST_STATUS_OPENING;
    slot.start_ts = now;
    slot.end_ts = now + Math.max(0, toInt(slot.duration_sec, 0));

    chestsState.slots[slotIndex] = slot;
    chestsState.last_server_time = now;

    return chestsState;
}

function markReadyForSlot(chestsState, slotIndex) {
    chestsState = sanitizeChestsState(chestsState);

    if (slotIndex < 0 || slotIndex >= MAX_CHEST_SLOTS) return chestsState;

    var slot = chestsState.slots[slotIndex];
    if (isEmptyChestSlot(slot)) return chestsState;

    slot.status = CHEST_STATUS_READY;
    slot.end_ts = 0;

    chestsState.slots[slotIndex] = slot;
    chestsState.last_server_time = nowUnix();

    return chestsState;
}

function promoteQueueIfNeeded(chestsState) {
    chestsState = sanitizeChestsState(chestsState);

    if (getOpeningChestSlotIndex(chestsState) !== -1) return chestsState;

    if (!(chestsState.queue_slot_indexes instanceof Array) || chestsState.queue_slot_indexes.length === 0) {
        return chestsState;
    }

    for (var i = 0; i < chestsState.queue_slot_indexes.length; i++) {
        var slotIndex = toInt(chestsState.queue_slot_indexes[i], -1);

        if (slotIndex < 0 || slotIndex >= MAX_CHEST_SLOTS) continue;

        var slot = chestsState.slots[slotIndex];

        if (!isEmptyChestSlot(slot) && sanitizeChestStatus(slot.status) === CHEST_STATUS_QUEUED) {
            return startOpeningForSlot(chestsState, slotIndex);
        }
    }

    return chestsState;
}

function refreshChestsStateByTime(chestsState) {
    chestsState = sanitizeChestsState(chestsState);

    var now = nowUnix();

    for (var i = 0; i < MAX_CHEST_SLOTS; i++) {
        var slot = chestsState.slots[i];
        if (isEmptyChestSlot(slot)) continue;

        if (sanitizeChestStatus(slot.status) === CHEST_STATUS_OPENING) {
            var endTs = Math.max(0, toInt(slot.end_ts, 0));

            if (endTs > 0 && now >= endTs) {
                slot.status = CHEST_STATUS_READY;
                slot.end_ts = 0;
                chestsState.slots[i] = slot;
            }
        }
    }

    chestsState.last_server_time = now;
    chestsState = promoteQueueIfNeeded(chestsState);

    return sanitizeChestsState(chestsState);
}

function getRemainingSecondsForSlot(chestsState, slotIndex) {
    chestsState = sanitizeChestsState(chestsState);

    if (slotIndex < 0 || slotIndex >= MAX_CHEST_SLOTS) return 0;

    var slot = chestsState.slots[slotIndex];
    if (isEmptyChestSlot(slot)) return 0;
    if (sanitizeChestStatus(slot.status) !== CHEST_STATUS_OPENING) return 0;

    var endTs = Math.max(0, toInt(slot.end_ts, 0));
    var rem = endTs - nowUnix();

    return rem > 0 ? rem : 0;
}

function getChestFinishGemCost(chestsState, slotIndex) {
    var remaining = getRemainingSecondsForSlot(chestsState, slotIndex);
    if (remaining <= 0) return 1;

    var gemAccelSeconds = 20 * 60;
    return Math.max(1, Math.ceil(remaining / gemAccelSeconds));
}

function grantNextRotatingChestToState(chestsState) {
    chestsState = refreshChestsStateByTime(chestsState);

    var rotationIndex = Math.max(0, toInt(chestsState.rotation_index, 0));
    var defId = getNextRotatingChestDefId(rotationIndex);

    if (!defId) {
        return { ok: false, error: "ROTATION_DEF_NOT_FOUND", rotation_index: rotationIndex, chests_state: chestsState };
    }

    var chestDef = getChestDefById(defId);
    if (!chestDef) {
        return { ok: false, error: "CHEST_DEF_NOT_FOUND", def_id: defId, rotation_index: rotationIndex, chests_state: chestsState };
    }

    var addRes = addChestToState(chestsState, chestDef);

    if (!addRes.ok) {
        return {
            ok: false,
            error: addRes.error || "ADD_CHEST_FAILED",
            requested_def_id: defId,
            rotation_index: rotationIndex,
            chests_state: addRes.chests_state || chestsState
        };
    }

    chestsState = refreshChestsStateByTime(addRes.chests_state);
    chestsState.rotation_index = rotationIndex + 1;

    return {
        ok: true,
        slot_index: addRes.slot_index,
        granted_def_id: defId,
        previous_rotation_index: rotationIndex,
        rotation_index: chestsState.rotation_index,
        chests_state: chestsState
    };
}

// =====================================================
// SHOP SANITIZE / DAILY HELPERS
// =====================================================

function sanitizeShopState(s) {
    var d = defaultShopState();
    if (!s) return d;

    d.daily_key = asString(s.daily_key, "").trim();
    d.daily_item_ids = sanitizeLowerStringArray((s.daily_item_ids instanceof Array) ? s.daily_item_ids : []);
    d.claimed_today = (s.claimed_today && typeof s.claimed_today === "object") ? clone(s.claimed_today) : {};

    var normalizedClaimed = {};

    for (var k in d.claimed_today) {
        if (!d.claimed_today.hasOwnProperty(k)) continue;
        normalizedClaimed[asString(k, "").trim().toLowerCase()] = toBool(d.claimed_today[k], false);
    }

    d.claimed_today = normalizedClaimed;

    return d;
}

function isTruthy(v) {
    return v === true || v === "true" || v === 1 || v === "1";
}

function isDailySection(section) {
    section = asString(section, "").trim().toLowerCase();

    return section === "free_daily" || section === "ad_daily" || section === "daily";
}

function isDailyCatalogItem(item) {
    if (!item) return false;

    var purchaseRule = asString(item.purchase_rule, "").trim().toLowerCase();
    var section = asString(item.shop_section, "").trim().toLowerCase();
    var enabled = isTruthy(item.enabled_in_daily_shop);

    if (purchaseRule === RULE_DAILY_ONCE) return true;
    if (enabled) return true;
    if (isDailySection(section)) return true;

    return false;
}

function sanitizePurchaseRule(v) {
    var s = asString(v, RULE_REPEATABLE).trim().toLowerCase();

    if (s === RULE_DAILY_ONCE) return RULE_DAILY_ONCE;
    if (s === RULE_ACCOUNT_ONCE) return RULE_ACCOUNT_ONCE;

    return RULE_REPEATABLE;
}

function normalizeDailyRotationMode(item) {
    var mode = asString(item.rotation_mode, "").trim().toLowerCase();

    if (mode === DAILY_ROT_FIXED || mode === DAILY_ROT_DETERMINISTIC || mode === DAILY_ROT_RANDOM_WEIGHTED) {
        return mode;
    }

    if (isTruthy(item.enabled_in_rotation)) return DAILY_ROT_RANDOM_WEIGHTED;

    return DAILY_ROT_FIXED;
}

function getDailyItemOrder(item) {
    return toInt(item.daily_order, 999999);
}

function sortDailyItemIdsForDisplay(catalog, itemIds) {
    itemIds.sort(function (a, b) {
        var ia = catalog[a] || {};
        var ib = catalog[b] || {};
        var oa = getDailyItemOrder(ia);
        var ob = getDailyItemOrder(ib);

        if (oa !== ob) return oa - ob;

        a = asString(a, "");
        b = asString(b, "");

        if (a < b) return -1;
        if (a > b) return 1;

        return 0;
    });

    return itemIds;
}

function pickDeterministicCandidateId(slotKey, candidates, todayKey) {
    candidates = sortStringsAsc(candidates.slice(0));
    if (candidates.length <= 0) return "";

    var idx = hashString32(todayKey + "|" + slotKey + "|det") % candidates.length;
    return asString(candidates[idx], "");
}

function pickWeightedCandidateId(slotKey, candidateEntries, todayKey) {
    if (!(candidateEntries instanceof Array) || candidateEntries.length <= 0) return "";

    var total = 0;

    for (var i = 0; i < candidateEntries.length; i++) {
        total += Math.max(1, toInt(candidateEntries[i].weight, 1));
    }

    if (total <= 0) return asString(candidateEntries[0].item_id, "");

    var roll = seededUnitInterval(todayKey + "|" + slotKey + "|rand") * total;
    var cursor = 0;

    for (var j = 0; j < candidateEntries.length; j++) {
        var w = Math.max(1, toInt(candidateEntries[j].weight, 1));
        cursor += w;

        if (roll < cursor) return asString(candidateEntries[j].item_id, "");
    }

    return asString(candidateEntries[candidateEntries.length - 1].item_id, "");
}

function buildTodayDailyShopItemIds(catalog, todayKey) {
    var fixedIds = [];
    var grouped = {};

    if (!catalog || typeof catalog !== "object") return fixedIds;

    for (var itemId in catalog) {
        if (!catalog.hasOwnProperty(itemId)) continue;

        var item = catalog[itemId];
        if (!isDailyCatalogItem(item)) continue;

        var safeItemId = asString(itemId, "").trim().toLowerCase();
        if (!safeItemId) continue;

        var slotKey = asString(item.slot_key, "").trim();
        var mode = normalizeDailyRotationMode(item);

        if (mode === DAILY_ROT_FIXED || slotKey === "") {
            if (fixedIds.indexOf(safeItemId) === -1) fixedIds.push(safeItemId);
            continue;
        }

        if (!grouped[slotKey]) {
            grouped[slotKey] = { mode: mode, ids: [], weighted_entries: [] };
        }

        grouped[slotKey].ids.push(safeItemId);
        grouped[slotKey].weighted_entries.push({
            item_id: safeItemId,
            weight: Math.max(1, toInt(item.daily_weight, 1))
        });
    }

    var out = fixedIds.slice(0);

    for (var gk in grouped) {
        if (!grouped.hasOwnProperty(gk)) continue;

        var group = grouped[gk];
        var pickedId = "";

        if (group.mode === DAILY_ROT_DETERMINISTIC) {
            pickedId = pickDeterministicCandidateId(gk, group.ids, todayKey);
        } else {
            pickedId = pickWeightedCandidateId(gk, group.weighted_entries, todayKey);
        }

        if (pickedId && out.indexOf(pickedId) === -1) out.push(pickedId);
    }

    return sortDailyItemIdsForDisplay(catalog, out);
}

function ensureTodayDailyShop(shopState, catalog, todayKey) {
    shopState = sanitizeShopState(shopState);
    catalog = catalog || {};
    todayKey = asString(todayKey, todayKeyUtc()).trim();

    if (
        shopState.daily_key === todayKey &&
        shopState.daily_item_ids instanceof Array &&
        shopState.daily_item_ids.length > 0
    ) {
        return shopState;
    }

    shopState.daily_key = todayKey;
    shopState.daily_item_ids = buildTodayDailyShopItemIds(catalog, todayKey);
    shopState.claimed_today = {};

    return shopState;
}

function isItemInTodayShop(shopState, itemId) {
    shopState = sanitizeShopState(shopState);
    itemId = asString(itemId, "").trim().toLowerCase();

    if (!itemId) return false;

    return shopState.daily_item_ids.indexOf(itemId) !== -1;
}

function isShopItemClaimedToday(shopState, itemId) {
    shopState = sanitizeShopState(shopState);
    itemId = asString(itemId, "").trim().toLowerCase();

    if (!itemId) return false;

    return toBool(shopState.claimed_today[itemId], false);
}

function markShopItemClaimedToday(shopState, itemId) {
    shopState = sanitizeShopState(shopState);
    itemId = asString(itemId, "").trim().toLowerCase();

    if (!itemId) return shopState;

    if (!shopState.claimed_today || typeof shopState.claimed_today !== "object") {
        shopState.claimed_today = {};
    }

    shopState.claimed_today[itemId] = true;

    return shopState;
}

function isShopItemAlreadyPurchased(itemId) {
    itemId = asString(itemId, "").trim().toLowerCase();
    if (!itemId) return false;

    var key = makeSafeInternalKey("purchased_", itemId);

    try {
        var data = getInternalDataRaw([key]);
        return !!(data && data[key]);
    } catch (e) {
        return false;
    }
}

function markShopItemPurchased(itemId) {
    itemId = asString(itemId, "").trim().toLowerCase();
    if (!itemId) return;

    var key = makeSafeInternalKey("purchased_", itemId);
    var mark = {};
    mark[key] = String(nowUnix());

    updateInternalStateRaw(mark);
}

// =====================================================
// MAILBOX
// =====================================================

var MAILBOX_MESSAGES_KEY = "MAILBOX_MESSAGES";
var MAILBOX_READ_IDS_KEY = "MAILBOX_READ_IDS";
var MAILBOX_MAX_MESSAGES = 100;

function sanitizeMailboxReadIds(rawIds) {
    var out = [];
    if (!(rawIds instanceof Array)) return out;

    for (var i = 0; i < rawIds.length; i++) {
        var id = Math.max(0, toInt(rawIds[i], 0));
        if (id <= 0) continue;
        if (out.indexOf(id) !== -1) continue;
        out.push(id);
    }

    out.sort(function (a, b) { return a - b; });
    return out;
}

function sanitizeMailboxMessagesPayload(payload) {
    var out = [];
    if (!payload || typeof payload !== "object") return out;

    var rawMessages = payload.messages;
    if (!(rawMessages instanceof Array)) return out;

    var seen = {};
    for (var i = 0; i < rawMessages.length; i++) {
        if (out.length >= MAILBOX_MAX_MESSAGES) break;

        var raw = rawMessages[i];
        if (!raw || typeof raw !== "object") continue;
        if (!toBool(raw.enabled, false)) continue;

        var id = Math.max(0, toInt(raw.id, 0));
        if (id <= 0 || seen[id]) continue;

        seen[id] = true;

        var title = asString(raw.title, "").trim();
        var body = asString(raw.body, "").trim();

        out.push({
            id: id,
            title: title || "Messaggio",
            body: body,
            enabled: true
        });
    }

    return out;
}

function getMailboxMessages() {
    return sanitizeMailboxMessagesPayload(readTitleDataJson(MAILBOX_MESSAGES_KEY, { messages: [] }));
}

function getMailboxReadIds() {
    var data = getUserDataState([MAILBOX_READ_IDS_KEY]);
    var state = data[MAILBOX_READ_IDS_KEY] || {};
    return sanitizeMailboxReadIds(state.read_ids || []);
}

function mergeMailboxReadIds(a, b) {
    return sanitizeMailboxReadIds((a || []).concat(b || []));
}

function mailboxHasUnread(messages, readIds) {
    var read = {};
    for (var i = 0; i < readIds.length; i++) {
        read[readIds[i]] = true;
    }

    for (var j = 0; j < messages.length; j++) {
        var id = Math.max(0, toInt(messages[j].id, 0));
        if (id > 0 && !read[id]) return true;
    }

    return false;
}

function buildMailboxState() {
    var messages = getMailboxMessages();
    var readIds = getMailboxReadIds();

    return {
        messages: messages,
        read_ids: readIds,
        has_unread: mailboxHasUnread(messages, readIds)
    };
}

handlers.GetMailboxState = function (args, context) {
    var state = buildMailboxState();

    return {
        ok: true,
        mailbox_state: state,
        messages: state.messages,
        read_ids: state.read_ids,
        has_unread: state.has_unread,
        server_unix: nowUnix()
    };
};

handlers.SetMailboxReadIds = function (args, context) {
    var incoming = sanitizeMailboxReadIds(args && args.read_ids);
    var replace = toBool(args && args.replace, false);
    var readIds = replace ? incoming : mergeMailboxReadIds(getMailboxReadIds(), incoming);

    updateUserDataState({
        MAILBOX_READ_IDS: {
            read_ids: readIds
        }
    });

    var messages = getMailboxMessages();

    return {
        ok: true,
        mailbox_state: {
            messages: messages,
            read_ids: readIds,
            has_unread: mailboxHasUnread(messages, readIds)
        },
        read_ids: readIds,
        has_unread: mailboxHasUnread(messages, readIds),
        server_unix: nowUnix()
    };
};
// =====================================================
// PLAYER STATE
// =====================================================

handlers.BootstrapPlayerState = function (args, context) {
    var roKeys = [
        "cosmetics",
        "unlocks_state",
        "quests_state",
        "chests_state",
        "shop_state",
        "journey_state",
        "meta_state"
    ];

    var udKeys = ["player_profile"];

    var ro = getReadOnlyState(roKeys);
    var ud = getUserDataState(udKeys);

    var cosmetics = sanitizeCosmetics(ro.cosmetics);
    var unlocksState = sanitizeUnlocksState(ro.unlocks_state);
    var quests = sanitizeQuestsState(ro.quests_state);
    var chests = refreshChestsStateByTime(ro.chests_state);

    var shop = sanitizeShopState(ro.shop_state);
    var today = todayKeyUtc();
    var shopCatalog = readShopCatalog();
    shop = ensureTodayDailyShop(shop, shopCatalog, today);

    var journeyState = sanitizeJourneyState(ro.journey_state);
    var meta = sanitizeMetaState(ro.meta_state);
    var playerProfile = sanitizePlayerProfile(ud.player_profile);

    updateReadOnlyState({
        cosmetics: cosmetics,
        unlocks_state: unlocksState,
        quests_state: quests,
        chests_state: chests,
        shop_state: shop,
        journey_state: journeyState,
        meta_state: meta
    });

    updateUserDataState({
        player_profile: playerProfile
    });

    return {
        ok: true,
        server_unix: nowUnix(),
        today_key_utc: today,
        player_profile: playerProfile,
        cosmetics: cosmetics,
        unlocks_state: unlocksState,
        quests_state: quests,
        chests_state: chests,
        shop_state: shop,
        journey_state: journeyState,
        meta_state: meta,
        balances: getBalances()
    };
};

handlers.GetFullPlayerState = function (args, context) {
    return handlers.BootstrapPlayerState(args, context);
};

// =====================================================
// PLAYER PROFILE
// =====================================================

handlers.SetPlayerProfile = function (args, context) {
    var ud = getUserDataState(["player_profile"]);
    var current = sanitizePlayerProfile(ud.player_profile);

    var nextProfile = clone(current);
    var allowRegress = !!(args && args.allow_regress === true);

    if (args && args.display_name != null) {
        nextProfile.display_name = asString(args.display_name, "").trim();
    }

    if (args && args.starter_character_id != null) {
        nextProfile.starter_character_id = asString(args.starter_character_id, "").trim().toLowerCase();
    }

    var requestedRankRating = null;

    if (args && args.rank_rating != null) {
        requestedRankRating = args.rank_rating;
    } else if (args && args.ranked_rating != null) {
        requestedRankRating = args.ranked_rating;
    }

    if (requestedRankRating != null) {
        nextProfile.rank_rating = Math.max(0, toInt(requestedRankRating, current.rank_rating));
    }

    if (args && args.language != null) {
        var nextLang = asString(args.language, current.language).trim().toLowerCase();
        if (nextLang !== "it" && nextLang !== "en") nextLang = current.language || "en";
        nextProfile.language = nextLang;
    }

    if (args && args.campaign_progress != null) {
        nextProfile.campaign_progress = mergeCampaignProgress(
            current.campaign_progress,
            args.campaign_progress,
            allowRegress
        );
    }

    nextProfile = sanitizePlayerProfile(nextProfile);

    updateUserDataState({
        player_profile: nextProfile
    });

    return {
        ok: true,
        previous_campaign_progress: current.campaign_progress,
        player_profile: nextProfile,
        server_unix: nowUnix()
    };
};

// =====================================================
// STATE SETTERS
// =====================================================

handlers.SetCosmeticsState = function (args, context) {
    var ro = getReadOnlyState(["cosmetics"]);
    var current = sanitizeCosmetics(ro.cosmetics);
    var next = sanitizeCosmetics(args || current);

    updateReadOnlyState({ cosmetics: next });

    return {
        ok: true,
        cosmetics: next,
        server_unix: nowUnix()
    };
};

handlers.SetUnlocksState = function (args, context) {
    var ro = getReadOnlyState(["unlocks_state"]);
    var current = sanitizeUnlocksState(ro.unlocks_state);
    var next = sanitizeUnlocksState(args || current);

    updateReadOnlyState({ unlocks_state: next });

    return {
        ok: true,
        unlocks_state: next,
        server_unix: nowUnix()
    };
};

handlers.SetQuestsState = function (args, context) {
    var ro = getReadOnlyState(["quests_state"]);
    var current = sanitizeQuestsState(ro.quests_state);
    var next = sanitizeQuestsState(args || current);

    updateReadOnlyState({ quests_state: next });

    return {
        ok: true,
        quests_state: next,
        balances: getBalances(),
        server_unix: nowUnix(),
        today_key_utc: todayKeyUtc()
    };
};

handlers.SetChestsState = function (args, context) {
    var ro = getReadOnlyState(["chests_state"]);
    var current = sanitizeChestsState(ro.chests_state);
    var next = sanitizeChestsState(args || current);

    next = refreshChestsStateByTime(next);

    updateReadOnlyState({ chests_state: next });

    return {
        ok: true,
        chests_state: next,
        server_unix: nowUnix()
    };
};

handlers.SetShopState = function (args, context) {
    var ro = getReadOnlyState(["shop_state"]);
    var current = sanitizeShopState(ro.shop_state);
    var next = sanitizeShopState(args || current);

    var today = todayKeyUtc();
    var catalog = readShopCatalog();

    next = ensureTodayDailyShop(next, catalog, today);

    updateReadOnlyState({ shop_state: next });

    return {
        ok: true,
        shop_state: next,
        server_unix: nowUnix(),
        today_key_utc: today
    };
};

// =====================================================
// CAMPAIGN
// =====================================================

handlers.SetCampaignProgress = function (args, context) {
    var ud = getUserDataState(["player_profile"]);
    var current = sanitizePlayerProfile(ud.player_profile);

    var incoming = sanitizeCampaignProgress(args && args.campaign_progress);
    var allowRegress = !!(args && args.allow_regress === true);

    current.campaign_progress = mergeCampaignProgress(
        current.campaign_progress,
        incoming,
        allowRegress
    );

    current = sanitizePlayerProfile(current);

    updateUserDataState({ player_profile: current });

    return {
        ok: true,
        campaign_progress: current.campaign_progress,
        player_profile: current,
        server_unix: nowUnix()
    };
};

handlers.AdvanceCampaignProgress = function (args, context) {
    var ud = getUserDataState(["player_profile"]);
    var current = sanitizePlayerProfile(ud.player_profile);

    var target = sanitizeCampaignProgress(args && args.target_progress);
    current.campaign_progress = Math.max(current.campaign_progress, target);

    current = sanitizePlayerProfile(current);

    updateUserDataState({ player_profile: current });

    return {
        ok: true,
        campaign_progress: current.campaign_progress,
        player_profile: current,
        server_unix: nowUnix()
    };
};

// =====================================================
// QUESTS ACTIONS
// =====================================================

handlers.AddQuestProgress = function (args, context) {
    var eventKey = asString(args && args.event_key, "").trim();
    var amount = Math.max(0, toInt(args && args.amount, 0));

    if (!eventKey) return { ok: false, error: "INVALID_EVENT_KEY" };
    if (amount <= 0) return { ok: false, error: "INVALID_AMOUNT" };

    var ro = getReadOnlyState(["quests_state"]);
    var questsState = sanitizeQuestsState(ro.quests_state);
    var addRes = addQuestProgressByEvent(questsState, eventKey, amount);

    questsState = sanitizeQuestsState(addRes.quests_state);

    if (addRes.changed) {
        updateReadOnlyState({ quests_state: questsState });
    }

    return {
        ok: true,
        changed: addRes.changed,
        completed_now: addRes.completed_now,
        quests_state: questsState,
        balances: getBalances(),
        server_unix: nowUnix(),
        today_key_utc: todayKeyUtc()
    };
};

function addQuestCurrencyReward(currencyCode, amount, balances) {
    amount = Math.max(0, toInt(amount, 0));
    if (amount <= 0) return;

    var result = server.AddUserVirtualCurrency({
        PlayFabId: currentPlayerId,
        VirtualCurrency: currencyCode,
        Amount: amount
    });

    if (result && result.Balance != null) {
        balances[currencyCode] = Math.max(0, toInt(result.Balance, 0));
    }
}

function claimQuest(args, context) {
    var questId = asString(args && args.quest_id, "").trim().toLowerCase();
    if (!questId) return { ok: false, error: "INVALID_QUEST_ID" };

    var ro = getReadOnlyState(["quests_state", "cosmetics", "unlocks_state"]);
    var questsState = sanitizeQuestsState(ro.quests_state);
    var q = questsState.data[questId];

    if (!q) {
        return { ok: false, error: "QUEST_NOT_FOUND", quests_state: questsState };
    }
    if (!toBool(q.completed, false)) {
        return { ok: false, error: "QUEST_NOT_COMPLETED", quests_state: questsState };
    }
    if (toBool(q.claimed, false)) {
        return { ok: false, error: "QUEST_ALREADY_CLAIMED", quests_state: questsState };
    }

    var rewardCo = Math.max(0, toInt(q.reward_co, 0));
    var rewardGe = Math.max(0, toInt(q.reward_ge, 0));
    var rewardCosmeticId = Math.max(0, toInt(q.reward_cosmetic_id, 0));
    var rewardChestId = asString(q.reward_chest_id, "none").trim().toLowerCase();
    var rewardUnlockIds = sanitizeUnlockIdArray(
        (q.unlock_ids instanceof Array) ? q.unlock_ids : []
    );

    var isCurrencyOnly = rewardCosmeticId <= 0 &&
        (!rewardChestId || rewardChestId === "none") &&
        rewardUnlockIds.length <= 0;

    if (isCurrencyOnly) {
        q.claimed = true;
        questsState.data[questId] = q;

        var balances = {};
        addQuestCurrencyReward("CO", rewardCo, balances);
        addQuestCurrencyReward("GE", rewardGe, balances);

        updateReadOnlyState({ quests_state: questsState });

        return {
            ok: true,
            quest_id: questId,
            reward_co: rewardCo,
            reward_ge: rewardGe,
            reward_cosmetic_id: 0,
            reward_chest_id: "none",
            unlock_ids: [],
            opened_chest_rewards: null,
            quests_state: questsState,
            balances: balances,
            server_unix: nowUnix(),
            today_key_utc: todayKeyUtc()
        };
    }

    var claimRes = claimQuestReward(
        questsState,
        ro.cosmetics,
        ro.unlocks_state,
        questId
    );

    if (!claimRes.ok) {
        claimRes.server_unix = nowUnix();
        claimRes.today_key_utc = todayKeyUtc();
        return claimRes;
    }

    updateReadOnlyState({
        quests_state: claimRes.quests_state,
        cosmetics: claimRes.cosmetics,
        unlocks_state: claimRes.unlocks_state
    });

    return {
        ok: true,
        quest_id: claimRes.quest_id,
        reward_co: claimRes.reward_co,
        reward_ge: claimRes.reward_ge,
        reward_cosmetic_id: claimRes.reward_cosmetic_id,
        reward_chest_id: claimRes.reward_chest_id,
        unlock_ids: claimRes.unlock_ids,
        opened_chest_rewards: claimRes.opened_chest_rewards,
        quests_state: claimRes.quests_state,
        cosmetics: claimRes.cosmetics,
        unlocks_state: claimRes.unlocks_state,
        balances: getBalances(),
        server_unix: nowUnix(),
        today_key_utc: todayKeyUtc()
    };
}

handlers.ClaimQuest = claimQuest;
handlers.ClaimQuestV2 = claimQuest;

// =====================================================
// COSMETICS ACTIONS
// =====================================================

function equipCosmeticById(cosmetics, cosmeticId) {
    if (!cosmetics || typeof cosmetics !== "object") return false;

    var category = getCosmeticCategoryFromCatalog(cosmeticId);
    if (!category) return false;

    if (!cosmetics.equipped || typeof cosmetics.equipped !== "object") {
        cosmetics.equipped = defaultCosmetics().equipped;
    }

    if (toInt(cosmetics.equipped[category], 0) === toInt(cosmeticId, 0)) return false;

    cosmetics.equipped[category] = toInt(cosmeticId, 0);
    return true;
};

handlers.EquipCosmetic = function (args, context) {
    var category = sanitizeCosmeticCategory(args && args.category);
    var cosmeticId = toInt(args && args.cosmetic_id, 0);

    if (!category) return { ok: false, error: "INVALID_CATEGORY" };
    if (cosmeticId <= 0) return { ok: false, error: "INVALID_COSMETIC_ID" };
    if (!cosmeticExistsInCatalog(cosmeticId)) return { ok: false, error: "COSMETIC_NOT_FOUND_IN_CATALOG" };

    var realCategory = getCosmeticCategoryFromCatalog(cosmeticId);
    if (!realCategory) return { ok: false, error: "INVALID_COSMETIC_CATEGORY" };

    if (category !== realCategory) {
        return {
            ok: false,
            error: "CATEGORY_COSMETIC_MISMATCH",
            expected: realCategory,
            received: category
        };
    }

    var ro = getReadOnlyState(["cosmetics"]);
    var cosmetics = sanitizeCosmetics(ro.cosmetics);

    var currentId = cosmetics.equipped[category];
    if (currentId === cosmeticId) {
        return {
            ok: true,
            already_equipped: true,
            cosmetics: cosmetics,
            server_unix: nowUnix()
        };
    }

    var changed = equipCosmeticById(cosmetics, cosmeticId);
    if (!changed) return { ok: false, error: "EQUIP_FAILED" };

    cosmetics = sanitizeCosmetics(cosmetics);

    updateReadOnlyState({ cosmetics: cosmetics });

    return {
        ok: true,
        category: category,
        cosmetic_id: cosmeticId,
        cosmetics: cosmetics,
        server_unix: nowUnix()
    };
};

handlers.SetStarterCharacter = function (args, context) {
    var humanId = toInt(args && args.human_id, 0);
    var hairId = toInt(args && args.hair_id, 0);
    var hairColor = sanitizeHairColor(args && args.hair_color);

    if (humanId <= 0 || !cosmeticExistsInCatalog(humanId)) return { ok: false, error: "INVALID_HUMAN_ID" };
    if (hairId <= 0 || !cosmeticExistsInCatalog(hairId)) return { ok: false, error: "INVALID_HAIR_ID" };

    if (getCosmeticCategoryFromCatalog(humanId) !== CAT_HUMAN) return { ok: false, error: "HUMAN_CATEGORY_MISMATCH" };
    if (getCosmeticCategoryFromCatalog(hairId) !== CAT_HAIR) return { ok: false, error: "HAIR_CATEGORY_MISMATCH" };

    var ro = getReadOnlyState(["cosmetics"]);
    var ud = getUserDataState(["player_profile"]);

    var cosmetics = sanitizeCosmetics(ro.cosmetics);
    var playerProfile = sanitizePlayerProfile(ud.player_profile);

    cosmetics.equipped.human = humanId;
    cosmetics.equipped.hair = hairId;
    cosmetics.equipped.hair_color = hairColor;

    cosmetics = sanitizeCosmetics(cosmetics);

    playerProfile.starter_character_id = String(humanId);
    playerProfile.campaign_progress = mergeCampaignProgress(
        playerProfile.campaign_progress,
        CP_HAIR_DONE,
        false
    );

    playerProfile = sanitizePlayerProfile(playerProfile);

    updateReadOnlyState({ cosmetics: cosmetics });
    updateUserDataState({ player_profile: playerProfile });

    return {
        ok: true,
        human_id: humanId,
        hair_id: hairId,
        hair_color: hairColor,
        cosmetics: cosmetics,
        player_profile: playerProfile,
        server_unix: nowUnix()
    };
};

// =====================================================
// CHESTS ACTIONS
// =====================================================

handlers.AddChest = function (args, context) {
    var chestId = asString(
        args && args.chest_id != null ? args.chest_id : args && args.def_id != null ? args.def_id : "",
        ""
    ).trim().toLowerCase();

    if (!chestId) return { ok: false, error: "INVALID_CHEST_ID" };

    var chestDef = getChestDefById(chestId);
    if (!chestDef) return { ok: false, error: "CHEST_DEF_NOT_FOUND" };

    var ro = getReadOnlyState(["chests_state"]);
    var chestsState = refreshChestsStateByTime(ro.chests_state);
    var addRes = addChestToState(chestsState, chestDef);

    if (!addRes.ok) {
        addRes.server_unix = nowUnix();
        return addRes;
    }

    chestsState = refreshChestsStateByTime(addRes.chests_state);

    updateReadOnlyState({ chests_state: chestsState });

    return {
        ok: true,
        slot_index: addRes.slot_index,
        chests_state: chestsState,
        server_unix: nowUnix()
    };
};

handlers.AddRotatingChest = function (args, context) {
    var ro = getReadOnlyState(["chests_state"]);
    var chestsState = refreshChestsStateByTime(ro.chests_state);
    var grantRes = grantNextRotatingChestToState(chestsState);

    if (!grantRes.ok) {
        grantRes.server_unix = nowUnix();
        return grantRes;
    }

    updateReadOnlyState({ chests_state: grantRes.chests_state });

    return {
        ok: true,
        slot_index: grantRes.slot_index,
        granted_def_id: grantRes.granted_def_id,
        previous_rotation_index: grantRes.previous_rotation_index,
        rotation_index: grantRes.rotation_index,
        chests_state: grantRes.chests_state,
        server_unix: nowUnix()
    };
};

handlers.OpenChestNow = function (args, context) {
    var chestId = asString(
        args && args.chest_id != null ? args.chest_id : args && args.def_id != null ? args.def_id : "",
        ""
    ).trim().toLowerCase();

    if (!chestId) return { ok: false, error: "INVALID_CHEST_ID" };

    var chestDef = getChestDefById(chestId);
    if (!chestDef) return { ok: false, error: "CHEST_DEF_NOT_FOUND" };

    var ro = getReadOnlyState(["cosmetics"]);
    var cosmetics = sanitizeCosmetics(ro.cosmetics);

    var virtualChest = makeChestDataFromDef(chestDef);
    var rewards = generateChestRewards(virtualChest, cosmetics);

    if (rewards.co > 0) {
        server.AddUserVirtualCurrency({ PlayFabId: currentPlayerId, VirtualCurrency: "CO", Amount: rewards.co });
    }

    if (rewards.ge > 0) {
        server.AddUserVirtualCurrency({ PlayFabId: currentPlayerId, VirtualCurrency: "GE", Amount: rewards.ge });
    }

    if (rewards.cosmetic_id > 0 && cosmeticExistsInCatalog(rewards.cosmetic_id)) {
        equipCosmeticById(cosmetics, rewards.cosmetic_id);
        cosmetics = sanitizeCosmetics(cosmetics);
        updateReadOnlyState({ cosmetics: cosmetics });
    }

    return {
        ok: true,
        rewards: rewards,
        cosmetics: cosmetics,
        balances: getBalances(),
        server_unix: nowUnix()
    };
};

handlers.ClaimReadyChest = function (args, context) {
    var slotIndex = toInt(args && args.slot_index, -1);

    if (slotIndex < 0 || slotIndex >= MAX_CHEST_SLOTS) return { ok: false, error: "INVALID_SLOT_INDEX" };

    var ro = getReadOnlyState(["chests_state", "cosmetics"]);
    var chestsState = refreshChestsStateByTime(ro.chests_state);
    var cosmetics = sanitizeCosmetics(ro.cosmetics);

    var chest = chestsState.slots[slotIndex];

    if (isEmptyChestSlot(chest)) return { ok: false, error: "CHEST_NOT_FOUND" };

    if (sanitizeChestStatus(chest.status) !== CHEST_STATUS_READY) {
        return {
            ok: false,
            error: "CHEST_NOT_READY",
            chests_state: chestsState,
            server_unix: nowUnix()
        };
    }

    var rewards = generateChestRewards(chest, cosmetics);

    if (rewards.co > 0) {
        server.AddUserVirtualCurrency({ PlayFabId: currentPlayerId, VirtualCurrency: "CO", Amount: rewards.co });
    }

    if (rewards.ge > 0) {
        server.AddUserVirtualCurrency({ PlayFabId: currentPlayerId, VirtualCurrency: "GE", Amount: rewards.ge });
    }

    if (rewards.cosmetic_id > 0 && cosmeticExistsInCatalog(rewards.cosmetic_id)) {
        equipCosmeticById(cosmetics, rewards.cosmetic_id);
        cosmetics = sanitizeCosmetics(cosmetics);
    }

    chestsState.slots[slotIndex] = {};
    chestsState.queue_slot_indexes = removeSlotIndexFromQueue(chestsState.queue_slot_indexes, slotIndex);
    chestsState.last_server_time = nowUnix();
    chestsState = refreshChestsStateByTime(chestsState);

    updateReadOnlyState({
        chests_state: chestsState,
        cosmetics: cosmetics
    });

    return {
        ok: true,
        rewards: rewards,
        chests_state: chestsState,
        cosmetics: cosmetics,
        balances: getBalances(),
        server_unix: nowUnix()
    };
};

handlers.EnqueueChest = function (args, context) {
    var slotIndex = toInt(args && args.slot_index, -1);

    if (slotIndex < 0 || slotIndex >= MAX_CHEST_SLOTS) return { ok: false, error: "INVALID_SLOT_INDEX" };

    var ro = getReadOnlyState(["chests_state"]);
    var chestsState = refreshChestsStateByTime(ro.chests_state);
    var chest = chestsState.slots[slotIndex];

    if (isEmptyChestSlot(chest)) return { ok: false, error: "CHEST_NOT_FOUND", chests_state: chestsState };
    if (sanitizeChestStatus(chest.status) !== CHEST_STATUS_IDLE) return { ok: false, error: "CHEST_NOT_IDLE", chests_state: chestsState };
    if (isSlotIndexQueued(chestsState, slotIndex)) return { ok: false, error: "CHEST_ALREADY_QUEUED", chests_state: chestsState };

    if ((chestsState.queue_slot_indexes || []).length >= MAX_CHEST_QUEUE) {
        return { ok: false, error: "CHEST_QUEUE_FULL", chests_state: chestsState };
    }

    chestsState.queue_slot_indexes.push(slotIndex);
    chest.status = CHEST_STATUS_QUEUED;
    chestsState.slots[slotIndex] = chest;
    chestsState = promoteQueueIfNeeded(chestsState);
    chestsState = sanitizeChestsState(chestsState);

    updateReadOnlyState({ chests_state: chestsState });

    return {
        ok: true,
        slot_index: slotIndex,
        chests_state: chestsState,
        server_unix: nowUnix()
    };
};

handlers.DequeueChest = function (args, context) {
    var slotIndex = toInt(args && args.slot_index, -1);

    if (slotIndex < 0 || slotIndex >= MAX_CHEST_SLOTS) return { ok: false, error: "INVALID_SLOT_INDEX" };

    var ro = getReadOnlyState(["chests_state"]);
    var chestsState = refreshChestsStateByTime(ro.chests_state);
    var chest = chestsState.slots[slotIndex];

    if (isEmptyChestSlot(chest)) return { ok: false, error: "CHEST_NOT_FOUND", chests_state: chestsState };
    if (sanitizeChestStatus(chest.status) === CHEST_STATUS_OPENING) return { ok: false, error: "CHEST_IS_OPENING", chests_state: chestsState };
    if (sanitizeChestStatus(chest.status) !== CHEST_STATUS_QUEUED) return { ok: false, error: "CHEST_NOT_QUEUED", chests_state: chestsState };

    chestsState.queue_slot_indexes = removeSlotIndexFromQueue(chestsState.queue_slot_indexes, slotIndex);
    chest.status = CHEST_STATUS_IDLE;
    chest.start_ts = 0;
    chest.end_ts = 0;

    chestsState.slots[slotIndex] = chest;
    chestsState.last_server_time = nowUnix();
    chestsState = promoteQueueIfNeeded(chestsState);
    chestsState = sanitizeChestsState(chestsState);

    updateReadOnlyState({ chests_state: chestsState });

    return {
        ok: true,
        slot_index: slotIndex,
        chests_state: chestsState,
        server_unix: nowUnix()
    };
};

handlers.FinishChestNow = function (args, context) {
    var slotIndex = toInt(args && args.slot_index, -1);

    if (slotIndex < 0 || slotIndex >= MAX_CHEST_SLOTS) return { ok: false, error: "INVALID_SLOT_INDEX" };

    var ro = getReadOnlyState(["chests_state"]);
    var chestsState = refreshChestsStateByTime(ro.chests_state);
    var chest = chestsState.slots[slotIndex];

    if (isEmptyChestSlot(chest)) return { ok: false, error: "CHEST_NOT_FOUND", chests_state: chestsState };
    if (sanitizeChestStatus(chest.status) !== CHEST_STATUS_OPENING) return { ok: false, error: "CHEST_NOT_OPENING", chests_state: chestsState };

    var gemCost = getChestFinishGemCost(chestsState, slotIndex);
    var spendRes = spendCurrencyByPriceType(PRICE_GE, 0, gemCost);

    if (!spendRes.ok) {
        return {
            ok: false,
            error: spendRes.error || "NOT_ENOUGH_GE",
            needed: spendRes.needed || gemCost,
            current: spendRes.current || getBalanceGE(),
            gem_cost: gemCost,
            chests_state: chestsState,
            balances: getBalances(),
            server_unix: nowUnix()
        };
    }

    chestsState = markReadyForSlot(chestsState, slotIndex);
    chestsState = promoteQueueIfNeeded(chestsState);
    chestsState = sanitizeChestsState(chestsState);

    updateReadOnlyState({ chests_state: chestsState });

    return {
        ok: true,
        slot_index: slotIndex,
        gem_cost: gemCost,
        chests_state: chestsState,
        balances: getBalances(),
        server_unix: nowUnix()
    };
};

// =====================================================
// JOURNEY ACTIONS
// =====================================================

handlers.AddJourneySteps = function (args, context) {
    var amount = Math.max(0, toInt(args && args.amount, 0));

    if (amount <= 0) {
        return { ok: false, error: "INVALID_AMOUNT" };
    }

    var ro = getReadOnlyState(["journey_state"]);
    var journeyState = sanitizeJourneyState(ro.journey_state);

    var previous = Math.max(0, toInt(journeyState.total_steps, 0));
    var next = previous + amount;

    journeyState.total_steps = next;
    journeyState.updated_at = nowUnix();

    updateReadOnlyState({
        journey_state: journeyState
    });

    return {
        ok: true,
        amount: amount,
        previous_total_steps: previous,
        total_steps: next,
        journey_state: journeyState,
        balances: getBalances(),
        server_unix: nowUnix()
    };
};
// =====================================================
// SHOP ACTIONS
// =====================================================

handlers.BuyShopItem = function (args, context) {
    var itemId = asString(args && args.item_id, "").trim().toLowerCase();

    if (!itemId) return { ok: false, error: "INVALID_ITEM_ID" };

    var catalog = readShopCatalog();
    var itemDef = catalog[itemId];

    if (!itemDef) return { ok: false, error: "ITEM_NOT_FOUND_IN_CATALOG" };

    var itemType = asString(itemDef.item_type, "").trim().toLowerCase();
    var priceType = asString(itemDef.price_type, "").trim().toLowerCase();
    var shopSection = asString(itemDef.shop_section, "").trim().toLowerCase();

    var priceCo = Math.max(0, toInt(itemDef.price_co, 0));
    var priceGe = Math.max(0, toInt(itemDef.price_ge, 0));
    var priceRealText = asString(itemDef.price_real_text, "").trim();

    var rewardCosmeticId = Math.max(0, toInt(itemDef.reward_cosmetic_id, 0));
    var rewardCo = Math.max(0, toInt(itemDef.reward_co, 0));
    var rewardGe = Math.max(0, toInt(itemDef.reward_ge, 0));
    var rewardChestId = asString(itemDef.reward_chest_id, "").trim().toLowerCase();
    var rewardUnlockIds = sanitizeUnlockIdArray(itemDef.unlock_ids instanceof Array ? itemDef.unlock_ids : []);
    var purchaseRule = sanitizePurchaseRule(itemDef.purchase_rule);

    var openedChestRewards = null;
    var chestRewardDef = null;

    if (
        itemType !== ITEM_COSMETIC &&
        itemType !== ITEM_CHEST &&
        itemType !== ITEM_CURRENCY &&
        itemType !== ITEM_BUNDLE &&
        itemType !== ITEM_UNLOCK
    ) {
        return { ok: false, error: "INVALID_ITEM_TYPE" };
    }

    if (
        priceType !== PRICE_FREE &&
        priceType !== PRICE_CO &&
        priceType !== PRICE_GE &&
        priceType !== PRICE_AD &&
        priceType !== PRICE_REAL
    ) {
        return { ok: false, error: "INVALID_PRICE_TYPE" };
    }

    if (priceType === PRICE_CO && priceCo <= 0) return { ok: false, error: "INVALID_PRICE_CO" };
    if (priceType === PRICE_GE && priceGe <= 0) return { ok: false, error: "INVALID_PRICE_GE" };
    if (priceType === PRICE_REAL && !priceRealText) return { ok: false, error: "INVALID_PRICE_REAL" };

    if (itemType === ITEM_COSMETIC) {
        if (rewardCosmeticId <= 0) return { ok: false, error: "INVALID_COSMETIC_REWARD" };
        if (!cosmeticExistsInCatalog(rewardCosmeticId)) return { ok: false, error: "COSMETIC_REWARD_NOT_FOUND_IN_CATALOG" };
    }

    if (itemType === ITEM_CHEST || (itemType === ITEM_BUNDLE && rewardChestId && rewardChestId !== "none")) {
        if (!rewardChestId || rewardChestId === "none") return { ok: false, error: "INVALID_CHEST_REWARD" };

        chestRewardDef = getChestDefById(rewardChestId);
        if (!chestRewardDef) return { ok: false, error: "CHEST_DEF_NOT_FOUND", reward_chest_id: rewardChestId };
    }

    if (itemType === ITEM_CURRENCY && rewardCo <= 0 && rewardGe <= 0) {
        return { ok: false, error: "INVALID_CURRENCY_REWARD" };
    }

    if (itemType === ITEM_UNLOCK && rewardUnlockIds.length <= 0) {
        return { ok: false, error: "INVALID_UNLOCK_REWARD" };
    }

    if (
        itemType === ITEM_BUNDLE &&
        rewardCosmeticId <= 0 &&
        rewardCo <= 0 &&
        rewardGe <= 0 &&
        (!rewardChestId || rewardChestId === "none") &&
        rewardUnlockIds.length <= 0
    ) {
        return { ok: false, error: "INVALID_BUNDLE_REWARD" };
    }

    var ro = getReadOnlyState(["shop_state", "cosmetics", "chests_state", "unlocks_state"]);

    var shopState = sanitizeShopState(ro.shop_state);
    var cosmetics = sanitizeCosmetics(ro.cosmetics);
    var chestsState = refreshChestsStateByTime(ro.chests_state);
    var unlocksState = sanitizeUnlocksState(ro.unlocks_state);

    var today = todayKeyUtc();
    shopState = ensureTodayDailyShop(shopState, catalog, today);

    if (purchaseRule === RULE_DAILY_ONCE && !isItemInTodayShop(shopState, itemId)) {
        return {
            ok: false,
            error: "ITEM_NOT_IN_DAILY_SHOP",
            shop_state: shopState,
            cosmetics: cosmetics,
            unlocks_state: unlocksState,
            chests_state: chestsState,
            balances: getBalances(),
            server_unix: nowUnix(),
            today_key_utc: today
        };
    }

    if (purchaseRule === RULE_DAILY_ONCE && isShopItemClaimedToday(shopState, itemId)) {
        return {
            ok: false,
            error: "ALREADY_CLAIMED_TODAY",
            shop_state: shopState,
            cosmetics: cosmetics,
            unlocks_state: unlocksState,
            chests_state: chestsState,
            balances: getBalances(),
            server_unix: nowUnix(),
            today_key_utc: today
        };
    }

    if (purchaseRule === RULE_ACCOUNT_ONCE && isShopItemAlreadyPurchased(itemId)) {
        return {
            ok: false,
            error: "ALREADY_OWNED",
            shop_state: shopState,
            cosmetics: cosmetics,
            unlocks_state: unlocksState,
            chests_state: chestsState,
            balances: getBalances(),
            server_unix: nowUnix(),
            today_key_utc: today
        };
    }

    if (itemType === ITEM_UNLOCK && hasAllUnlockIds(unlocksState, rewardUnlockIds)) {
        return {
            ok: false,
            error: "ALREADY_OWNED",
            shop_state: shopState,
            cosmetics: cosmetics,
            unlocks_state: unlocksState,
            chests_state: chestsState,
            balances: getBalances(),
            server_unix: nowUnix(),
            today_key_utc: today
        };
    }

    var spendRes = spendCurrencyByPriceType(priceType, priceCo, priceGe);

    if (!spendRes.ok) {
        spendRes.shop_state = shopState;
        spendRes.cosmetics = cosmetics;
        spendRes.unlocks_state = unlocksState;
        spendRes.chests_state = chestsState;
        spendRes.balances = getBalances();
        spendRes.server_unix = nowUnix();
        spendRes.today_key_utc = today;
        return spendRes;
    }

    if (itemType === ITEM_COSMETIC) {
        equipCosmeticById(cosmetics, rewardCosmeticId);

    } else if (itemType === ITEM_UNLOCK) {
        var unlockGrantRes = grantUnlockIdsToState(unlocksState, rewardUnlockIds);
        unlocksState = sanitizeUnlocksState(unlockGrantRes.unlocks_state);

    } else if (itemType === ITEM_CURRENCY) {
        if (rewardCo > 0) server.AddUserVirtualCurrency({ PlayFabId: currentPlayerId, VirtualCurrency: "CO", Amount: rewardCo });
        if (rewardGe > 0) server.AddUserVirtualCurrency({ PlayFabId: currentPlayerId, VirtualCurrency: "GE", Amount: rewardGe });

    } else if (itemType === ITEM_CHEST) {
        var virtualChest = makeChestDataFromDef(chestRewardDef);
        var rewards = generateChestRewards(virtualChest, cosmetics);

        if (rewards.co > 0) server.AddUserVirtualCurrency({ PlayFabId: currentPlayerId, VirtualCurrency: "CO", Amount: rewards.co });
        if (rewards.ge > 0) server.AddUserVirtualCurrency({ PlayFabId: currentPlayerId, VirtualCurrency: "GE", Amount: rewards.ge });

        if (rewards.cosmetic_id > 0 && cosmeticExistsInCatalog(rewards.cosmetic_id)) {
            equipCosmeticById(cosmetics, rewards.cosmetic_id);
        }

        openedChestRewards = rewards;

    } else if (itemType === ITEM_BUNDLE) {
        if (rewardCo > 0) server.AddUserVirtualCurrency({ PlayFabId: currentPlayerId, VirtualCurrency: "CO", Amount: rewardCo });
        if (rewardGe > 0) server.AddUserVirtualCurrency({ PlayFabId: currentPlayerId, VirtualCurrency: "GE", Amount: rewardGe });

        if (rewardCosmeticId > 0 && cosmeticExistsInCatalog(rewardCosmeticId)) {
            equipCosmeticById(cosmetics, rewardCosmeticId);
        }

        if (rewardUnlockIds.length > 0) {
            var bundleUnlockGrantRes = grantUnlockIdsToState(unlocksState, rewardUnlockIds);
            unlocksState = sanitizeUnlocksState(bundleUnlockGrantRes.unlocks_state);
        }

        if (rewardChestId && rewardChestId !== "none") {
            var virtualBundleChest = makeChestDataFromDef(chestRewardDef);
            var bundleRewards = generateChestRewards(virtualBundleChest, cosmetics);

            if (bundleRewards.co > 0) server.AddUserVirtualCurrency({ PlayFabId: currentPlayerId, VirtualCurrency: "CO", Amount: bundleRewards.co });
            if (bundleRewards.ge > 0) server.AddUserVirtualCurrency({ PlayFabId: currentPlayerId, VirtualCurrency: "GE", Amount: bundleRewards.ge });

            if (bundleRewards.cosmetic_id > 0 && cosmeticExistsInCatalog(bundleRewards.cosmetic_id)) {
                equipCosmeticById(cosmetics, bundleRewards.cosmetic_id);
            }

            openedChestRewards = bundleRewards;
        }
    }

    cosmetics = sanitizeCosmetics(cosmetics);
    unlocksState = sanitizeUnlocksState(unlocksState);
    shopState = sanitizeShopState(shopState);
    chestsState = sanitizeChestsState(chestsState);

    if (purchaseRule === RULE_DAILY_ONCE) {
        shopState = markShopItemClaimedToday(shopState, itemId);
    }

    if (purchaseRule === RULE_ACCOUNT_ONCE) {
        markShopItemPurchased(itemId);
    }

    updateReadOnlyState({
        cosmetics: cosmetics,
        unlocks_state: unlocksState,
        shop_state: shopState,
        chests_state: chestsState
    });

    return {
        ok: true,
        item_id: itemId,
        item_type: itemType,
        shop_section: shopSection,
        reward_cosmetic_id: rewardCosmeticId,
        reward_chest_id: rewardChestId,
        reward_unlock_ids: rewardUnlockIds,
        reward_co: rewardCo,
        reward_ge: rewardGe,
        purchase_rule: purchaseRule,
        opened_chest_rewards: openedChestRewards,
        cosmetics: cosmetics,
        unlocks_state: unlocksState,
        shop_state: shopState,
        chests_state: chestsState,
        balances: getBalances(),
        server_unix: nowUnix(),
        today_key_utc: today
    };
};
// =====================================================
// WEIGHTED DAILY SHOP REWARD SLOT
// =====================================================

function buildRandomShopRewardEntries(itemDef) {
    var catalog = readCosmeticCatalog();
    var wantedCategory = asString(itemDef.random_cosmetic_category, "any").trim().toLowerCase();
    var out = [];

    for (var rawId in catalog) {
        if (!catalog.hasOwnProperty(rawId)) continue;

        var entry = catalog[rawId] || {};
        var cosmeticId = Math.max(0, toInt(entry.id || rawId, 0));
        var category = asString(entry.category, "").trim().toLowerCase();
        var weight = Math.max(0, toFloat(entry.shop_slot_weight, 0));

        if (cosmeticId <= 0 || !toBool(entry.enabled, true) || toBool(entry.starter_available, false)) continue;
        if (!toBool(entry.can_appear_in_shop, false) || weight <= 0) continue;
        if (wantedCategory !== "" && wantedCategory !== "any" && category !== wantedCategory) continue;

        out.push({
            item_type: ITEM_COSMETIC,
            reward_cosmetic_id: cosmeticId,
            reward_unlock_ids: [],
            weight: weight
        });
    }

    if (out.length > 0) return out;

    // B4/C4 are guaranteed random slots. If no Def explicitly opted into the
    // shop pool, fall back to all enabled non-starter cosmetics.
    for (var fallbackRawId in catalog) {
        if (!catalog.hasOwnProperty(fallbackRawId)) continue;
        var fallbackDef = catalog[fallbackRawId] || {};
        var fallbackId = Math.max(0, toInt(fallbackDef.id, toInt(fallbackRawId, 0)));
        var fallbackCategory = asString(fallbackDef.category, "").trim().toLowerCase();
        if (fallbackId <= 0 || !toBool(fallbackDef.enabled, true) || toBool(fallbackDef.starter_available, false)) continue;
        if (wantedCategory !== "any" && fallbackCategory !== wantedCategory) continue;
        out.push({
            source: "cosmetic_fallback",
            item_type: ITEM_COSMETIC,
            reward_cosmetic_id: fallbackId,
            reward_unlock_ids: [],
            weight: 1
        });
    }

    return out;
}

function resolveDailyShopReward(itemDef, itemId, todayKey) {
    var candidates = itemDef.random_reward_entries instanceof Array
        ? clone(itemDef.random_reward_entries)
        : [];

    if (candidates.length <= 0) {
        candidates = buildRandomShopRewardEntries(itemDef);
    }

    var valid = [];
    var totalWeight = 0;

    for (var i = 0; i < candidates.length; i++) {
        var candidate = candidates[i] || {};
        var weight = Math.max(0, toFloat(candidate.weight, 0));
        if (weight <= 0) continue;
        candidate.weight = weight;
        valid.push(candidate);
        totalWeight += weight;
    }

    if (valid.length <= 0 || totalWeight <= 0) return null;

    var roll = seededUnitInterval(todayKey + "|" + itemId + "|reward") * totalWeight;
    var cursor = 0;

    for (var j = 0; j < valid.length; j++) {
        cursor += valid[j].weight;
        if (roll < cursor) return valid[j];
    }

    return valid[valid.length - 1];
}

function buildDailyOfferCosmeticIds(shopState, catalog, todayKey) {
    var state = shopState || {};
    var ids = sanitizeLowerStringArray(state.daily_item_ids || []);
    var offers = {};

    for (var i = 0; i < ids.length; i++) {
        var itemId = ids[i];
        var itemDef = (catalog || {})[itemId];
        if (!itemDef || !toBool(itemDef.random_cosmetic_reward, false)) continue;

        var resolvedReward = resolveDailyShopReward(itemDef, itemId, todayKey);
        var cosmeticId = resolvedReward
            ? Math.max(0, toInt(resolvedReward.reward_cosmetic_id, 0))
            : 0;
        if (cosmeticId > 0) offers[itemId] = cosmeticId;
    }

    return offers;
}

function attachDailyOfferCosmeticIds(shopState, catalog, todayKey) {
    if (!shopState) return shopState;
    var safeTodayKey = asString(todayKey, todayKeyUtc()).trim();
    shopState.daily_offer_cosmetic_ids = buildDailyOfferCosmeticIds(shopState, catalog || {}, safeTodayKey);
    return shopState;
}

var baseGenerateChestRewards = generateChestRewards;

generateChestRewards = function (chest, cosmetics) {
    var effectiveChest = clone(chest || {});
    var chestDef = getChestDefById(asString(effectiveChest.def_id, "").trim().toLowerCase());

    if (chestDef) {
        effectiveChest.reward_co_min = Math.max(0, toInt(chestDef.reward_co_min, 0));
        effectiveChest.reward_co_max = Math.max(effectiveChest.reward_co_min, toInt(chestDef.reward_co_max, effectiveChest.reward_co_min));
        effectiveChest.reward_ge_min = Math.max(0, toInt(chestDef.reward_ge_min, 0));
        effectiveChest.reward_ge_max = Math.max(effectiveChest.reward_ge_min, toInt(chestDef.reward_ge_max, effectiveChest.reward_ge_min));
        effectiveChest.cosmetic_drop_chance = Math.max(0, toFloat(chestDef.cosmetic_drop_chance, 0));
        effectiveChest.possible_cosmetic_ids = chestDef.possible_cosmetic_ids instanceof Array
            ? clone(chestDef.possible_cosmetic_ids)
            : [];
    }

    return baseGenerateChestRewards(effectiveChest, cosmetics);
};

var baseEnsureTodayDailyShop = ensureTodayDailyShop;

ensureTodayDailyShop = function (shopState, catalog, todayKey) {
    var state = sanitizeShopState(shopState);
    var safeTodayKey = asString(todayKey, todayKeyUtc()).trim();
    var expectedIds = buildTodayDailyShopItemIds(catalog || {}, safeTodayKey);

    if (state.daily_key !== safeTodayKey) {
        return attachDailyOfferCosmeticIds(
            baseEnsureTodayDailyShop(state, catalog, safeTodayKey),
            catalog,
            safeTodayKey
        );
    }

    var currentIds = sanitizeLowerStringArray(state.daily_item_ids || []);
    var sameCatalogSelection = currentIds.length === expectedIds.length;

    if (sameCatalogSelection) {
        for (var i = 0; i < expectedIds.length; i++) {
            if (currentIds[i] !== expectedIds[i]) {
                sameCatalogSelection = false;
                break;
            }
        }
    }

    if (sameCatalogSelection) return attachDailyOfferCosmeticIds(state, catalog, safeTodayKey);

    var preservedClaims = {};
    for (var itemId in state.claimed_today) {
        if (!state.claimed_today.hasOwnProperty(itemId)) continue;
        if (expectedIds.indexOf(itemId) !== -1 && toBool(state.claimed_today[itemId], false)) {
            preservedClaims[itemId] = true;
        }
    }

    state.daily_item_ids = expectedIds;
    state.claimed_today = preservedClaims;
    return attachDailyOfferCosmeticIds(state, catalog, safeTodayKey);
};

if (typeof handlers.GetFullPlayerState === "function") {
    var baseGetFullPlayerState = handlers.GetFullPlayerState;
    handlers.GetFullPlayerState = function (args, context) {
        var result = baseGetFullPlayerState(args, context);
        if (result && result.shop_state) {
            var catalog = readShopCatalog();
            var key = asString(result.shop_state.daily_key, todayKeyUtc()).trim();
            attachDailyOfferCosmeticIds(result.shop_state, catalog, key);
        }
        return result;
    };
}

var baseBuyShopItem = handlers.BuyShopItem;

handlers.BuyShopItem = function (args, context) {
    var itemId = asString(args && args.item_id, "").trim().toLowerCase();
    var originalReadShopCatalog = readShopCatalog;
    var catalog = originalReadShopCatalog();
    var itemDef = catalog[itemId];

    var offeredCosmeticId = Math.max(0, toInt(args && args.offered_cosmetic_id, 0));
    var resolvedCosmeticId = 0;

    if (itemDef && toBool(itemDef.random_cosmetic_reward, false)) {
        var resolvedReward = resolveDailyShopReward(itemDef, itemId, todayKeyUtc());
        if (resolvedReward) {
            resolvedCosmeticId = Math.max(0, toInt(resolvedReward.reward_cosmetic_id, 0));
            if (offeredCosmeticId > 0 && offeredCosmeticId !== resolvedCosmeticId) {
                return { ok: false, error: "DAILY_OFFER_MISMATCH" };
            }

            itemDef.item_type = asString(resolvedReward.item_type, ITEM_COSMETIC).trim().toLowerCase();
            itemDef.reward_cosmetic_id = resolvedCosmeticId;
            itemDef.unlock_ids = sanitizeUnlockIdArray(resolvedReward.reward_unlock_ids || []);
            itemDef.reward_co = Math.max(0, toInt(resolvedReward.reward_co, 0));
            itemDef.reward_ge = Math.max(0, toInt(resolvedReward.reward_ge, 0));
            itemDef.reward_chest_id = asString(resolvedReward.reward_chest_id, "none").trim().toLowerCase();
        }
    }

    readShopCatalog = function () {
        return catalog;
    };

    try {
        var result = baseBuyShopItem(args, context);
        if (result && result.shop_state) {
            attachDailyOfferCosmeticIds(result.shop_state, catalog, todayKeyUtc());
        }
        if (result && result.ok && resolvedCosmeticId > 0) {
            result.reward_cosmetic_id = resolvedCosmeticId;
        }
        return result;
    } finally {
        readShopCatalog = originalReadShopCatalog;
    }
};

function runShopPurchaseWithVerifiedPrice(itemId, verifiedPriceType, context) {
    var originalSpendCurrency = spendCurrencyByPriceType;
    spendCurrencyByPriceType = function (priceType, priceCo, priceGe) {
        var safeType = asString(priceType, "").trim().toLowerCase();
        if (safeType === verifiedPriceType) return { ok: true };
        return originalSpendCurrency(priceType, priceCo, priceGe);
    };

    try {
        return handlers.BuyShopItem({ item_id: itemId }, context);
    } finally {
        spendCurrencyByPriceType = originalSpendCurrency;
    }
}

handlers.ClaimRewardedShopItem = function (args, context) {
    var itemId = asString(args && args.item_id, "").trim().toLowerCase();
    var catalog = readShopCatalog();
    var itemDef = catalog[itemId];

    if (!itemDef) return { ok: false, error: "ITEM_NOT_FOUND_IN_CATALOG" };
    if (asString(itemDef.price_type, "").trim().toLowerCase() !== PRICE_AD) {
        return { ok: false, error: "ITEM_IS_NOT_REWARDED_AD" };
    }

    var result = runShopPurchaseWithVerifiedPrice(itemId, PRICE_AD, context);
    if (result && result.ok) result.rewarded_ad_claim = true;
    return result;
};

handlers.ClaimRoulettePrize = function (args, context) {
    var itemId = asString(args && args.item_id, "").trim().toLowerCase();
    var catalog = readShopCatalog();
    var itemDef = catalog[itemId];

    if (!itemDef) return { ok: false, error: "ITEM_NOT_FOUND_IN_CATALOG" };
    if (!toBool(itemDef.enabled_in_rotation, false)) {
        return { ok: false, error: "ITEM_IS_NOT_ROULETTE_PRIZE" };
    }
    if (asString(itemDef.price_type, "").trim().toLowerCase() !== PRICE_FREE) {
        return { ok: false, error: "ROULETTE_PRIZE_NOT_FREE" };
    }

    var result = runShopPurchaseWithVerifiedPrice(itemId, PRICE_FREE, context);
    if (result && result.ok) result.roulette_claim = true;
    return result;
};



// =====================================================
// CURRENCY ACTIONS
// =====================================================

handlers.SpendCO = function (args, context) {
    var amount = Math.max(0, toInt(args && args.amount, 0));
    var reason = asString(args && args.reason, "spend").trim();
    var purchaseId = asString(args && args.purchase_id, "").trim();

    if (amount <= 0) {
        return {
            ok: false,
            error: "INVALID_AMOUNT",
            balances: getBalances(),
            server_unix: nowUnix()
        };
    }

    var currentCo = getBalanceCO();

    if (currentCo < amount) {
        return {
            ok: false,
            error: "NOT_ENOUGH_CO",
            needed: amount,
            current: currentCo,
            balances: getBalances(),
            server_unix: nowUnix()
        };
    }

    server.SubtractUserVirtualCurrency({
        PlayFabId: currentPlayerId,
        VirtualCurrency: "CO",
        Amount: amount
    });

    return {
        ok: true,
        amount: amount,
        reason: reason,
        purchase_id: purchaseId,
        balances: getBalances(),
        server_unix: nowUnix()
    };
};

// =====================================================
// ACCOUNT / JOURNEY / CURRENCY ACTIONS
// =====================================================

handlers.SetJourneyState = function (args, context) {
    var ro = getReadOnlyState(["journey_state"]);
    var current = sanitizeJourneyState(ro.journey_state);
    var next = sanitizeJourneyState(args || current);

    updateReadOnlyState({ journey_state: next });

    return {
        ok: true,
        journey_state: next,
        server_unix: nowUnix()
    };
};

function subtractKnownCurrency(currencyCode, amount) {
    amount = Math.max(0, toInt(amount, 0));
    if (amount <= 0) return 0;

    server.SubtractUserVirtualCurrency({
        PlayFabId: currentPlayerId,
        VirtualCurrency: currencyCode,
        Amount: amount
    });

    return amount;
}

function clearPurchasedShopMarksForCurrentCatalog() {
    var catalog = readShopCatalog();
    var mark = {};
    var count = 0;

    for (var itemId in catalog) {
        if (!catalog.hasOwnProperty(itemId)) continue;
        mark[makeSafeInternalKey("purchased_", itemId)] = false;
        count++;
    }

    if (count > 0) {
        updateInternalStateRaw(mark);
    }

    return count;
}

function buildResetAccountState() {
    var ud = getUserDataState(["player_profile"]);
    var currentProfile = sanitizePlayerProfile(ud.player_profile);

    var playerProfile = defaultPlayerProfile();
    playerProfile.display_name = currentProfile.display_name;
    playerProfile.language = currentProfile.language;
    playerProfile = sanitizePlayerProfile(playerProfile);

    var cosmetics = defaultCosmetics();
    var unlocksState = defaultUnlocksState();
    var questsState = sanitizeQuestsState(defaultQuestsState());
    var chestsState = sanitizeChestsState(defaultChestsState());
    var shopState = sanitizeShopState(defaultShopState());
    var today = todayKeyUtc();
    shopState = ensureTodayDailyShop(shopState, readShopCatalog(), today);
    var journeyState = sanitizeJourneyState(defaultJourneyState());
    var metaState = defaultMetaState();

    return {
        today_key_utc: today,
        player_profile: playerProfile,
        cosmetics: cosmetics,
        unlocks_state: unlocksState,
        quests_state: questsState,
        chests_state: chestsState,
        shop_state: shopState,
        journey_state: journeyState,
        meta_state: metaState
    };
}

function applyResetAccountState(state) {
    updateUserDataState({
        player_profile: state.player_profile
    });

    updateReadOnlyState({
        cosmetics: state.cosmetics,
        unlocks_state: state.unlocks_state,
        quests_state: state.quests_state,
        chests_state: state.chests_state,
        shop_state: state.shop_state,
        journey_state: state.journey_state,
        meta_state: state.meta_state
    });

    clearPurchasedShopMarksForCurrentCatalog();
}

function resetKnownBalancesToZero() {
    var before = getBalances();
    var removedCO = subtractKnownCurrency("CO", before.CO);
    var removedGE = subtractKnownCurrency("GE", before.GE);

    return {
        before: before,
        removed: {
            CO: removedCO,
            GE: removedGE
        },
        after: getBalances()
    };
}

handlers.ResetAccountProgress = function (args, context) {
    var state = buildResetAccountState();
    applyResetAccountState(state);
    var currencyReset = resetKnownBalancesToZero();

    return {
        ok: true,
        server_unix: nowUnix(),
        today_key_utc: state.today_key_utc,
        player_profile: state.player_profile,
        cosmetics: state.cosmetics,
        unlocks_state: state.unlocks_state,
        quests_state: state.quests_state,
        chests_state: state.chests_state,
        shop_state: state.shop_state,
        journey_state: state.journey_state,
        meta_state: state.meta_state,
        balances: currencyReset.after,
        currency_reset: currencyReset
    };
};

handlers.DeleteAccount = function (args, context) {
    var hardDeleteError = "";

    try {
        if (server.DeletePlayer) {
            server.DeletePlayer({
                PlayFabId: currentPlayerId
            });

            return {
                ok: true,
                deleted: true,
                soft_deleted: false,
                server_unix: nowUnix()
            };
        }

        hardDeleteError = "SERVER_DELETE_PLAYER_NOT_AVAILABLE";

    } catch (e) {
        hardDeleteError = asString(e, "DELETE_PLAYER_FAILED");
    }

    var state = buildResetAccountState();
    state.player_profile.display_name = "";
    state.player_profile.language = "en";
    state.player_profile = sanitizePlayerProfile(state.player_profile);

    applyResetAccountState(state);
    var currencyReset = resetKnownBalancesToZero();

    updateInternalStateRaw({
        account_deleted: {
            deleted_at: nowUnix(),
            hard_delete_error: hardDeleteError
        }
    });

    return {
        ok: true,
        deleted: false,
        soft_deleted: true,
        hard_delete_error: hardDeleteError,
        server_unix: nowUnix(),
        balances: currencyReset.after,
        currency_reset: currencyReset
    };
};

handlers.GrantCO = function (args, context) {
    var amount = Math.max(0, toInt(args && args.amount, 0));
    var reason = asString(args && args.reason, "reward").trim();
    var rewardId = asString(args && (args.reward_id || args.rewardId), "").trim();

    if (amount <= 0) {
        return {
            ok: false,
            error: "INVALID_AMOUNT",
            balances: getBalances(),
            server_unix: nowUnix()
        };
    }

    server.AddUserVirtualCurrency({
        PlayFabId: currentPlayerId,
        VirtualCurrency: "CO",
        Amount: amount
    });

    return {
        ok: true,
        amount: amount,
        reason: reason,
        reward_id: rewardId,
        balances: getBalances(),
        server_unix: nowUnix()
    };
};

handlers.GrantGE = function (args, context) {
    var amount = Math.max(0, toInt(args && args.amount, 0));
    var reason = asString(args && args.reason, "reward_ge").trim();
    var rewardId = asString(args && (args.reward_id || args.rewardId), "").trim();

    if (amount <= 0) {
        return {
            ok: false,
            error: "INVALID_AMOUNT",
            balances: getBalances(),
            server_unix: nowUnix()
        };
    }

    server.AddUserVirtualCurrency({
        PlayFabId: currentPlayerId,
        VirtualCurrency: "GE",
        Amount: amount
    });

    return {
        ok: true,
        amount: amount,
        reason: reason,
        reward_id: rewardId,
        balances: getBalances(),
        server_unix: nowUnix()
    };
};

handlers.SpendGE = function (args, context) {
    var amount = Math.max(0, toInt(args && args.amount, 0));
    var reason = asString(args && args.reason, "spend_ge").trim();
    var purchaseId = asString(args && (args.purchase_id || args.purchaseId), "").trim();

    if (amount <= 0) {
        return {
            ok: false,
            error: "INVALID_AMOUNT",
            balances: getBalances(),
            server_unix: nowUnix()
        };
    }

    var currentGe = getBalanceGE();

    if (currentGe < amount) {
        return {
            ok: false,
            error: "NOT_ENOUGH_GE",
            needed: amount,
            current: currentGe,
            balances: getBalances(),
            server_unix: nowUnix()
        };
    }

    server.SubtractUserVirtualCurrency({
        PlayFabId: currentPlayerId,
        VirtualCurrency: "GE",
        Amount: amount
    });

    return {
        ok: true,
        amount: amount,
        reason: reason,
        purchase_id: purchaseId,
        balances: getBalances(),
        server_unix: nowUnix()
    };
};
