# ddd
// ============================================================
// game_systems_firebase.js  —  Firebase 연동 확장 시스템
// Firestore: 유저DB / 그룹 / 랭킹 / 마이룸 / 아이템
// ============================================================

// ── Firebase SDK (CDN 모듈 방식) ──
import { initializeApp }                          from "https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js";
import { getFirestore, doc, getDoc, setDoc,
         updateDoc, arrayUnion, arrayRemove,
         onSnapshot, collection, query,
         orderBy, serverTimestamp, writeBatch }   from "https://www.gstatic.com/firebasejs/10.12.2/firebase-firestore.js";

const firebaseConfig = {
  apiKey:            "AIzaSyBo1KtM2kTV0Vzo02gbt1pO0R_l4FO5_Q8",
  authDomain:        "english-rpg-game-5478e.firebaseapp.com",
  projectId:         "english-rpg-game-5478e",
  storageBucket:     "english-rpg-game-5478e.firebasestorage.app",
  messagingSenderId: "419282044565",
  appId:             "1:419282044565:web:cb7aac385519cbc979fd9c",
  measurementId:     "G-314HEG3L5V"
};

const _app = initializeApp(firebaseConfig);
const db   = getFirestore(_app);

// ──────────────────────────────────────────
// 1. 레벨 & 경험치 (순수 계산 — 로컬 OK)
// ──────────────────────────────────────────
function xpRequiredForLevel(level) {
  return Math.floor(100 * level * (level + 1) / 2);
}
function calcLevel(totalXp) {
  let lv = 1;
  while (totalXp >= xpRequiredForLevel(lv)) lv++;
  return lv - 1;
}
function xpProgress(totalXp) {
  const lv   = calcLevel(totalXp);
  const prev = xpRequiredForLevel(lv - 1) || 0;
  const next = xpRequiredForLevel(lv);
  return { current: totalXp - prev, required: next - prev, level: lv };
}

const XP_REWARDS = {
  training_correct : 5,
  training_clear   : 30,
  dungeon_clear    : (floor) => Math.floor(50 + floor * 15),
  dungeon_replay   : (floor) => Math.floor(10 + floor * 3),
};

// ──────────────────────────────────────────
// 2. 유저 Firestore CRUD
//    컬렉션: "users/{userId}"
// ──────────────────────────────────────────
async function fsGetUser(userId) {
  const snap = await getDoc(doc(db, "users", userId));
  return snap.exists() ? snap.data() : null;
}

async function fsSetUser(userId, data) {
  await setDoc(doc(db, "users", userId), data, { merge: true });
}

/** XP 추가 후 레벨업 체크, 레벨업 시 새 레벨 반환 (없으면 null) */
async function fsAddXP(userId, amount) {
  const userData = await fsGetUser(userId);
  const before   = userData?.totalXp || 0;
  const lvBefore = calcLevel(before);
  const after    = before + amount;
  const lvAfter  = calcLevel(after);
  await fsSetUser(userId, { totalXp: after, level: lvAfter });
  return lvAfter > lvBefore ? lvAfter : null;
}

// ──────────────────────────────────────────
// 3. 그룹 Firestore CRUD
//    컬렉션: "groups/{groupCode}"
// ──────────────────────────────────────────
function generateGroupCode() {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let code = "";
  for (let i = 0; i < 6; i++) code += chars[Math.floor(Math.random() * chars.length)];
  return code;
}

async function fsCreateGroup(ownerUserId, groupName) {
  let code;
  // 중복 코드 방지 루프
  while (true) {
    code = generateGroupCode();
    const snap = await getDoc(doc(db, "groups", code));
    if (!snap.exists()) break;
  }
  const batch = writeBatch(db);
  batch.set(doc(db, "groups", code), {
    code,
    name        : groupName || `${ownerUserId}의 그룹`,
    owner       : ownerUserId,
    members     : [ownerUserId],
    createdAt   : serverTimestamp(),
    weeklyReset : getWeekStart(),
  });
  batch.set(doc(db, "users", ownerUserId), { groupCode: code }, { merge: true });
  await batch.commit();
  return code;
}

async function fsJoinGroup(userId, code) {
  code = code.toUpperCase().trim();
  const snap = await getDoc(doc(db, "groups", code));
  if (!snap.exists()) return { ok: false, msg: "존재하지 않는 그룹 코드입니다." };
  const batch = writeBatch(db);
  batch.update(doc(db, "groups", code), { members: arrayUnion(userId) });
  batch.set(doc(db, "users", userId), { groupCode: code }, { merge: true });
  await batch.commit();
  return { ok: true, group: snap.data() };
}

async function fsLeaveGroup(userId) {
  const userData = await fsGetUser(userId);
  const code = userData?.groupCode;
  if (!code) return;
  const batch = writeBatch(db);
  batch.update(doc(db, "groups", code), { members: arrayRemove(userId) });
  batch.set(doc(db, "users", userId), { groupCode: null }, { merge: true });
  await batch.commit();
}

// ──────────────────────────────────────────
// 4. 실시간 랭킹 리스너
//    그룹 멤버 목록 → 각 유저 XP/레벨 조회 → 정렬
//    callback(rankingArray) 으로 결과 전달
// ──────────────────────────────────────────
let _rankingUnsub = null;   // 기존 리스너 해제용

function listenGroupRanking(groupCode, callback) {
  if (_rankingUnsub) { _rankingUnsub(); _rankingUnsub = null; }

  _rankingUnsub = onSnapshot(doc(db, "groups", groupCode), async (snap) => {
    if (!snap.exists()) return;
    const members = snap.data().members || [];

    // 멤버 유저 데이터 병렬 조회
    const userDocs = await Promise.all(members.map(uid => fsGetUser(uid)));
    const ranking  = userDocs
      .map((u, i) => ({
        uid   : members[i],
        name  : u?.name  || members[i],
        xp    : u?.totalXp || 0,
        level : u?.level   || 1,
        items : u?.weeklyRewardItems || [],
      }))
      .sort((a, b) => b.level - a.level || b.xp - a.xp);

    callback(ranking);
  });
}

function stopListeningRanking() {
  if (_rankingUnsub) { _rankingUnsub(); _rankingUnsub = null; }
}

// ──────────────────────────────────────────
// 5. 주간 보상 (서버 타임스탬프 기준)
// ──────────────────────────────────────────
function getWeekStart() {
  const now  = new Date();
  const day  = now.getDay();
  const diff = now.getDate() - day + (day === 0 ? -6 : 1);
  const mon  = new Date(now.setDate(diff));
  mon.setHours(0, 0, 0, 0);
  return mon.getTime();
}

const WEEKLY_REWARD_ITEMS = {
  rank1: { id: "crown_gold",   name: "👑 황금 왕관",  rarity: "legendary", emoji: "👑", roomEmoji: "👑" },
  rank2: { id: "crown_silver", name: "🥈 은빛 왕관",  rarity: "epic",      emoji: "🥈", roomEmoji: "🥈" },
  rank3: { id: "crown_bronze", name: "🥉 동빛 왕관",  rarity: "rare",      emoji: "🥉", roomEmoji: "🥉" },
};

async function checkAndGrantWeeklyRewards(groupCode) {
  const snap = await getDoc(doc(db, "groups", groupCode));
  if (!snap.exists()) return;
  const group    = snap.data();
  const thisWeek = getWeekStart();
  if ((group.weeklyReset || 0) >= thisWeek) return; // 이미 지급됨

  // 랭킹 1회 계산
  const members  = group.members || [];
  const userDocs = await Promise.all(members.map(uid => fsGetUser(uid)));
  const ranking  = userDocs
    .map((u, i) => ({ uid: members[i], xp: u?.totalXp || 0, level: u?.level || 1 }))
    .sort((a, b) => b.level - a.level || b.xp - a.xp);

  const keys    = ["rank1", "rank2", "rank3"];
  const batch   = writeBatch(db);

  keys.forEach((key, idx) => {
    if (!ranking[idx]) return;
    const item = WEEKLY_REWARD_ITEMS[key];
    batch.set(
      doc(db, "users", ranking[idx].uid),
      { weeklyRewardItems: arrayUnion(item) },
      { merge: true }
    );
  });
  batch.update(doc(db, "groups", groupCode), { weeklyReset: thisWeek });
  await batch.commit();
}

// ──────────────────────────────────────────
// 6. 아이템 정의
// ──────────────────────────────────────────
const SHOP_CONSUMABLES = [
  { id: "apple",     name: "🍎 사과",      desc: "체력 즉시 1 회복",         price: 20,  type: "consumable" },
  { id: "spellbook", name: "📖 비전서",    desc: "던전 문제 스킵 + 피해",     price: 50,  type: "consumable" },
  { id: "sword",     name: "⚔️ 용사의 검", desc: "던전 공격력 +1",           price: 80,  type: "consumable" },
];

const ROOM_ITEMS = [
  { id: "room_table",   name: "🪑 나무 책상",   desc: "클래식한 책상.",         price: 100, emoji: "🪑", type: "room" },
  { id: "room_plant",   name: "🌿 화분",        desc: "싱그러운 초록.",          price: 60,  emoji: "🌿", type: "room" },
  { id: "room_lamp",    name: "🪔 마법 등불",   desc: "마법 불꽃이 타오른다.",   price: 120, emoji: "🪔", type: "room" },
  { id: "room_shield",  name: "🛡️ 훈장 방패",  desc: "전공을 기념하는 방패.",   price: 150, emoji: "🛡️", type: "room" },
  { id: "room_chest",   name: "📦 보물 상자",   desc: "황금빛 보물이 가득.",     price: 200, emoji: "📦", type: "room" },
  { id: "room_trophy",  name: "🏆 우승 트로피", desc: "승리의 빛나는 증표.",     price: 300, emoji: "🏆", type: "room" },
  { id: "room_crystal", name: "🔮 수정 구슬",   desc: "신비로운 마나가 담겨있다.", price: 250, emoji: "🔮", type: "room" },
  { id: "room_book",    name: "📚 마법 서가",   desc: "지식의 보고.",            price: 180, emoji: "📚", type: "room" },
  { id: "room_rug",     name: "🪄 마법 양탄자", desc: "하늘을 나는 느낌.",       price: 220, emoji: "🪄", type: "room" },
  { id: "room_pet",     name: "🐉 미니 드래곤", desc: "훈련된 아기 드래곤.",     price: 500, emoji: "🐉", type: "room" },
  // 랭킹 보상 전용 (상점 미판매 — 자동 지급)
  { id: "crown_gold",   name: "👑 황금 왕관",   desc: "1주일 1위의 영광.",       price: 0,   emoji: "👑", type: "room", rankReward: true },
  { id: "crown_silver", name: "🥈 은빛 왕관",   desc: "1주일 2위 영웅.",         price: 0,   emoji: "🥈", type: "room", rankReward: true },
  { id: "crown_bronze", name: "🥉 동빛 왕관",   desc: "1주일 3위 용사.",         price: 0,   emoji: "🥉", type: "room", rankReward: true },
];

// ──────────────────────────────────────────
// 7. 아이템 인벤토리 (Firestore)
//    users/{userId}.inventory = [ {id, name, emoji, type, ...} ]
// ──────────────────────────────────────────
async function fsGetInventory(userId) {
  const u = await fsGetUser(userId);
  return u?.inventory || [];
}

async function fsGrantItem(userId, item) {
  await setDoc(doc(db, "users", userId),
    { inventory: arrayUnion({ ...item, grantedAt: Date.now() }) },
    { merge: true }
  );
}

async function fsPurchaseRoomItem(userId, itemId, currentPoints) {
  const item = ROOM_ITEMS.find(i => i.id === itemId && !i.rankReward);
  if (!item) return { ok: false, msg: "아이템이 없습니다." };
  if (currentPoints < item.price) return { ok: false, msg: "포인트가 부족합니다." };
  await fsGrantItem(userId, item);
  return { ok: true, cost: item.price };
}

// ──────────────────────────────────────────
// 8. 마이룸 레이아웃 (Firestore)
//    users/{userId}.roomLayout = [ {id, emoji, name} ]
// ──────────────────────────────────────────
async function fsGetRoom(userId) {
  const u = await fsGetUser(userId);
  return u?.roomLayout || [];
}

async function fsPlaceItem(userId, item) {
  const room = await fsGetRoom(userId);
  if (room.find(r => r.id === item.id)) return false;
  await setDoc(doc(db, "users", userId),
    { roomLayout: arrayUnion({ id: item.id, emoji: item.emoji, name: item.name }) },
    { merge: true }
  );
  return true;
}

async function fsRemoveItem(userId, itemId) {
  const room   = await fsGetRoom(userId);
  const target = room.find(r => r.id === itemId);
  if (!target) return;
  await updateDoc(doc(db, "users", userId), { roomLayout: arrayRemove(target) });
}

// ──────────────────────────────────────────
// 9. 진척도 / 포인트 (Firestore)
// ──────────────────────────────────────────
async function fsGetProgress(userId) {
  const u = await fsGetUser(userId);
  return u?.progress || { word:1, grammar:1, writing:1, listening:1, dungeon:1 };
}

async function fsSetProgress(userId, progress) {
  await setDoc(doc(db, "users", userId), { progress }, { merge: true });
}

async function fsGetPoints(userId) {
  const u = await fsGetUser(userId);
  return u?.points || 0;
}

async function fsAddPoints(userId, amount) {
  const cur = await fsGetPoints(userId);
  await setDoc(doc(db, "users", userId), { points: cur + amount }, { merge: true });
  return cur + amount;
}

async function fsSpendPoints(userId, amount) {
  const cur = await fsGetPoints(userId);
  if (cur < amount) return { ok: false };
  await setDoc(doc(db, "users", userId), { points: cur - amount }, { merge: true });
  return { ok: true, remaining: cur - amount };
}

// ── 외부 노출 ──
export {
  db,
  // 레벨 계산
  xpRequiredForLevel, calcLevel, xpProgress, XP_REWARDS,
  // 유저
  fsGetUser, fsSetUser, fsAddXP,
  // 그룹
  fsCreateGroup, fsJoinGroup, fsLeaveGroup,
  // 랭킹
  listenGroupRanking, stopListeningRanking,
  // 주간 보상
  getWeekStart, checkAndGrantWeeklyRewards, WEEKLY_REWARD_ITEMS,
  // 아이템
  SHOP_CONSUMABLES, ROOM_ITEMS,
  fsGetInventory, fsGrantItem, fsPurchaseRoomItem,
  // 마이룸
  fsGetRoom, fsPlaceItem, fsRemoveItem,
  // 진척도 / 포인트
  fsGetProgress, fsSetProgress, fsGetPoints, fsAddPoints, fsSpendPoints,
};
