// ══════════════════════════════════════════════════════════════════════════
// FIRESTORE "DI ESEMPIO" per il portale Richieste di prova.
// Stesse funzioni di firebase-firestore 10.8.1 usate dal portale, ma:
//   • LEGGE i dati veri (richieste, turni, Bibbia...) dal Firebase vero, in sola lettura;
//   • SCRIVE solo in questo browser (localStorage): niente arriva al Firebase vero,
//     ai colleghi o al programma turni. Le prove si vedono sopra ai dati veri.
// "Azzera esempio" (window.esempioReset) cancella le prove fatte su questo telefono.
// ══════════════════════════════════════════════════════════════════════════
import * as R from "https://www.gstatic.com/firebasejs/10.8.1/firebase-firestore.js";

const KEY = 'esempio_richieste_' + location.pathname;
let overlay = {};
try { overlay = JSON.parse(localStorage.getItem(KEY) || '{}') || {}; } catch (e) { overlay = {}; }
const save = () => { try { localStorage.setItem(KEY, JSON.stringify(overlay)); } catch (e) {} };
const realCache = {};            // "col/id" -> dati veri (ultima lettura)
const listeners = new Set();
const DELETE = { __esempioDelete: true };
let seq = Date.now();

const clone = (o) => o === undefined ? undefined : JSON.parse(JSON.stringify(o));
const ovGet = (col, id) => (overlay[col] || {})[id];
function ovSet(col, id, data) { (overlay[col] = overlay[col] || {})[id] = data === null ? { deleted: true } : { data: clone(data) }; save(); notify(col); }
function notify(col) { setTimeout(() => listeners.forEach(l => { if (l.col === col) l.emit(); }), 0); }

export function getFirestore(app) { return { real: R.getFirestore(app) }; }
export function collection(db, name) { return { kind: 'col', col: name, real: R.collection(db.real, name), db }; }
export function doc(db, name, id) { return { kind: 'doc', col: name, id, real: R.doc(db.real, name, id), db }; }
export function where(f, op, v) { return { t: 'where', f, op, v, real: R.where(f, op, v) }; }
export function orderBy(f, dir) { return { t: 'order', f, dir: dir || 'asc', real: R.orderBy(f, dir) }; }
export function limit(n) { return { t: 'limit', n, real: R.limit(n) }; }
export function query(c, ...cons) { return { kind: 'col', col: c.col, cons, real: R.query(c.real, ...cons.map(x => x.real)), db: c.db }; }
export function deleteField() { return DELETE; }

function docSnap(col, id, data) {
    return { id, ref: { kind: 'doc', col, id }, exists: () => data !== undefined, data: () => clone(data) };
}
function getPath(o, path) { return path.split('.').reduce((a, k) => (a == null ? undefined : a[k]), o); }
function merged(col, realDocs, cons) {
    const map = new Map(realDocs.map(d => [d.id, d.data]));
    Object.entries(overlay[col] || {}).forEach(([id, v]) => { if (v.deleted) map.delete(id); else map.set(id, v.data); });
    let list = [...map.entries()].map(([id, data]) => ({ id, data }));
    (cons || []).forEach(c => {
        if (c.t === 'where') list = list.filter(d => {
            const x = getPath(d.data, c.f);
            return c.op === '==' ? x === c.v : c.op === '!=' ? x !== c.v : c.op === 'in' ? (c.v || []).includes(x) : true;
        });
    });
    const ord = (cons || []).filter(c => c.t === 'order');
    if (ord.length) list.sort((a, b) => { for (const o of ord) { const x = getPath(a.data, o.f), y = getPath(b.data, o.f); if (x === y) continue; const r = (x ?? '') < (y ?? '') ? -1 : 1; return o.dir === 'desc' ? -r : r; } return 0; });
    const lim = (cons || []).find(c => c.t === 'limit'); if (lim) list = list.slice(0, lim.n);
    const docs = list.map(d => docSnap(col, d.id, d.data));
    return { docs, size: docs.length, empty: !docs.length, forEach: f => docs.forEach(f) };
}
function cacheReal(col, id, data) { realCache[col + '/' + id] = data; }

export function onSnapshot(ref, cb, errCb) {
    let realDocs = [], realOne;
    const l = { col: ref.col, emit: () => {
        if (ref.kind === 'doc') {
            const o = ovGet(ref.col, ref.id);
            cb(docSnap(ref.col, ref.id, o ? (o.deleted ? undefined : o.data) : realOne));
        } else cb(merged(ref.col, realDocs, ref.cons));
    } };
    listeners.add(l);
    const unsub = R.onSnapshot(ref.real, (snap) => {
        if (ref.kind === 'doc') { realOne = snap.exists() ? snap.data() : undefined; cacheReal(ref.col, ref.id, realOne); }
        else { realDocs = snap.docs.map(d => { const data = d.data(); cacheReal(ref.col, d.id, data); return { id: d.id, data }; }); }
        l.emit();
    }, (e) => { l.emit(); if (errCb) errCb(e); });
    return () => { listeners.delete(l); unsub(); };
}
export async function getDoc(ref) {
    const o = ovGet(ref.col, ref.id);
    if (o) return docSnap(ref.col, ref.id, o.deleted ? undefined : o.data);
    const s = await R.getDoc(ref.real); const data = s.exists() ? s.data() : undefined;
    cacheReal(ref.col, ref.id, data);
    return docSnap(ref.col, ref.id, data);
}
export async function getDocs(q) {
    let realDocs = [];
    try { const s = await R.getDocs(q.real); realDocs = s.docs.map(d => ({ id: d.id, data: d.data() })); } catch (e) { console.warn('Esempio: lettura non riuscita', e); }
    return merged(q.col, realDocs, q.cons);
}
async function currentData(ref) {
    const o = ovGet(ref.col, ref.id);
    if (o) return o.deleted ? undefined : clone(o.data);
    const k = ref.col + '/' + ref.id;
    if (k in realCache) return clone(realCache[k]);
    try { const s = await R.getDoc(ref.real); return s.exists() ? s.data() : undefined; } catch (e) { return undefined; }
}
function applyFields(target, data) {
    Object.entries(data).forEach(([path, v]) => {
        const parts = path.split('.'); let o = target;
        for (let i = 0; i < parts.length - 1; i++) { if (typeof o[parts[i]] !== 'object' || o[parts[i]] === null) o[parts[i]] = {}; o = o[parts[i]]; }
        const last = parts[parts.length - 1];
        if (v === DELETE) delete o[last]; else o[last] = clone(v);
    });
    return target;
}
function deepMerge(a, b) {
    Object.entries(b).forEach(([k, v]) => {
        if (v === DELETE) delete a[k];
        else if (v && typeof v === 'object' && !Array.isArray(v) && a[k] && typeof a[k] === 'object') deepMerge(a[k], v);
        else a[k] = clone(v);
    });
    return a;
}
export async function addDoc(c, data) { const id = 'esempio_' + (seq++).toString(36); ovSet(c.col, id, data); return { id, ...doc(c.db, c.col, id) }; }
export async function setDoc(ref, data, opts) {
    const base = opts && opts.merge ? ((await currentData(ref)) || {}) : {};
    ovSet(ref.col, ref.id, opts && opts.merge ? deepMerge(base, data) : deepMerge({}, data));
}
export async function updateDoc(ref, data) {
    const base = await currentData(ref);
    if (base === undefined) throw new Error('Documento non trovato (esempio)');
    ovSet(ref.col, ref.id, applyFields(base, data));
}
export async function deleteDoc(ref) { ovSet(ref.col, ref.id, null); }

window.esempioReset = function() {
    if (!confirm('Cancellare tutte le prove fatte su questo telefono nel portale di esempio?')) return;
    overlay = {}; save(); location.reload();
};
