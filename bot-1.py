#!/usr/bin/env python3
"""
Telegram Virtual-Credit Games Bot — single-file monolith.

ONLY REQUIRED CONFIG:
    BOT_TOKEN = "PASTE_BOT_TOKEN_HERE"
    ADMIN_IDS = {123456789}

Install:
    pip install "python-telegram-bot[job-queue]>=21,<23"

Run:
    python bot.py

This bot uses NON-REDEEMABLE virtual credits only.
There are no deposits, withdrawals, payments, crypto, cash-out, or
real-money wagering features.

Games:
  Telegram emoji: Dice, Bowling, Football, Basketball
  Custom: Blackjack, Keno, Hi-Lo, Higher/Lower, Red/Black, Plinko,
          Mines, Crash, Ride the Bus, Battleship, Tower, Roulette,
          Diamond Grid

Security design:
  - Telegram numeric IDs are the only user identity.
  - All game state is server-side.
  - Every callback validates ownership.
  - Game settlement is idempotent at the DB layer.
  - Balance changes happen through one transactional ledger function.
  - No client-supplied payout is trusted.
  - Active game state is persisted in SQLite.
  - Secrets use SystemRandom.
  - Admin operations are allowlisted and audited.
  - User/game inputs are bounded and validated.
  - Group moderation ignores ordinary @mentions.
"""

import asyncio
import json
import math
import os
import random
import re
import secrets
import sqlite3
import time
import traceback
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ChatPermissions,
)
from telegram.constants import ChatType
from telegram.error import BadRequest, Forbidden, TelegramError
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ============================================================
# CONFIG — ONLY THESE TWO VALUES NEED TO BE CHANGED
# ============================================================

BOT_TOKEN = "PASTE_BOT_TOKEN_HERE"
ADMIN_IDS = {
    123456789,
}

# ============================================================
# GENERAL CONFIG
# ============================================================

DB_PATH = os.getenv("DB_PATH", "games.sqlite3")
HOUSE_EDGE = 0.02
MIN_BET = 1
MAX_BET = 1_000_000
MAX_COMMAND_ARGS = 8
MAX_TEXT_LENGTH = 1000

# Emoji match modes are 1d1w through 5d5w.
# d is the number of concurrent emoji rolls in each round.
# w is the number of round wins required to win the match.
# Tied rounds are pushes and are not counted.
#
# Example:
#   /bowl 1d2w 5
# means 1 emoji roll per round and 2 round wins required.
#
# The match continues until someone reaches the required number of wins.

EMOJI_TYPES = {
    "dice": "🎲",
    "bowl": "🎳",
    "football": "⚽",
    "basketball": "🏀",
}

# ============================================================
# DATABASE
# ============================================================

DB_LOCK = asyncio.Lock()
USER_LOCKS: dict[int, asyncio.Lock] = {}

SUITS = ["♠", "♥", "♦", "♣"]
RANKS = ["2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K", "A"]


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utcnow().isoformat()


def money(value: int) -> str:
    return f"{int(value):,}"


def safe_int(value: str, minimum: Optional[int] = None,
             maximum: Optional[int] = None) -> Optional[int]:
    try:
        n = int(value)
    except (TypeError, ValueError):
        return None
    if minimum is not None and n < minimum:
        return None
    if maximum is not None and n > maximum:
        return None
    return n


def user_lock(user_id: int) -> asyncio.Lock:
    return USER_LOCKS.setdefault(user_id, asyncio.Lock())


@asynccontextmanager
async def transaction():
    async with DB_LOCK:
        con = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
        con.row_factory = sqlite3.Row
        try:
            con.execute("PRAGMA foreign_keys=ON")
            con.execute("BEGIN IMMEDIATE")
            yield con
            con.execute("COMMIT")
        except Exception:
            try:
                con.execute("ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            con.close()


def open_db():
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON")
    return con


def init_db():
    con = open_db()
    try:
        con.execute("PRAGMA journal_mode=WAL")
        con.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT NOT NULL DEFAULT '',
                first_name TEXT NOT NULL DEFAULT '',
                balance INTEGER NOT NULL DEFAULT 0 CHECK(balance >= 0),
                games INTEGER NOT NULL DEFAULT 0,
                wagered INTEGER NOT NULL DEFAULT 0 CHECK(wagered >= 0),
                payouts INTEGER NOT NULL DEFAULT 0 CHECK(payouts >= 0),
                losses INTEGER NOT NULL DEFAULT 0 CHECK(losses >= 0),
                warnings INTEGER NOT NULL DEFAULT 0 CHECK(warnings >= 0),
                mutes INTEGER NOT NULL DEFAULT 0 CHECK(mutes >= 0),
                banned INTEGER NOT NULL DEFAULT 0 CHECK(banned IN (0,1)),
                muted_until TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                kind TEXT NOT NULL,
                amount INTEGER NOT NULL,
                balance_after INTEGER NOT NULL CHECK(balance_after >= 0),
                game TEXT,
                ref TEXT,
                note TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            );

            CREATE TABLE IF NOT EXISTS games (
                id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                game TEXT NOT NULL,
                bet INTEGER NOT NULL CHECK(bet >= 0),
                payout INTEGER NOT NULL DEFAULT 0 CHECK(payout >= 0),
                result TEXT NOT NULL DEFAULT '',
                state TEXT NOT NULL DEFAULT '{}',
                status TEXT NOT NULL DEFAULT 'active'
                    CHECK(status IN ('active','finished','cancelled')),
                created_at TEXT NOT NULL,
                ended_at TEXT,
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            );

            CREATE TABLE IF NOT EXISTS admin_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER NOT NULL,
                action TEXT NOT NULL,
                target_id INTEGER,
                details TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS moderation_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                action TEXT NOT NULL,
                reason TEXT NOT NULL DEFAULT '',
                admin_id INTEGER,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_games_user_created
                ON games(user_id, created_at DESC);

            CREATE INDEX IF NOT EXISTS idx_ledger_user_created
                ON ledger(user_id, created_at DESC);

            CREATE INDEX IF NOT EXISTS idx_admin_log_created
                ON admin_log(created_at DESC);
            """
        )
        con.commit()
    finally:
        con.close()


async def ensure_user(tg_user):
    now = iso_now()
    async with transaction() as con:
        row = con.execute(
            "SELECT user_id FROM users WHERE user_id=?",
            (tg_user.id,),
        ).fetchone()
        if row:
            con.execute(
                """
                UPDATE users
                SET username=?, first_name=?, updated_at=?
                WHERE user_id=?
                """,
                (
                    tg_user.username or "",
                    (tg_user.first_name or "")[:128],
                    now,
                    tg_user.id,
                ),
            )
        else:
            # Deliberately ZERO. No free balance.
            con.execute(
                """
                INSERT INTO users(
                    user_id,username,first_name,balance,created_at,updated_at
                ) VALUES(?,?,?,?,?,?)
                """,
                (
                    tg_user.id,
                    (tg_user.username or "")[:64],
                    (tg_user.first_name or "")[:128],
                    0,
                    now,
                    now,
                ),
            )


async def get_user(user_id: int):
    async with DB_LOCK:
        con = open_db()
        try:
            return con.execute(
                "SELECT * FROM users WHERE user_id=?",
                (user_id,),
            ).fetchone()
        finally:
            con.close()


async def write_admin_log(admin_id: int, action: str,
                          target_id: Optional[int] = None,
                          details: str = ""):
    async with transaction() as con:
        con.execute(
            """
            INSERT INTO admin_log(admin_id,action,target_id,details,created_at)
            VALUES(?,?,?,?,?)
            """,
            (admin_id, action[:100], target_id, details[:2000], iso_now()),
        )


async def ledger_change(
    user_id: int,
    amount: int,
    kind: str,
    game: Optional[str] = None,
    ref: Optional[str] = None,
    note: Optional[str] = None,
) -> int:
    """
    The only balance mutation path.

    The balance and ledger row are committed in ONE transaction.
    A negative balance is impossible at the database boundary.
    """
    if not isinstance(amount, int):
        raise ValueError("Amount must be an integer.")

    async with transaction() as con:
        row = con.execute(
            "SELECT balance FROM users WHERE user_id=?",
            (user_id,),
        ).fetchone()
        if not row:
            raise ValueError("User does not exist.")

        new_balance = row["balance"] + amount
        if new_balance < 0:
            raise ValueError("Insufficient balance.")

        con.execute(
            """
            UPDATE users
            SET balance=?,updated_at=?
            WHERE user_id=?
            """,
            (new_balance, iso_now(), user_id),
        )

        con.execute(
            """
            INSERT INTO ledger(
                user_id,kind,amount,balance_after,game,ref,note,created_at
            ) VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                user_id,
                kind[:50],
                amount,
                new_balance,
                game[:50] if game else None,
                ref[:100] if ref else None,
                (note or "")[:2000],
                iso_now(),
            ),
        )
        return new_balance


async def create_game(user_id: int, game: str, bet: int, state: dict):
    if not isinstance(bet, int) or not (MIN_BET <= bet <= MAX_BET):
        raise ValueError("Invalid wager.")

    async with transaction() as con:
        row = con.execute(
            "SELECT balance FROM users WHERE user_id=?",
            (user_id,),
        ).fetchone()
        if not row:
            raise ValueError("User does not exist.")
        if row["balance"] < bet:
            raise ValueError("Insufficient balance.")

        gid = secrets.token_hex(16)
        now = iso_now()

        new_balance = row["balance"] - bet
        con.execute(
            "UPDATE users SET balance=?,games=games+1,wagered=wagered+?,updated_at=? "
            "WHERE user_id=?",
            (new_balance, bet, now, user_id),
        )
        con.execute(
            """
            INSERT INTO ledger(
                user_id,kind,amount,balance_after,game,ref,note,created_at
            ) VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                user_id,
                "wager",
                -bet,
                new_balance,
                game,
                gid,
                "Game wager",
                now,
            ),
        )
        con.execute(
            """
            INSERT INTO games(
                id,user_id,game,bet,state,status,created_at
            ) VALUES(?,?,?,?,?,'active',?)
            """,
            (
                gid,
                user_id,
                game,
                bet,
                json.dumps(state, separators=(",", ":")),
                now,
            ),
        )
        return gid


async def finish_game(
    gid: str,
    user_id: int,
    payout: int,
    result: str,
    state: dict,
):
    """
    Idempotent settlement.

    UPDATE status='finished' is conditional on status='active'.
    If the game was already settled, no second payout is possible.
    """
    payout = max(0, int(payout))
    async with transaction() as con:
        game = con.execute(
            "SELECT * FROM games WHERE id=? AND user_id=?",
            (gid, user_id),
        ).fetchone()
        if not game:
            raise ValueError("Game not found.")
        if game["status"] != "active":
            return False

        now = iso_now()

        if payout:
            user = con.execute(
                "SELECT balance FROM users WHERE user_id=?",
                (user_id,),
            ).fetchone()
            new_balance = user["balance"] + payout

            con.execute(
                """
                UPDATE users
                SET balance=?,payouts=payouts+?,updated_at=?
                WHERE user_id=?
                """,
                (new_balance, payout, now, user_id),
            )
            con.execute(
                """
                INSERT INTO ledger(
                    user_id,kind,amount,balance_after,game,ref,note,created_at
                ) VALUES(?,?,?,?,?,?,?,?)
                """,
                (
                    user_id,
                    "payout",
                    payout,
                    new_balance,
                    game["game"],
                    gid,
                    result[:500],
                    now,
                ),
            )
        else:
            con.execute(
                """
                UPDATE users
                SET losses=losses+?,updated_at=?
                WHERE user_id=?
                """,
                (game["bet"], now, user_id),
            )

        con.execute(
            """
            UPDATE games
            SET payout=?,result=?,state=?,status='finished',ended_at=?
            WHERE id=? AND status='active'
            """,
            (
                payout,
                result[:500],
                json.dumps(state, separators=(",", ":")),
                now,
                gid,
            ),
        )
        return True


async def cancel_game_refund(gid: str, user_id: int, reason: str):
    """
    Used only for recoverable games after restart/expiry.
    Refunds exactly once because status must still be active.
    """
    async with transaction() as con:
        game = con.execute(
            "SELECT * FROM games WHERE id=? AND user_id=?",
            (gid, user_id),
        ).fetchone()
        if not game or game["status"] != "active":
            return False

        user = con.execute(
            "SELECT balance FROM users WHERE user_id=?",
            (user_id,),
        ).fetchone()
        new_balance = user["balance"] + game["bet"]
        now = iso_now()

        con.execute(
            "UPDATE users SET balance=?,updated_at=? WHERE user_id=?",
            (new_balance, now, user_id),
        )
        con.execute(
            """
            INSERT INTO ledger(
                user_id,kind,amount,balance_after,game,ref,note,created_at
            ) VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                user_id,
                "refund",
                game["bet"],
                new_balance,
                game["game"],
                gid,
                reason[:500],
                now,
            ),
        )
        con.execute(
            """
            UPDATE games
            SET status='cancelled',result=?,ended_at=?
            WHERE id=? AND status='active'
            """,
            (reason[:500], now, gid),
        )
        return True


async def active_games_for_user(user_id: int):
    async with DB_LOCK:
        con = open_db()
        try:
            return con.execute(
                "SELECT * FROM games WHERE user_id=? AND status='active'",
                (user_id,),
            ).fetchall()
        finally:
            con.close()


# ============================================================
# GAME HELPERS
# ============================================================

def cards_deck():
    return [(rank, suit) for suit in SUITS for rank in RANKS]


def card_value(hand):
    total = 0
    aces = 0
    for rank, _suit in hand:
        if rank in ("J", "Q", "K"):
            total += 10
        elif rank == "A":
            total += 11
            aces += 1
        else:
            total += int(rank)

    while total > 21 and aces:
        total -= 10
        aces -= 1
    return total


def card_text(card):
    return f"{card[0]}{card[1]}"


def emoji_match_payout(bet: int, required_wins: int) -> int:
    # Conservative virtual-credit multipliers with a 2% house edge.
    # Higher required wins = higher risk.
    fair = {
        1: 1.92,
        2: 2.85,
        3: 4.20,
        4: 6.20,
        5: 9.00,
    }[required_wins]
    return max(bet, int(bet * fair * (1 - HOUSE_EDGE)))


def multiplier_payout(bet: int, multiplier: float) -> int:
    return max(0, int(math.floor(bet * multiplier * (1 - HOUSE_EDGE))))


# ============================================================
# MENUS
# ============================================================

def home_markup():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🎲 Dice", callback_data="menu:dice"),
            InlineKeyboardButton("🎳 Bowling", callback_data="menu:bowl"),
        ],
        [
            InlineKeyboardButton("⚽ Football", callback_data="menu:football"),
            InlineKeyboardButton("🏀 Basketball", callback_data="menu:basketball"),
        ],
        [
            InlineKeyboardButton("🃏 Blackjack", callback_data="menu:blackjack"),
            InlineKeyboardButton("🎟 Keno", callback_data="menu:keno"),
        ],
        [
            InlineKeyboardButton("🔺 Hi-Lo", callback_data="menu:hilo"),
            InlineKeyboardButton("⬆️ Higher/Lower", callback_data="menu:higherlower"),
        ],
        [
            InlineKeyboardButton("🔴 Red/Black", callback_data="menu:redblack"),
            InlineKeyboardButton("🔺 Plinko", callback_data="menu:plinko"),
        ],
        [
            InlineKeyboardButton("💣 Mines", callback_data="menu:mines"),
            InlineKeyboardButton("💥 Crash", callback_data="menu:crash"),
        ],
        [
            InlineKeyboardButton("🚌 Ride the Bus", callback_data="menu:bus"),
            InlineKeyboardButton("🚢 Battleship", callback_data="menu:battleship"),
        ],
        [
            InlineKeyboardButton("🏰 Tower", callback_data="menu:tower"),
            InlineKeyboardButton("🎡 Roulette", callback_data="menu:roulette"),
        ],
        [
            InlineKeyboardButton("💎 Diamond Grid", callback_data="menu:diamonds"),
        ],
        [
            InlineKeyboardButton("💰 Balance", callback_data="menu:balance"),
            InlineKeyboardButton("📜 History", callback_data="menu:history"),
        ],
    ])


def back_markup():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Games", callback_data="home")]
    ])


# ============================================================
# USER COMMANDS
# ============================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_user(update.effective_user)
    if await is_blocked(update.effective_user.id):
        return
    await update.message.reply_text(
        "🎮 Virtual Credit Games\n\n"
        "Balance starts at 0. An administrator must credit an account.\n"
        "Use /games to open the game menu.",
        reply_markup=home_markup(),
    )


async def games_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_user(update.effective_user)
    if await is_blocked(update.effective_user.id):
        return
    await update.message.reply_text("🎮 Choose a game:", reply_markup=home_markup())


async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_user(update.effective_user)
    if await is_blocked(update.effective_user.id):
        return
    row = await get_user(update.effective_user.id)
    await update.message.reply_text(
        f"💰 Balance: {money(row['balance'])} credits"
    )


async def history_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_user(update.effective_user)
    if await is_blocked(update.effective_user.id):
        return
    rows = await query_games(update.effective_user.id, 15)
    if not rows:
        await update.message.reply_text("📜 No games yet.")
        return
    lines = ["📜 Last 15 games"]
    for r in rows:
        lines.append(
            f"{r['game']} | bet {money(r['bet'])} | "
            f"payout {money(r['payout'])} | {r['result']}"
        )
    await update.message.reply_text("\n".join(lines))


async def query_games(user_id, limit=15):
    limit = max(1, min(50, limit))
    async with DB_LOCK:
        con = open_db()
        try:
            return con.execute(
                """
                SELECT game,bet,payout,result,created_at
                FROM games
                WHERE user_id=?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (user_id, limit),
            ).fetchall()
        finally:
            con.close()


async def is_blocked(user_id: int) -> bool:
    row = await get_user(user_id)
    if not row:
        return False
    if row["banned"]:
        return True
    if row["muted_until"]:
        try:
            until = datetime.fromisoformat(row["muted_until"])
            if until > utcnow():
                return True
        except ValueError:
            pass
    return False


# ============================================================
# EMOJI MATCH
# ============================================================

async def emoji_command(update: Update, context: ContextTypes.DEFAULT_TYPE, kind: str):
    await ensure_user(update.effective_user)
    uid = update.effective_user.id
    if await is_blocked(uid):
        return

    if len(context.args) != 2:
        await update.message.reply_text(
            f"Usage: /{kind} 1d2w 5\n"
            "Modes: 1d1w through 5d5w."
        )
        return

    mode = context.args[0].lower()
    bet = safe_int(context.args[1], MIN_BET, MAX_BET)

    match = re.fullmatch(r"([1-5])d([1-5])w", mode)
    if not match or bet is None:
        await update.message.reply_text(
            f"Usage: /{kind} 1d2w 5\n"
            "Modes: 1d1w through 5d5w."
        )
        return

    dice_count = int(match.group(1))
    wins_required = int(match.group(2))

    row = await get_user(uid)
    if row["balance"] < bet:
        await update.message.reply_text("Insufficient balance.")
        return

    async with user_lock(uid):
        try:
            gid = await create_game(
                uid,
                kind,
                bet,
                {
                    "mode": mode,
                    "dice_count": dice_count,
                    "wins_required": wins_required,
                    "player_wins": 0,
                    "opponent_wins": 0,
                    "rounds": 0,
                },
            )
        except ValueError as e:
            await update.message.reply_text(str(e))
            return

        player_wins = 0
        opponent_wins = 0
        rounds = 0

        # No arbitrary round cap: ties do not count and the match ends
        # exactly when one side reaches the configured win target.
        while player_wins < wins_required and opponent_wins < wins_required:
            rounds += 1
            player_values = []
            opponent_values = []

            for _ in range(dice_count):
                p = await update.message.reply_dice(emoji=EMOJI_TYPES[kind])
                o = await update.message.reply_dice(emoji=EMOJI_TYPES[kind])
                player_values.append(p.dice.value)
                opponent_values.append(o.dice.value)

            p_score = sum(player_values)
            o_score = sum(opponent_values)

            if p_score > o_score:
                player_wins += 1
            elif o_score > p_score:
                opponent_wins += 1
            # Equal scores = push. No win is added.

            # Persist progress so an interrupted process can be audited.
            await update_game_state(
                gid,
                {
                    "mode": mode,
                    "dice_count": dice_count,
                    "wins_required": wins_required,
                    "player_wins": player_wins,
                    "opponent_wins": opponent_wins,
                    "rounds": rounds,
                },
            )

        won = player_wins >= wins_required
        payout = emoji_match_payout(bet, wins_required) if won else 0
        result = f"{player_wins}-{opponent_wins}; {rounds} rounds"

        await finish_game(
            gid,
            uid,
            payout,
            "win " + result if won else "loss " + result,
            {
                "mode": mode,
                "dice_count": dice_count,
                "wins_required": wins_required,
                "player_wins": player_wins,
                "opponent_wins": opponent_wins,
                "rounds": rounds,
            },
        )

        row = await get_user(uid)
        await update.message.reply_text(
            f"{EMOJI_TYPES[kind]} {kind.title()} • {mode}\n"
            f"Final: {player_wins}-{opponent_wins}\n"
            f"Rounds: {rounds}\n"
            f"Bet: {money(bet)}\n"
            f"Payout: {money(payout)}\n"
            f"Balance: {money(row['balance'])}"
        )


async def update_game_state(gid: str, state: dict):
    async with transaction() as con:
        con.execute(
            "UPDATE games SET state=? WHERE id=? AND status='active'",
            (json.dumps(state, separators=(",", ":")), gid),
        )


# ============================================================
# BLACKJACK
# ============================================================

async def blackjack_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_user(update.effective_user)
    uid = update.effective_user.id
    if await is_blocked(uid):
        return

    bet = safe_int(context.args[0], MIN_BET, MAX_BET) if context.args else 10
    row = await get_user(uid)
    if bet is None or row["balance"] < bet:
        await update.message.reply_text("Invalid bet or insufficient balance.")
        return

    async with user_lock(uid):
        # Refuse concurrent active games of the same user.
        if await active_games_for_user(uid):
            await update.message.reply_text(
                "You already have an active game. Finish it first."
            )
            return

        d = cards_deck()
        random.SystemRandom().shuffle(d)
        player = [d.pop(), d.pop()]
        dealer = [d.pop(), d.pop()]

        gid = await create_game(
            uid,
            "blackjack",
            bet,
            {
                "player": player,
                "dealer": dealer,
                "deck": d,
            },
        )

        if card_value(player) == 21:
            payout = multiplier_payout(bet, 2.94)
            await finish_game(
                gid, uid, payout, "natural blackjack",
                {"player": player, "dealer": dealer},
            )
            await update.message.reply_text(
                f"🃏 BLACKJACK\n"
                f"Your: {' '.join(map(card_text, player))} = 21\n"
                f"Dealer: {' '.join(map(card_text, dealer))}\n"
                f"Payout: {money(payout)}"
            )
            return

        await update_game_state(
            gid,
            {"player": player, "dealer": dealer, "deck": d},
        )

        await update.message.reply_text(
            f"🃏 BLACKJACK\n"
            f"Your: {' '.join(map(card_text, player))} = {card_value(player)}\n"
            f"Dealer: {card_text(dealer[0])} + ❓",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("Hit", callback_data=f"bj:{gid}:hit"),
                    InlineKeyboardButton("Stand", callback_data=f"bj:{gid}:stand"),
                ],
            ]),
        )


async def blackjack_action(query, uid, gid, action):
    game = await load_owned_active_game(gid, uid)
    if not game:
        await query.answer("Game is no longer active.", show_alert=True)
        return

    state = json.loads(game["state"])
    player = [tuple(x) for x in state["player"]]
    dealer = [tuple(x) for x in state["dealer"]]
    d = [tuple(x) for x in state["deck"]]

    if action == "hit":
        if not d:
            await query.answer("Deck exhausted.", show_alert=True)
            return
        player.append(d.pop())
        value = card_value(player)

        await update_game_state(
            gid,
            {"player": player, "dealer": dealer, "deck": d},
        )

        if value > 21:
            await finish_game(
                gid, uid, 0, "bust",
                {"player": player, "dealer": dealer, "deck": d},
            )
            await query.edit_message_text(
                f"💥 Bust: {' '.join(map(card_text, player))} = {value}"
            )
            return

        await query.edit_message_text(
            f"🃏 Your: {' '.join(map(card_text, player))} = {value}\n"
            f"Dealer: {card_text(dealer[0])} + ❓",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("Hit", callback_data=f"bj:{gid}:hit"),
                    InlineKeyboardButton("Stand", callback_data=f"bj:{gid}:stand"),
                ],
            ]),
        )
        return

    while card_value(dealer) < 17:
        if not d:
            break
        dealer.append(d.pop())

    pv = card_value(player)
    dv = card_value(dealer)

    if dv > 21 or pv > dv:
        payout = multiplier_payout(game["bet"], 1.96)
        result = "win"
    elif pv == dv:
        payout = game["bet"]
        result = "push"
    else:
        payout = 0
        result = "loss"

    await finish_game(
        gid,
        uid,
        payout,
        result,
        {"player": player, "dealer": dealer, "deck": d},
    )

    await query.edit_message_text(
        f"🃏 BLACKJACK\n"
        f"Your: {' '.join(map(card_text, player))} = {pv}\n"
        f"Dealer: {' '.join(map(card_text, dealer))} = {dv}\n"
        f"Result: {result}\nPayout: {money(payout)}"
    )


# ============================================================
# KENO
# ============================================================

def keno_markup(chosen: set[int]):
    rows = []
    for start in range(1, 41, 5):
        rows.append([
            InlineKeyboardButton(
                f"{'✅' if n in chosen else ''}{n}",
                callback_data=f"keno:{n}",
            )
            for n in range(start, start + 5)
        ])
    rows.append([
        InlineKeyboardButton("🎟 DRAW", callback_data="keno:draw")
    ])
    return InlineKeyboardMarkup(rows)


async def keno_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_user(update.effective_user)
    uid = update.effective_user.id
    if await is_blocked(uid):
        return

    bet = safe_int(context.args[0], MIN_BET, MAX_BET) if context.args else 10
    row = await get_user(uid)
    if bet is None or row["balance"] < bet:
        await update.message.reply_text("Invalid bet or insufficient balance.")
        return

    if await active_games_for_user(uid):
        await update.message.reply_text("Finish your current game first.")
        return

    gid = await create_game(uid, "keno", bet, {"chosen": []})
    state = {
        "gid": gid,
        "bet": bet,
        "chosen": set(),
    }

    # Keno's random draw is generated only at draw time, not exposed to user.
    await update.message.reply_text(
        "🎟 KENO\nSelect exactly 10 numbers.",
        reply_markup=keno_markup(set()),
    )
    # The active UI state is persisted in the game DB. No payout data is client-side.
    await update_game_state(gid, {"chosen": []})


# ============================================================
# CARD GUESS GAMES
# ============================================================

async def card_guess_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    kind: str,
):
    await ensure_user(update.effective_user)
    uid = update.effective_user.id
    if await is_blocked(uid):
        return

    bet = safe_int(context.args[0], MIN_BET, MAX_BET) if context.args else 10
    row = await get_user(uid)

    if bet is None or row["balance"] < bet:
        await update.message.reply_text("Invalid bet or insufficient balance.")
        return

    if await active_games_for_user(uid):
        await update.message.reply_text("Finish your current game first.")
        return

    current = random.choice(cards_deck())
    gid = await create_game(uid, kind, bet, {"current": current})

    await update_game_state(
        gid,
        {"current": current},
    )

    await update.message.reply_text(
        f"{'🔺 HI-LO' if kind == 'hilo' else '⬆️ HIGHER / LOWER'}\n"
        f"Card: {card_text(current)}\n"
        "Choose your prediction:",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("Higher", callback_data=f"cg:{gid}:higher"),
                InlineKeyboardButton("Lower", callback_data=f"cg:{gid}:lower"),
            ],
        ]),
    )


# ============================================================
# RED / BLACK
# ============================================================

async def redblack_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_user(update.effective_user)
    uid = update.effective_user.id
    if await is_blocked(uid):
        return

    bet = safe_int(context.args[0], MIN_BET, MAX_BET) if context.args else 10
    row = await get_user(uid)

    if bet is None or row["balance"] < bet:
        await update.message.reply_text("Invalid bet or insufficient balance.")
        return

    if await active_games_for_user(uid):
        await update.message.reply_text("Finish your current game first.")
        return

    gid = await create_game(uid, "redblack", bet, {})
    await update_game_state(gid, {"choice": None})

    await update.message.reply_text(
        "🔴 RED / BLACK\nChoose:",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🔴 Red", callback_data=f"rb:{gid}:red"),
                InlineKeyboardButton("⚫ Black", callback_data=f"rb:{gid}:black"),
            ],
        ]),
    )


# ============================================================
# DICE ROULETTE
# ============================================================

async def dice_roulette_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_user(update.effective_user)
    uid = update.effective_user.id
    if await is_blocked(uid):
        return

    bet = safe_int(context.args[0], MIN_BET, MAX_BET) if context.args else 10
    row = await get_user(uid)

    if bet is None or row["balance"] < bet:
        await update.message.reply_text("Invalid bet or insufficient balance.")
        return

    if await active_games_for_user(uid):
        await update.message.reply_text("Finish your current game first.")
        return

    gid = await create_game(uid, "diceroulette", bet, {})
    await update_game_state(gid, {"choice": None})

    await update.message.reply_text(
        "🎲 DICE ROULETTE\nChoose:",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("1", callback_data=f"dr:{gid}:n:1"),
                InlineKeyboardButton("2", callback_data=f"dr:{gid}:n:2"),
                InlineKeyboardButton("3", callback_data=f"dr:{gid}:n:3"),
                InlineKeyboardButton("4", callback_data=f"dr:{gid}:n:4"),
                InlineKeyboardButton("5", callback_data=f"dr:{gid}:n:5"),
                InlineKeyboardButton("6", callback_data=f"dr:{gid}:n:6"),
            ],
            [
                InlineKeyboardButton("High", callback_data=f"dr:{gid}:high"),
                InlineKeyboardButton("Low", callback_data=f"dr:{gid}:low"),
            ],
            [
                InlineKeyboardButton("Odd", callback_data=f"dr:{gid}:odd"),
                InlineKeyboardButton("Even", callback_data=f"dr:{gid}:even"),
            ],
        ]),
    )


# ============================================================
# PLINKO
# ============================================================

async def plinko_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_user(update.effective_user)
    uid = update.effective_user.id
    if await is_blocked(uid):
        return

    bet = safe_int(context.args[0], MIN_BET, MAX_BET) if context.args else 10
    row = await get_user(uid)
    if bet is None or row["balance"] < bet:
        await update.message.reply_text("Invalid bet or insufficient balance.")
        return

    async with user_lock(uid):
        if await active_games_for_user(uid):
            await update.message.reply_text("Finish your current game first.")
            return

        path = "".join(secrets.choice(("L", "R")) for _ in range(10))
        displacement = abs(path.count("R") - path.count("L"))

        multipliers = {
            0: 1.00,
            2: 1.50,
            4: 2.00,
            6: 3.00,
            8: 5.00,
            10: 8.00,
        }
        multiplier = multipliers[displacement]
        payout = multiplier_payout(bet, multiplier)

        gid = await create_game(
            uid,
            "plinko",
            bet,
            {"path": path, "multiplier": multiplier},
        )
        await finish_game(
            gid,
            uid,
            payout,
            f"{multiplier:.2f}x",
            {"path": path, "multiplier": multiplier},
        )

        await update.message.reply_text(
            f"🔺 PLINKO\n"
            f"Path: {path}\n"
            f"Multiplier: {multiplier:.2f}x\n"
            f"Payout: {money(payout)}"
        )


# ============================================================
# MINES / DIAMOND GRID
# ============================================================

def mines_markup(state: dict):
    size = state["size"]
    revealed = set(state["revealed"])
    buttons = []

    for r in range(size):
        row = []
        for c in range(size):
            i = r * size + c
            row.append(
                InlineKeyboardButton(
                    "💎" if i in revealed else "⬜",
                    callback_data=f"mine:{state['gid']}:{i}",
                )
            )
        buttons.append(row)

    buttons.append([
        InlineKeyboardButton(
            "💰 CASH OUT",
            callback_data=f"mine:{state['gid']}:cash",
        )
    ])
    return InlineKeyboardMarkup(buttons)


async def mines_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    diamonds=False,
):
    await ensure_user(update.effective_user)
    uid = update.effective_user.id
    if await is_blocked(uid):
        return

    args = context.args
    bet = safe_int(args[0], MIN_BET, MAX_BET) if args else 10
    size = safe_int(args[1], 3, 7) if len(args) > 1 else 5
    mine_count = safe_int(args[2], 1, MINES_MAX) if len(args) > 2 else 3

    row = await get_user(uid)
    if bet is None or row["balance"] < bet:
        await update.message.reply_text("Invalid bet or insufficient balance.")
        return

    total = size * size
    if mine_count is None or mine_count > min(MINES_MAX, total - 1):
        await update.message.reply_text(
            f"Mines must be 1–{min(MINES_MAX, total - 1)} "
            "and at least one safe square must remain."
        )
        return

    if await active_games_for_user(uid):
        await update.message.reply_text("Finish your current game first.")
        return

    game_name = "diamonds" if diamonds else "mines"

    mine_positions = set(
        secrets.SystemRandom().sample(range(total), mine_count)
    )

    gid = await create_game(
        uid,
        game_name,
        bet,
        {
            "size": size,
            "mine_count": mine_count,
            "revealed": [],
            "safe_count": 0,
            # The hidden mine positions are persisted server-side.
            "mines": sorted(mine_positions),
        },
    )

    state = {
        "gid": gid,
        "size": size,
        "mine_count": mine_count,
        "mines": mine_positions,
        "revealed": set(),
        "safe_count": 0,
        "bet": bet,
        "game": game_name,
    }

    await update_message_mines(
        update.message,
        state,
        first=True,
    )


async def update_message_mines(message, state, first=False):
    await message.reply_text(
        (
            "💎 DIAMOND GRID" if state["game"] == "diamonds"
            else "💣 MINES"
        )
        + f"\nGrid: {state['size']}×{state['size']}"
        + f"\nMines: {state['mine_count']}"
        + f"\nSafe picks: {state['safe_count']}",
        reply_markup=mines_markup(state),
    )

    # Persist only safe public state plus hidden mines in DB.
    await update_game_state(
        state["gid"],
        {
            "size": state["size"],
            "mine_count": state["mine_count"],
            "revealed": sorted(state["revealed"]),
            "safe_count": state["safe_count"],
            "mines": sorted(state["mines"]),
        },
    )


# ============================================================
# CRASH
# ============================================================

async def crash_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_user(update.effective_user)
    uid = update.effective_user.id
    if await is_blocked(uid):
        return

    bet = safe_int(context.args[0], MIN_BET, MAX_BET) if context.args else 10
    row = await get_user(uid)
    if bet is None or row["balance"] < bet:
        await update.message.reply_text("Invalid bet or insufficient balance.")
        return

    if await active_games_for_user(uid):
        await update.message.reply_text("Finish your current game first.")
        return

    # Crash point generated once and persisted. It is never accepted from client.
    r = secrets.SystemRandom().random()
    crash_point = max(1.01, min(1000.0, (1.0 / max(r, 1e-9)) ** 0.45))
    crash_point = round(crash_point, 2)

    gid = await create_game(
        uid,
        "crash",
        bet,
        {"crash_point": crash_point, "current": 1.0},
    )

    await update_message_crash(
        update.message,
        gid,
        bet,
        crash_point,
        1.0,
    )


async def update_message_crash(message, gid, bet, crash_point, current):
    await message.reply_text(
        f"💥 CRASH\n"
        f"Current: {current:.2f}x\n"
        f"Bet: {money(bet)}\n"
        "Cash out before the hidden crash point.",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    "🚀 CASH OUT",
                    callback_data=f"crash:{gid}",
                )
            ],
        ]),
    )


# ============================================================
# TOWER
# ============================================================

TOWER_CHOICES = {
    "low": 4,
    "medium": 3,
    "high": 2,
}

TOWER_STEP = {
    "low": 1.30,
    "medium": 1.65,
    "high": 2.20,
}


def tower_markup(gid, choices):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                str(i + 1),
                callback_data=f"tower:{gid}:{i}",
            )
            for i in range(choices)
        ],
        [
            InlineKeyboardButton(
                "💰 CASH OUT",
                callback_data=f"tower:{gid}:cash",
            )
        ],
    ])


async def tower_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_user(update.effective_user)
    uid = update.effective_user.id
    if await is_blocked(uid):
        return

    bet = safe_int(context.args[0], MIN_BET, MAX_BET) if context.args else 10
    difficulty = context.args[1].lower() if len(context.args) > 1 else "medium"

    row = await get_user(uid)
    if bet is None or row["balance"] < bet:
        await update.message.reply_text("Invalid bet or insufficient balance.")
        return

    if difficulty not in TOWER_CHOICES:
        await update.message.reply_text("Difficulty: low, medium, or high.")
        return

    if await active_games_for_user(uid):
        await update.message.reply_text("Finish your current game first.")
        return

    choices = TOWER_CHOICES[difficulty]

    gid = await create_game(
        uid,
        "tower",
        bet,
        {
            "difficulty": difficulty,
            "choices": choices,
            "floor": 0,
            "multiplier": 1.0,
        },
    )

    await update.message.reply_text(
        f"🏰 TOWER • {difficulty.upper()}\n"
        f"Choices per floor: {choices}\n"
        "Choose a tile.",
        reply_markup=tower_markup(gid, choices),
    )


# ============================================================
# ROULETTE
# ============================================================

ROULETTE_RED = {
    1,3,5,7,9,12,14,16,18,19,21,23,25,27,30,32,34,36
}


async def roulette_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_user(update.effective_user)
    uid = update.effective_user.id
    if await is_blocked(uid):
        return

    bet = safe_int(context.args[0], MIN_BET, MAX_BET) if context.args else 10
    row = await get_user(uid)

    if bet is None or row["balance"] < bet:
        await update.message.reply_text("Invalid bet or insufficient balance.")
        return

    if await active_games_for_user(uid):
        await update.message.reply_text("Finish your current game first.")
        return

    gid = await create_game(uid, "roulette", bet, {})
    await update_message_roulette(update.message, gid)


async def update_message_roulette(message, gid):
    await message.reply_text(
        "🎡 ROULETTE\nChoose a bet:",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("0", callback_data=f"roulette:{gid}:n:0"),
                InlineKeyboardButton("1", callback_data=f"roulette:{gid}:n:1"),
                InlineKeyboardButton("2", callback_data=f"roulette:{gid}:n:2"),
                InlineKeyboardButton("3", callback_data=f"roulette:{gid}:n:3"),
                InlineKeyboardButton("4", callback_data=f"roulette:{gid}:n:4"),
                InlineKeyboardButton("5", callback_data=f"roulette:{gid}:n:5"),
            ],
            [
                InlineKeyboardButton("6", callback_data=f"roulette:{gid}:n:6"),
                InlineKeyboardButton("7", callback_data=f"roulette:{gid}:n:7"),
                InlineKeyboardButton("8", callback_data=f"roulette:{gid}:n:8"),
                InlineKeyboardButton("9", callback_data=f"roulette:{gid}:n:9"),
                InlineKeyboardButton("10", callback_data=f"roulette:{gid}:n:10"),
                InlineKeyboardButton("11", callback_data=f"roulette:{gid}:n:11"),
            ],
            [
                InlineKeyboardButton("Red", callback_data=f"roulette:{gid}:red"),
                InlineKeyboardButton("Black", callback_data=f"roulette:{gid}:black"),
            ],
            [
                InlineKeyboardButton("Odd", callback_data=f"roulette:{gid}:odd"),
                InlineKeyboardButton("Even", callback_data=f"roulette:{gid}:even"),
            ],
        ]),
    )


# ============================================================
# RIDE THE BUS
# ============================================================

async def bus_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_user(update.effective_user)
    uid = update.effective_user.id
    if await is_blocked(uid):
        return

    bet = safe_int(context.args[0], MIN_BET, MAX_BET) if context.args else 10
    row = await get_user(uid)

    if bet is None or row["balance"] < bet:
        await update.message.reply_text("Invalid bet or insufficient balance.")
        return

    if await active_games_for_user(uid):
        await update.message.reply_text("Finish your current game first.")
        return

    gid = await create_game(
        uid,
        "bus",
        bet,
        {"step": 0, "cards": []},
    )

    await update.message.reply_text(
        "🚌 RIDE THE BUS\nRound 1: Red or Black?",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("Red", callback_data=f"bus:{gid}:red"),
                InlineKeyboardButton("Black", callback_data=f"bus:{gid}:black"),
            ]
        ]),
    )


# ============================================================
# BATTLESHIP
# ============================================================

def battleship_markup(gid, state):
    shots = set(state.get("shots", []))
    ships = set(state.get("ships", []))
    rows = []
    for r in range(5):
        line = []
        for c in range(5):
            i = r * 5 + c
            if i in shots:
                label = "💥" if i in ships else "🌊"
            else:
                label = "⬜"
            line.append(
                InlineKeyboardButton(
                    label,
                    callback_data=f"ship:{gid}:{i}",
                )
            )
        rows.append(line)
    return InlineKeyboardMarkup(rows)


async def battleship_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_user(update.effective_user)
    uid = update.effective_user.id
    if await is_blocked(uid):
        return

    bet = safe_int(context.args[0], MIN_BET, MAX_BET) if context.args else 10
    row = await get_user(uid)

    if bet is None or row["balance"] < bet:
        await update.message.reply_text("Invalid bet or insufficient balance.")
        return

    if await active_games_for_user(uid):
        await update.message.reply_text("Finish your current game first.")
        return

    ships = set(secrets.SystemRandom().sample(range(25), 5))
    gid = await create_game(
        uid,
        "battleship",
        bet,
        {"ships": sorted(ships), "shots": []},
    )

    await update.message.reply_text(
        "🚢 BATTLESHIP\n"
        "5×5 private board. Sink all 5 hidden ship squares.",
        reply_markup=battleship_markup(
            gid,
            {"ships": sorted(ships), "shots": []},
        ),
    )


# ============================================================
# CALLBACK SECURITY
# ============================================================

async def load_owned_active_game(gid: str, uid: int):
    # Game ID is unguessable, but ownership is still explicitly checked.
    async with DB_LOCK:
        con = open_db()
        try:
            return con.execute(
                """
                SELECT *
                FROM games
                WHERE id=? AND user_id=? AND status='active'
                """,
                (gid, uid),
            ).fetchone()
        finally:
            con.close()


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = query.from_user.id
    await ensure_user(query.from_user)

    if await is_blocked(uid):
        await query.answer("Account unavailable.", show_alert=True)
        return

    data = query.data or ""

    try:
        if data == "home":
            await query.answer()
            await query.edit_message_text(
                "🎮 Choose a game:",
                reply_markup=home_markup(),
            )
            return

        if data.startswith("menu:"):
            await query.answer()
            key = data.split(":", 1)[1]
            help_text = {
                "dice": "/dice 1d1w 5",
                "bowl": "/bowl 1d2w 5",
                "football": "/football 1d1w 5",
                "basketball": "/basketball 1d1w 5",
                "blackjack": "/blackjack 5",
                "keno": "/keno 5",
                "hilo": "/hilo 5",
                "higherlower": "/higherlower 5",
                "redblack": "/redblack 5",
                "plinko": "/plinko 5",
                "mines": "/mines 5 5 3",
                "crash": "/crash 5",
                "bus": "/bus 5",
                "battleship": "/battleship 5",
                "tower": "/tower 5 medium",
                "roulette": "/roulette 5",
                "diamonds": "/diamonds 5 5 3",
                "balance": f"💰 {money((await get_user(uid))['balance'])} credits",
                "history": "Use /history to see your last 15 games.",
            }.get(key, "Use /games.")
            await query.edit_message_text(
                f"🎮 {key.title()}\n\n{help_text}",
                reply_markup=back_markup(),
            )
            return

        if data.startswith("bj:"):
            _, gid, action = data.split(":")
            async with user_lock(uid):
                await query.answer()
                await blackjack_action(query, uid, gid, action)
            return

        if data.startswith("keno:"):
            await query.answer()
            await keno_callback(query, uid, data)
            return

        if data.startswith("cg:"):
            _, gid, choice = data.split(":")
            await query.answer()
            async with user_lock(uid):
                await card_guess_callback(query, uid, gid, choice)
            return

        if data.startswith("rb:"):
            _, gid, choice = data.split(":")
            await query.answer()
            async with user_lock(uid):
                await redblack_callback(query, uid, gid, choice)
            return

        if data.startswith("dr:"):
            parts = data.split(":")
            await query.answer()
            async with user_lock(uid):
                await dice_roulette_callback(query, uid, parts)
            return

        if data.startswith("mine:"):
            _, gid, action = data.split(":")
            await query.answer()
            async with user_lock(uid):
                await mines_callback(query, uid, gid, action)
            return

        if data.startswith("crash:"):
            _, gid = data.split(":")
            await query.answer()
            async with user_lock(uid):
                await crash_callback(query, uid, gid)
            return

        if data.startswith("tower:"):
            _, gid, action = data.split(":")
            await query.answer()
            async with user_lock(uid):
                await tower_callback(query, uid, gid, action)
            return

        if data.startswith("roulette:"):
            parts = data.split(":")
            await query.answer()
            async with user_lock(uid):
                await roulette_callback(query, uid, parts)
            return

        if data.startswith("bus:"):
            parts = data.split(":")
            await query.answer()
            async with user_lock(uid):
                choice = parts[2]
                if len(parts) > 3:
                    choice = parts[3]
                await bus_callback(query, uid, parts[1], choice)
            return

        if data.startswith("ship:"):
            _, gid, cell = data.split(":")
            await query.answer()
            async with user_lock(uid):
                await ship_callback(query, uid, gid, cell)
            return

        await query.answer("Unknown action.", show_alert=True)

    except Exception:
        traceback.print_exc()
        try:
            await query.answer(
                "Something went wrong. No balance change was applied.",
                show_alert=True,
            )
        except Exception:
            pass


# ============================================================
# CALLBACK IMPLEMENTATIONS
# ============================================================

async def keno_callback(query, uid, data):
    parts = data.split(":")
    # Keno game ID is recovered from the user's single active Keno game.
    games = await active_games_for_user(uid)
    game = next((g for g in games if g["game"] == "keno"), None)
    if not game:
        await query.answer("No active Keno game.", show_alert=True)
        return

    state = json.loads(game["state"])
    chosen = set(state.get("chosen", []))

    if parts[1] == "draw":
        if len(chosen) != 10:
            await query.answer("Select exactly 10 numbers.", show_alert=True)
            return

        draw = set(secrets.SystemRandom().sample(range(1, 41), 10))
        hits = len(chosen & draw)

        multipliers = {
            0: 0.0,
            1: 0.0,
            2: 0.0,
            3: 1.00,
            4: 2.00,
            5: 5.00,
            6: 12.00,
            7: 25.00,
            8: 60.00,
            9: 150.00,
            10: 500.00,
        }
        payout = multiplier_payout(game["bet"], multipliers[hits])

        await finish_game(
            game["id"],
            uid,
            payout,
            f"{hits}/10 hits",
            {
                "chosen": sorted(chosen),
                "draw": sorted(draw),
                "hits": hits,
            },
        )

        await query.edit_message_text(
            f"🎟 KENO\n"
            f"Draw: {' '.join(map(str, sorted(draw)))}\n"
            f"Hits: {hits}/10\n"
            f"Payout: {money(payout)}"
        )
        return

    n = safe_int(parts[1], 1, 40)
    if n is None:
        return

    if n in chosen:
        chosen.remove(n)
    elif len(chosen) < 10:
        chosen.add(n)
    else:
        await query.answer("You can select exactly 10.", show_alert=True)
        return

    await update_game_state(
        game["id"],
        {"chosen": sorted(chosen)},
    )

    await query.edit_message_text(
        f"🎟 KENO\nSelected: {len(chosen)}/10",
        reply_markup=keno_markup(chosen),
    )


async def card_guess_callback(query, uid, gid, choice):
    game = await load_owned_active_game(gid, uid)
    if not game:
        await query.answer("Game is no longer active.", show_alert=True)
        return

    state = json.loads(game["state"])
    old = tuple(state["current"])
    new = secrets.choice(cards_deck())

    old_index = RANKS.index(old[0])
    new_index = RANKS.index(new[0])

    if new_index == old_index:
        payout = game["bet"]
        result = "push"
    else:
        won = (
            new_index > old_index
            if choice == "higher"
            else new_index < old_index
        )
        payout = multiplier_payout(game["bet"], 1.92) if won else 0
        result = "win" if won else "loss"

    await finish_game(
        gid,
        uid,
        payout,
        result,
        {"old": old, "new": new, "choice": choice},
    )

    await query.edit_message_text(
        f"🃏 {card_text(old)} → {card_text(new)}\n"
        f"Result: {result}\n"
        f"Payout: {money(payout)}"
    )


async def redblack_callback(query, uid, gid, choice):
    game = await load_owned_active_game(gid, uid)
    if not game:
        await query.answer("Game is no longer active.", show_alert=True)
        return

    c = secrets.choice(cards_deck())
    color = "red" if c[1] in ("♥", "♦") else "black"
    won = color == choice
    payout = multiplier_payout(game["bet"], 1.92) if won else 0

    await finish_game(
        gid, uid, payout,
        "win" if won else "loss",
        {"card": c, "choice": choice},
    )

    await query.edit_message_text(
        f"🔴⚫ {card_text(c)} = {color}\n"
        f"Payout: {money(payout)}"
    )


async def dice_roulette_callback(query, uid, parts):
    if len(parts) < 3:
        return

    gid = parts[1]
    choice = parts[2]
    subchoice = parts[3] if len(parts) > 3 else None

    game = await load_owned_active_game(gid, uid)
    if not game:
        await query.answer("Game is no longer active.", show_alert=True)
        return

    result = secrets.randbelow(6) + 1

    if choice == "n":
        target = safe_int(subchoice, 1, 6)
        won = result == target
        multiplier = 5.0
    elif choice == "high":
        won = result >= 4
        multiplier = 1.92
    elif choice == "low":
        won = result <= 3
        multiplier = 1.92
    elif choice == "odd":
        won = result % 2 == 1
        multiplier = 1.92
    elif choice == "even":
        won = result % 2 == 0
        multiplier = 1.92
    else:
        return

    payout = multiplier_payout(game["bet"], multiplier) if won else 0

    await finish_game(
        gid, uid, payout,
        "win" if won else "loss",
        {
            "choice": [choice, subchoice],
            "result": result,
            "multiplier": multiplier,
        },
    )

    await query.edit_message_text(
        f"🎲 Result: {result}\n"
        f"{'WIN' if won else 'LOSS'}\n"
        f"Payout: {money(payout)}"
    )


async def mines_callback(query, uid, gid, action):
    game = await load_owned_active_game(gid, uid)
    if not game:
        await query.answer("Game is no longer active.", show_alert=True)
        return

    state = json.loads(game["state"])
    size = int(state["size"])
    total = size * size
    mines = set(state["mines"])
    revealed = set(state["revealed"])
    safe_count = int(state["safe_count"])

    if action == "cash":
        if safe_count <= 0:
            await query.answer(
                "Reveal at least one safe square first.",
                show_alert=True,
            )
            return

        safe_total = total - len(mines)
        multiplier = (
            math.comb(total, safe_count)
            / max(1, math.comb(safe_total, safe_count))
        )
        multiplier = max(1.01, min(multiplier, 100000.0))
        payout = multiplier_payout(game["bet"], multiplier)

        await finish_game(
            gid,
            uid,
            payout,
            f"cashout {multiplier:.4f}x",
            {
                "size": size,
                "mines": sorted(mines),
                "revealed": sorted(revealed),
                "safe_count": safe_count,
            },
        )

        await query.edit_message_text(
            f"💰 Cashed out at {multiplier:.2f}x\n"
            f"Payout: {money(payout)}"
        )
        return

    cell = safe_int(action, 0, total - 1)
    if cell is None:
        return

    if cell in revealed:
        await query.answer("Already revealed.", show_alert=True)
        return

    revealed.add(cell)

    if cell in mines:
        await finish_game(
            gid,
            uid,
            0,
            "mine",
            {
                "size": size,
                "mines": sorted(mines),
                "revealed": sorted(revealed),
                "safe_count": safe_count,
                "hit": cell,
            },
        )
        await query.edit_message_text("💥 Mine hit. Game over.")
        return

    safe_count += 1

    if safe_count >= total - len(mines):
        # All safe squares found: settle at the current exact survival value.
        multiplier = max(
            1.01,
            math.comb(total, safe_count)
            / max(1, math.comb(total - len(mines), safe_count)),
        )
        payout = multiplier_payout(game["bet"], multiplier)
        await finish_game(
            gid,
            uid,
            payout,
            "all safe squares",
            {
                "size": size,
                "mines": sorted(mines),
                "revealed": sorted(revealed),
                "safe_count": safe_count,
            },
        )
        await query.edit_message_text(
            f"💎 All safe squares found!\nPayout: {money(payout)}"
        )
        return

    new_state = {
        "size": size,
        "mine_count": len(mines),
        "mines": sorted(mines),
        "revealed": sorted(revealed),
        "safe_count": safe_count,
    }
    await update_game_state(gid, new_state)

    live_state = {
        "gid": gid,
        "size": size,
        "mine_count": len(mines),
        "mines": mines,
        "revealed": revealed,
        "safe_count": safe_count,
    }

    await query.edit_message_text(
        f"💎 Safe!\n"
        f"Safe picks: {safe_count}\n"
        "Continue or cash out.",
        reply_markup=mines_markup(live_state),
    )


async def crash_callback(query, uid, gid):
    game = await load_owned_active_game(gid, uid)
    if not game:
        await query.answer("Game is no longer active.", show_alert=True)
        return

    state = json.loads(game["state"])
    crash_point = float(state["crash_point"])

    # The actual current multiplier is computed from elapsed time and persisted.
    # The user must cash out before the deterministic crash time.
    started = datetime.fromisoformat(game["created_at"])
    elapsed = max(0.0, (utcnow() - started).total_seconds())
    current = round(1.0 + elapsed * 0.25, 2)

    if current >= crash_point:
        await finish_game(
            gid,
            uid,
            0,
            f"crashed {crash_point:.2f}x",
            {"crash_point": crash_point, "current": crash_point},
        )
        await query.edit_message_text(
            f"💥 CRASHED at {crash_point:.2f}x"
        )
        return

    payout = multiplier_payout(game["bet"], current)
    await finish_game(
        gid,
        uid,
        payout,
        f"cashout {current:.2f}x",
        {"crash_point": crash_point, "current": current},
    )
    await query.edit_message_text(
        f"🚀 Cashed out at {current:.2f}x\n"
        f"Payout: {money(payout)}"
    )


async def tower_callback(query, uid, gid, action):
    game = await load_owned_active_game(gid, uid)
    if not game:
        await query.answer("Game is no longer active.", show_alert=True)
        return

    state = json.loads(game["state"])
    difficulty = state["difficulty"]
    choices = int(state["choices"])
    floor = int(state["floor"])
    multiplier = float(state["multiplier"])

    if action == "cash":
        if floor <= 0:
            await query.answer(
                "Clear at least one floor first.",
                show_alert=True,
            )
            return

        payout = multiplier_payout(game["bet"], multiplier)
        await finish_game(
            gid,
            uid,
            payout,
            f"cashout floor {floor}",
            state,
        )
        await query.edit_message_text(
            f"🏰 Cash out • Floor {floor}\n"
            f"Multiplier: {multiplier:.2f}x\n"
            f"Payout: {money(payout)}"
        )
        return

    pick = safe_int(action, 0, choices - 1)
    if pick is None:
        return

    correct = secrets.randbelow(choices)
    if pick != correct:
        await finish_game(
            gid,
            uid,
            0,
            f"wrong choice floor {floor + 1}",
            {**state, "correct": correct},
        )
        await query.edit_message_text(
            f"💥 Tower failed on floor {floor + 1}."
        )
        return

    floor += 1
    multiplier *= TOWER_STEP[difficulty]

    new_state = {
        "difficulty": difficulty,
        "choices": choices,
        "floor": floor,
        "multiplier": multiplier,
    }
    await update_game_state(gid, new_state)

    await query.edit_message_text(
        f"🏰 {difficulty.title()} Tower\n"
        f"Floor {floor}\n"
        f"Multiplier: {multiplier:.2f}x",
        reply_markup=tower_markup(gid, choices),
    )


async def roulette_callback(query, uid, parts):
    if len(parts) < 3:
        return

    gid = parts[1]
    choice = parts[2]
    subchoice = parts[3] if len(parts) > 3 else None

    game = await load_owned_active_game(gid, uid)
    if not game:
        await query.answer("Game is no longer active.", show_alert=True)
        return

    n = secrets.randbelow(37)

    if choice == "n":
        target = safe_int(subchoice, 0, 36)
        if target is None:
            return
        won = n == target
        multiplier = 35.0
    elif choice == "red":
        won = n in ROULETTE_RED
        multiplier = 1.92
    elif choice == "black":
        won = n != 0 and n not in ROULETTE_RED
        multiplier = 1.92
    elif choice == "odd":
        won = n != 0 and n % 2 == 1
        multiplier = 1.92
    elif choice == "even":
        won = n != 0 and n % 2 == 0
        multiplier = 1.92
    else:
        return

    payout = multiplier_payout(game["bet"], multiplier) if won else 0

    await finish_game(
        gid,
        uid,
        payout,
        "win" if won else "loss",
        {
            "number": n,
            "choice": choice,
            "subchoice": subchoice,
        },
    )

    await query.edit_message_text(
        f"🎡 Roulette result: {n}\n"
        f"{'WIN' if won else 'LOSS'}\n"
        f"Payout: {money(payout)}"
    )


async def bus_callback(query, uid, gid, choice):
    game = await load_owned_active_game(gid, uid)
    if not game:
        await query.answer("Game is no longer active.", show_alert=True)
        return

    state = json.loads(game["state"])
    step = int(state["step"])
    cards = [tuple(x) for x in state["cards"]]

    new_card = secrets.choice(cards_deck())

    if step == 0:
        correct = (
            "red" if new_card[1] in ("♥", "♦") else "black"
        )
        won = choice == correct
        prompt = "Higher or Lower?"
        buttons = [
            InlineKeyboardButton("Higher", callback_data=f"bus:{gid}:higher"),
            InlineKeyboardButton("Lower", callback_data=f"bus:{gid}:lower"),
        ]
    elif step == 1:
        previous = cards[-1]
        a = RANKS.index(previous[0])
        b = RANKS.index(new_card[0])
        correct = "higher" if b > a else "lower" if b < a else "tie"
        won = choice == correct
        prompt = "Inside or Outside?"
        buttons = [
            InlineKeyboardButton("Inside", callback_data=f"bus:{gid}:inside"),
            InlineKeyboardButton("Outside", callback_data=f"bus:{gid}:outside"),
        ]
        if correct == "tie":
            # Same-rank result is a push at this stage.
            won = True
    elif step == 2:
        previous = cards[-1]
        lo = min(RANKS.index(previous[0]), RANKS.index(cards[0][0]))
        hi = max(RANKS.index(previous[0]), RANKS.index(cards[0][0]))
        b = RANKS.index(new_card[0])
        correct = "inside" if lo < b < hi else "outside"
        won = choice == correct
        prompt = "Suit?"
        buttons = [
            InlineKeyboardButton(
                suit,
                callback_data=f"bus:{gid}:suit:{suit}",
            )
            for suit in SUITS
        ]
    else:
        # The callback contains "suit:<symbol>".
        expected = new_card[1]
        won = choice == expected
        correct = expected
        prompt = ""

        if won:
            payout = multiplier_payout(game["bet"], 8.0)
        else:
            payout = 0

        cards.append(new_card)
        await finish_game(
            gid,
            uid,
            payout,
            "complete" if won else "loss",
            {"cards": cards, "step": step + 1},
        )
        await query.edit_message_text(
            f"🚌 Final card: {card_text(new_card)}\n"
            f"{'COMPLETE' if won else 'WRONG'}\n"
            f"Payout: {money(payout)}"
        )
        return

    cards.append(new_card)

    if not won:
        await finish_game(
            gid,
            uid,
            0,
            "loss",
            {"cards": cards, "step": step},
        )
        await query.edit_message_text(
            f"🚌 Card: {card_text(new_card)}\nWrong prediction. Bus ended."
        )
        return

    step += 1
    new_state = {"step": step, "cards": cards}
    await update_game_state(gid, new_state)

    await query.edit_message_text(
        f"🚌 Card: {card_text(new_card)}\n"
        f"Round {step + 1}: {prompt}",
        reply_markup=InlineKeyboardMarkup([buttons]),
    )


async def ship_callback(query, uid, gid, cell_text):
    cell = safe_int(cell_text, 0, 24)
    if cell is None:
        return

    game = await load_owned_active_game(gid, uid)
    if not game:
        await query.answer("Game is no longer active.", show_alert=True)
        return

    state = json.loads(game["state"])
    ships = set(state["ships"])
    shots = set(state["shots"])

    if cell in shots:
        await query.answer("Already fired there.", show_alert=True)
        return

    shots.add(cell)

    if ships.issubset(shots):
        payout = multiplier_payout(game["bet"], 4.0)
        await finish_game(
            gid,
            uid,
            payout,
            "all ships sunk",
            {"ships": sorted(ships), "shots": sorted(shots)},
        )
        await query.edit_message_text(
            f"🚢 All ships sunk!\nPayout: {money(payout)}"
        )
        return

    await update_game_state(
        gid,
        {"ships": sorted(ships), "shots": sorted(shots)},
    )

    await query.edit_message_text(
        "🚢 BATTLESHIP\n"
        f"Hits: {len(ships & shots)}/5",
        reply_markup=battleship_markup(
            gid,
            {"ships": sorted(ships), "shots": sorted(shots)},
        ),
    )


# ============================================================
# ADMIN
# ============================================================

def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS


def admin_markup():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 P&L", callback_data="adm:pnl"),
            InlineKeyboardButton("🎮 Games", callback_data="adm:games"),
        ],
        [
            InlineKeyboardButton("👤 Search", callback_data="adm:search"),
            InlineKeyboardButton("🧾 Ledger", callback_data="adm:ledger"),
        ],
        [
            InlineKeyboardButton("🛡 Moderation", callback_data="adm:mod"),
        ],
    ])


async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_user(update.effective_user)
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Admin only.")
        return

    await update.message.reply_text(
        "🛠 ADMIN SUITE\n\n"
        "/user <id>\n"
        "/addbalance <id> <amount> <reason>\n"
        "/removebalance <id> <amount> <reason>\n"
        "/ban <id> [reason]\n"
        "/unban <id>\n"
        "/mute <id> [hours] [reason]\n"
        "/unmute <id>\n"
        "/pnl\n"
        "/gamestats\n"
        "/ledger <id>\n"
        "/broadcast <text>\n",
        reply_markup=admin_markup(),
    )


async def admin_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    if not context.args or safe_int(context.args[0], 1) is None:
        await update.message.reply_text("Usage: /user <telegram_id>")
        return

    uid = int(context.args[0])
    row = await get_user(uid)

    if not row:
        await update.message.reply_text("User not found.")
        return

    await update.message.reply_text(
        f"👤 USER\n"
        f"ID: {uid}\n"
        f"Username: @{row['username'] or 'none'}\n"
        f"Name: {row['first_name']}\n"
        f"Balance: {money(row['balance'])}\n"
        f"Games: {row['games']}\n"
        f"Wagered: {money(row['wagered'])}\n"
        f"Payouts: {money(row['payouts'])}\n"
        f"Losses: {money(row['losses'])}\n"
        f"Warnings: {row['warnings']}\n"
        f"Mutes: {row['mutes']}\n"
        f"Banned: {bool(row['banned'])}\n"
        f"Muted until: {row['muted_until'] or 'no'}"
    )


async def admin_balance_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    add: bool,
):
    if not is_admin(update.effective_user.id):
        return

    if len(context.args) < 3:
        command = "/addbalance" if add else "/removebalance"
        await update.message.reply_text(
            f"Usage: {command} <id> <amount> <reason>"
        )
        return

    uid = safe_int(context.args[0], 1)
    amount = safe_int(context.args[1], 1, MAX_BET * 100)
    reason = " ".join(context.args[2:])[:1000]

    if uid is None or amount is None:
        await update.message.reply_text("Invalid user ID or amount.")
        return

    target = await get_user(uid)
    if not target:
        await update.message.reply_text("User not found.")
        return

    delta = amount if add else -amount

    try:
        new_balance = await ledger_change(
            uid,
            delta,
            "admin_adjustment",
            ref=f"admin:{update.effective_user.id}",
            note=reason,
        )
    except ValueError as e:
        await update.message.reply_text(str(e))
        return

    await write_admin_log(
        update.effective_user.id,
        "balance_adjustment",
        uid,
        f"delta={delta};new_balance={new_balance};reason={reason}",
    )

    await update.message.reply_text(
        f"✅ Balance updated.\n"
        f"User: {uid}\n"
        f"Change: {delta:+,}\n"
        f"New balance: {money(new_balance)}"
    )


async def ban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("Usage: /ban <id> [reason]")
        return

    uid = safe_int(context.args[0], 1)
    if uid is None:
        await update.message.reply_text("Invalid ID.")
        return

    reason = " ".join(context.args[1:])[:1000] or "Admin ban"

    await ensure_user_by_id(uid)
    await sql_update_user(uid, banned=1)

    await sql_insert_moderation(uid, "ban", reason, update.effective_user.id)
    await write_admin_log(
        update.effective_user.id,
        "ban",
        uid,
        reason,
    )
    await update.message.reply_text("🚫 User banned.")


async def unban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("Usage: /unban <id>")
        return

    uid = safe_int(context.args[0], 1)
    if uid is None:
        return

    await ensure_user_by_id(uid)
    await sql_update_user(uid, banned=0)

    await sql_insert_moderation(
        uid, "unban", "Admin unban", update.effective_user.id
    )
    await write_admin_log(
        update.effective_user.id, "unban", uid, ""
    )
    await update.message.reply_text("✅ User unbanned.")


async def mute_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("Usage: /mute <id> [hours] [reason]")
        return

    uid = safe_int(context.args[0], 1)
    hours = safe_int(context.args[1], 1, 720) if len(context.args) > 1 else 3
    reason_start = 2 if len(context.args) > 1 and hours is not None else 1
    reason = " ".join(context.args[reason_start:])[:1000] or "Admin mute"

    if uid is None:
        return

    until = utcnow() + timedelta(hours=hours)
    await ensure_user_by_id(uid)
    await sql_update_user(
        uid,
        muted_until=until.isoformat(),
        mutes_increment=1,
    )
    await sql_insert_moderation(
        uid, "mute", reason, update.effective_user.id
    )
    await write_admin_log(
        update.effective_user.id,
        "mute",
        uid,
        f"hours={hours};reason={reason}",
    )
    await update.message.reply_text(
        f"🔇 User muted until {until.isoformat()}."
    )


async def unmute_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("Usage: /unmute <id>")
        return

    uid = safe_int(context.args[0], 1)
    if uid is None:
        return

    await ensure_user_by_id(uid)
    await sql_update_user(uid, muted_until=None)
    await sql_insert_moderation(
        uid, "unmute", "Admin unmute", update.effective_user.id
    )
    await write_admin_log(
        update.effective_user.id, "unmute", uid, ""
    )
    await update.message.reply_text("🔊 User unmuted.")


async def ensure_user_by_id(uid: int):
    row = await get_user(uid)
    if row:
        return
    async with transaction() as con:
        now = iso_now()
        con.execute(
            """
            INSERT OR IGNORE INTO users(
                user_id,username,first_name,balance,created_at,updated_at
            ) VALUES(?,?,?,?,?,?)
            """,
            (uid, "", "", 0, now, now),
        )


async def sql_update_user(
    uid: int,
    banned: Optional[int] = None,
    muted_until: Optional[str] = "__NOCHANGE__",
    mutes_increment: int = 0,
):
    async with transaction() as con:
        row = con.execute(
            "SELECT * FROM users WHERE user_id=?",
            (uid,),
        ).fetchone()
        if not row:
            raise ValueError("User not found.")

        if banned is not None:
            con.execute(
                "UPDATE users SET banned=?,updated_at=? WHERE user_id=?",
                (int(banned), iso_now(), uid),
            )

        if muted_until != "__NOCHANGE__":
            if mutes_increment:
                con.execute(
                    """
                    UPDATE users
                    SET muted_until=?,mutes=mutes+?,updated_at=?
                    WHERE user_id=?
                    """,
                    (muted_until, mutes_increment, iso_now(), uid),
                )
            else:
                con.execute(
                    "UPDATE users SET muted_until=?,updated_at=? WHERE user_id=?",
                    (muted_until, iso_now(), uid),
                )


async def sql_insert_moderation(
    uid: int,
    action: str,
    reason: str,
    admin_id: Optional[int],
):
    async with transaction() as con:
        con.execute(
            """
            INSERT INTO moderation_log(
                user_id,action,reason,admin_id,created_at
            ) VALUES(?,?,?,?,?)
            """,
            (uid, action, reason[:1000], admin_id, iso_now()),
        )


async def pnl_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    async with DB_LOCK:
        con = open_db()
        try:
            r = con.execute(
                """
                SELECT
                  COALESCE(SUM(bet),0) AS wagers,
                  COALESCE(SUM(payout),0) AS payouts,
                  COUNT(*) AS games
                FROM games
                WHERE status='finished'
                """
            ).fetchone()
            balances = con.execute(
                "SELECT COALESCE(SUM(balance),0) AS total FROM users"
            ).fetchone()["total"]
            users = con.execute(
                "SELECT COUNT(*) AS n FROM users"
            ).fetchone()["n"]
        finally:
            con.close()

    gross = r["wagers"] - r["payouts"]

    await update.message.reply_text(
        "📊 VIRTUAL-CREDIT P&L\n\n"
        f"Players: {users:,}\n"
        f"Finished games: {r['games']:,}\n"
        f"Total wagered: {money(r['wagers'])}\n"
        f"Total payouts: {money(r['payouts'])}\n"
        f"House result: {money(gross)}\n"
        f"Outstanding player credits: {money(balances)}"
    )


async def gamestats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    async with DB_LOCK:
        con = open_db()
        try:
            rows = con.execute(
                """
                SELECT game,
                       COUNT(*) games,
                       COALESCE(SUM(bet),0) wagered,
                       COALESCE(SUM(payout),0) payouts
                FROM games
                WHERE status='finished'
                GROUP BY game
                ORDER BY wagered DESC
                """
            ).fetchall()
        finally:
            con.close()

    if not rows:
        await update.message.reply_text("No finished games.")
        return

    lines = ["🎮 GAME STATS"]
    for r in rows:
        lines.append(
            f"{r['game']}: games={r['games']:,} "
            f"wagered={money(r['wagered'])} "
            f"payouts={money(r['payouts'])} "
            f"house={money(r['wagered']-r['payouts'])}"
        )

    await update.message.reply_text("\n".join(lines))


async def ledger_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    if not context.args:
        await update.message.reply_text("Usage: /ledger <id>")
        return

    uid = safe_int(context.args[0], 1)
    if uid is None:
        return

    rows = await get_ledger(uid, 30)
    if not rows:
        await update.message.reply_text("No ledger records.")
        return

    lines = [f"🧾 LEDGER {uid}"]
    for r in rows:
        lines.append(
            f"{r['created_at']} | {r['kind']} | {r['amount']:+,} | "
            f"bal={r['balance_after']:,} | {r['note'] or ''}"
        )
    await update.message.reply_text("\n".join(lines))


async def get_ledger(uid, limit=30):
    async with DB_LOCK:
        con = open_db()
        try:
            return con.execute(
                """
                SELECT *
                FROM ledger
                WHERE user_id=?
                ORDER BY id DESC
                LIMIT ?
                """,
                (uid, limit),
            ).fetchall()
        finally:
            con.close()


async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    text = " ".join(context.args).strip()
    if not text or len(text) > 4000:
        await update.message.reply_text("Usage: /broadcast <text up to 4000 chars>")
        return

    async with DB_LOCK:
        con = open_db()
        try:
            rows = con.execute("SELECT user_id FROM users WHERE banned=0").fetchall()
        finally:
            con.close()

    sent = 0
    failed = 0
    for row in rows:
        try:
            await context.bot.send_message(row["user_id"], text)
            sent += 1
        except TelegramError:
            failed += 1
        await asyncio.sleep(0.05)

    await write_admin_log(
        update.effective_user.id,
        "broadcast",
        None,
        f"sent={sent};failed={failed}",
    )

    await update.message.reply_text(
        f"📣 Broadcast complete.\nSent: {sent}\nFailed: {failed}"
    )


# ============================================================
# ADMIN CALLBACKS
# ============================================================

async def admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = query.from_user.id

    if not is_admin(uid):
        await query.answer("Admin only.", show_alert=True)
        return

    await query.answer()

    if query.data == "adm:pnl":
        # Reuse a direct aggregate here.
        async with DB_LOCK:
            con = open_db()
            try:
                r = con.execute(
                    """
                    SELECT COALESCE(SUM(bet),0) wagers,
                           COALESCE(SUM(payout),0) payouts,
                           COUNT(*) games
                    FROM games
                    WHERE status='finished'
                    """
                ).fetchone()
            finally:
                con.close()

        await query.edit_message_text(
            f"📊 P&L\n"
            f"Games: {r['games']:,}\n"
            f"Wagered: {money(r['wagers'])}\n"
            f"Payouts: {money(r['payouts'])}\n"
            f"House result: {money(r['wagers']-r['payouts'])}",
            reply_markup=admin_markup(),
        )
        return

    if query.data == "adm:games":
        await query.edit_message_text(
            "🎮 Use /gamestats for per-game statistics.",
            reply_markup=admin_markup(),
        )
        return

    if query.data == "adm:search":
        await query.edit_message_text(
            "👤 Use /user <telegram_id>.",
            reply_markup=admin_markup(),
        )
        return

    if query.data == "adm:ledger":
        await query.edit_message_text(
            "🧾 Use /ledger <telegram_id>.",
            reply_markup=admin_markup(),
        )
        return

    if query.data == "adm:mod":
        await query.edit_message_text(
            "🛡 Moderation\n"
            "/ban <id> [reason]\n"
            "/unban <id>\n"
            "/mute <id> [hours] [reason]\n"
            "/unmute <id>\n\n"
            "Group auto-moderation only targets promotional links/channel-style "
            "promotion. Ordinary @player mentions are allowed.",
            reply_markup=admin_markup(),
        )
        return


# ============================================================
# MODERATION
# ============================================================

URL_RE = re.compile(
    r"(?:https?://|www\.)[^\s]+|(?:t\.me|telegram\.me)/[^\s]+",
    re.IGNORECASE,
)

PROMO_RE = re.compile(
    r"\b(?:promo(?:tion)?|referral|ref code|giveaway|airdrop|"
    r"subscribe|channel)\b",
    re.IGNORECASE,
)


def is_channel_style_promotion(text: str) -> bool:
    # Ordinary @mentions are NOT moderation targets.
    # A channel-style @handle plus promotional language is targeted.
    has_handle = bool(re.search(r"(?<!\w)@[A-Za-z0-9_]{4,64}", text))
    return has_handle and bool(PROMO_RE.search(text))


async def moderation_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message

    if not message or not message.text:
        return

    chat = update.effective_chat
    if not chat or chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return

    uid = message.from_user.id

    if uid in ADMIN_IDS:
        return

    await ensure_user(message.from_user)

    text = message.text[:MAX_TEXT_LENGTH]

    # User-to-user @mentions are explicitly allowed.
    promotional = bool(URL_RE.search(text)) or is_channel_style_promotion(text)
    if not promotional:
        return

    # Delete first. If Telegram permissions do not allow deletion, continue
    # with warning escalation anyway.
    try:
        await message.delete()
    except TelegramError:
        pass

    row = await get_user(uid)
    warnings = int(row["warnings"]) + 1

    if warnings < 3:
        await sql_update_user_warnings(uid, warnings)
        await sql_insert_moderation(uid, "warning", "promo/link", None)
        try:
            await context.bot.send_message(
                chat.id,
                f"⚠️ Warning {warnings}/3 for promotional links/content.",
            )
        except TelegramError:
            pass
        return

    # Third warning -> 3-hour mute.
    await sql_update_user_warnings(uid, 0)

    mutes = int(row["mutes"]) + 1
    until = utcnow() + timedelta(hours=3)

    await sql_update_user_mute(uid, until.isoformat(), mutes)
    await sql_insert_moderation(uid, "mute", "promo/link", None)

    try:
        await context.bot.restrict_chat_member(
            chat.id,
            uid,
            permissions=ChatPermissions(can_send_messages=False),
            until_date=until,
        )
    except TelegramError:
        pass

    if mutes >= 3:
        await sql_update_user_banned(uid, 1)
        await sql_insert_moderation(uid, "ban", "3 moderation mutes", None)
        try:
            await context.bot.ban_chat_member(chat.id, uid)
        except TelegramError:
            pass
        try:
            await context.bot.send_message(
                chat.id,
                "🚫 User banned after 3 moderation mutes.",
            )
        except TelegramError:
            pass
    else:
        try:
            await context.bot.send_message(
                chat.id,
                f"🔇 User muted for 3 hours ({mutes}/3).",
            )
        except TelegramError:
            pass


async def sql_update_user_warnings(uid, warnings):
    async with transaction() as con:
        con.execute(
            "UPDATE users SET warnings=?,updated_at=? WHERE user_id=?",
            (warnings, iso_now(), uid),
        )


async def sql_update_user_mute(uid, until, mutes):
    async with transaction() as con:
        con.execute(
            "UPDATE users SET muted_until=?,mutes=?,updated_at=? WHERE user_id=?",
            (until, mutes, iso_now(), uid),
        )


async def sql_update_user_banned(uid, banned):
    async with transaction() as con:
        con.execute(
            "UPDATE users SET banned=?,updated_at=? WHERE user_id=?",
            (banned, iso_now(), uid),
        )


# ============================================================
# RECOVERY
# ============================================================

async def recover_stale_games():
    """
    Active games live in the DB. If the process restarts while a game is
    active, do not invent a result. Refund the original wager exactly once
    after 30 minutes and mark the game cancelled.
    """
    cutoff = utcnow() - timedelta(minutes=30)

    async with DB_LOCK:
        con = open_db()
        try:
            rows = con.execute(
                """
                SELECT id,user_id
                FROM games
                WHERE status='active' AND created_at < ?
                """,
                (cutoff.isoformat(),),
            ).fetchall()
        finally:
            con.close()

    for row in rows:
        try:
            await cancel_game_refund(
                row["id"],
                row["user_id"],
                "Stale active game automatically cancelled after restart/timeout.",
            )
        except Exception:
            traceback.print_exc()


# ============================================================
# COMMAND WRAPPERS
# ============================================================

async def cmd_dice(update, context):
    await emoji_command(update, context, "dice")


async def cmd_bowl(update, context):
    await emoji_command(update, context, "bowl")


async def cmd_football(update, context):
    await emoji_command(update, context, "football")


async def cmd_basketball(update, context):
    await emoji_command(update, context, "basketball")


async def cmd_hilo(update, context):
    await card_guess_command(update, context, "hilo")


async def cmd_higherlower(update, context):
    await card_guess_command(update, context, "higherlower")


async def cmd_mines(update, context):
    await mines_command(update, context, False)


async def cmd_diamonds(update, context):
    await mines_command(update, context, True)


async def cmd_bus(update, context):
    await bus_command(update, context)


# ============================================================
# ERROR HANDLER
# ============================================================

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    # Never leak stack traces to users.
    traceback.print_exception(
        type(context.error),
        context.error,
        context.error.__traceback__,
    )



async def cmd_addbalance(update, context):
    await admin_balance_command(update, context, True)

async def cmd_removebalance(update, context):
    await admin_balance_command(update, context, False)

# ============================================================
# MAIN
# ============================================================

def validate_config():
    if not BOT_TOKEN or BOT_TOKEN == "PASTE_BOT_TOKEN_HERE":
        raise SystemExit("Set BOT_TOKEN in bot.py before running.")
    if not ADMIN_IDS or ADMIN_IDS == {123456789}:
        raise SystemExit("Replace ADMIN_IDS in bot.py before running.")


def main():
    validate_config()
    init_db()

    async def post_init(application: Application):
        await recover_stale_games()

    app: Application = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    # User commands.
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("games", games_command))
    app.add_handler(CommandHandler("balance", balance_command))
    app.add_handler(CommandHandler("history", history_command))

    app.add_handler(CommandHandler("dice", cmd_dice))
    app.add_handler(CommandHandler("bowl", cmd_bowl))
    app.add_handler(CommandHandler("football", cmd_football))
    app.add_handler(CommandHandler("basketball", cmd_basketball))

    app.add_handler(CommandHandler("blackjack", blackjack_command))
    app.add_handler(CommandHandler("keno", keno_command))
    app.add_handler(CommandHandler("hilo", cmd_hilo))
    app.add_handler(CommandHandler("higherlower", cmd_higherlower))
    app.add_handler(CommandHandler("redblack", redblack_command))
    app.add_handler(CommandHandler("diceroulette", dice_roulette_command))
    app.add_handler(CommandHandler("plinko", plinko_command))
    app.add_handler(CommandHandler("mines", cmd_mines))
    app.add_handler(CommandHandler("diamonds", cmd_diamonds))
    app.add_handler(CommandHandler("crash", crash_command))
    app.add_handler(CommandHandler("tower", tower_command))
    app.add_handler(CommandHandler("roulette", roulette_command))
    app.add_handler(CommandHandler("bus", cmd_bus))
    app.add_handler(CommandHandler("battleship", battleship_command))

    # Admin commands.
    app.add_handler(CommandHandler("admin", admin_command))
    app.add_handler(CommandHandler("user", admin_user_command))
    app.add_handler(CommandHandler("addbalance", cmd_addbalance))
    app.add_handler(CommandHandler("removebalance", cmd_removebalance))
    app.add_handler(CommandHandler("ban", ban_command))
    app.add_handler(CommandHandler("unban", unban_command))
    app.add_handler(CommandHandler("mute", mute_command))
    app.add_handler(CommandHandler("unmute", unmute_command))
    app.add_handler(CommandHandler("pnl", pnl_command))
    app.add_handler(CommandHandler("gamestats", gamestats_command))
    app.add_handler(CommandHandler("ledger", ledger_command))
    app.add_handler(CommandHandler("broadcast", broadcast_command))

    # Callbacks.
    app.add_handler(CallbackQueryHandler(admin_callback, pattern=r"^adm:"))
    app.add_handler(CallbackQueryHandler(callback_handler))

    # Group moderation.
    app.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            moderation_message,
        ),
        group=10,
    )

    app.add_error_handler(error_handler)

    print("Bot starting...")
    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=False,
    )


if __name__ == "__main__":
    main()
