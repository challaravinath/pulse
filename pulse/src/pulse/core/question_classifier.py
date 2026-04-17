"""
★ iter16: Question Classifier — single routing brain for PULSE.

One file, one decision point, one source of truth.
Every question gets classified into exactly one bucket.
Each bucket maps to exactly one handler.

Buckets:
  META        → session/config info, no SQL needed
  COMPOUND    → multi-table briefing (WoW + regions + orgs)
  INVESTIGATE → cross-table analysis (why/what caused/anomalies)
  SINGLE      → single-table query (fast_router handles sub-routing)
  KUSTO       → live Kusto query (real-time / direct KQL)
  GREETING    → conversational response (hi, thanks, help)
"""

import re
import logging
from typing import Tuple, Optional

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# BUCKET DEFINITIONS — order matters (first match wins)
# ═══════════════════════════════════════════════════════════════════════════

_BUCKETS = [
    # ── GREETING: catch first so "hi" doesn't fall to SINGLE ──────────
    {
        'id': 'GREETING',
        'patterns': [
            r'^(hi|hello|hey|hey\s+there|howdy|good\s+(morning|afternoon|evening))[\s!?.]*$',
            r'^(thanks|thank\s*you|thx|cheers)[\s!?.]*$',
            r'^(what can you do|help|what are you|who are you)[\s!?.]*$',
            r'^(ok|okay|got it|cool|great|nice|perfect)[\s,!?.]*(?:thanks?!?)?[\s!?.]*$',
        ],
    },

    # ── KUSTO_DIRECT: explicit KQL or real-time requests ──────────────
    {
        'id': 'KUSTO',
        'patterns': [
            r'^\s*\w+\s*\|',                    # KQL syntax: "Events | count"
            r'\brun\b.*\bkql\b',                 # "run this KQL"
            r'\braw\s+events?\b',                # "raw events"
            r'\blast\s+\d+\s*(?:min|minute)',     # "last 30 minutes"
            r'\blast\s+\d+\s*(?:hour|hr)',        # "last 2 hours"
            r'\breal[\s-]?time\b',               # "real-time"
            r'\blive\s+(?:data|query|events)\b',  # "live data"
            r'\bexecute\s+(?:kql|query)\b',       # "execute KQL"
            r'\bwhich\s+orgs?\s+(?:use|have|send|generate|produce)',  # entity-org cross-tab
        ],
    },

    # ── META: session/config info — no data query needed ──────────────
    {
        'id': 'META',
        'patterns': [
            r'which\s+cluster',                   # "which cluster am I talking to"
            r'what\s+(?:data|tables?|info)\s+(?:do\s+)?(?:i|we)\s+have',
            r'how\s+(?:much|many)\s+data\b',      # "how much data"
            r'(?:what|which)\s+tables?\s+(?:are\s+)?(?:available|exist)',
            r'how\s+(?:many)\s+tables?\b',
            r'how\s+old\s+is\s+(?:this|the|my)\s+data',
            r'(?:what|which)\s+(?:time\s+)?range',
            r'am\s+i\s+connected',
            r'what\s+config',
            r'(?:data|connection)\s+status',
            r'what\s+(?:scope|period)',
            r'how\s+(?:fresh|stale|recent)',
            r'what\s+(?:am\s+i|are\s+we)\s+(?:looking|connected|talking)',
        ],
    },

    # ── INVESTIGATE: cross-table analysis, root cause, anomalies ──────
    {
        'id': 'INVESTIGATE',
        'patterns': [
            # Why/cause questions
            r'\bwhy\s+(?:is|did|are|has|does|was)',
            r'\bwhat\s+caused\b',
            r'\bwhat\s+changed\b',
            r'\bwhat\s+went\s+wrong\b',
            r'\broot\s+cause\b',

            # Health/anomaly checks
            r'\bany\s+(?:issues?|problems?|anomal|red\s+flag)',
            r'\banything\s+(?:wrong|broken|off|unusual)',
            r'\bis\s+(?:anything|something)\s+(?:wrong|broken|off|unusual)',
            r'\bsomething\b.*\b(?:off|wrong|broken)\b',
            r'\bhealth\s*(?:check|analysis|report)?\b',
            r'\bdiagnos[ei]',
            r'\binvestigat[ei]',
            r'\bconcern',
            r'\bworr(?:y|ied)',

            # Decline/spike investigation
            r'\b(?:why|what)\b.*\b(?:drop|decline|spike|surge|crash|fell|dip)\b',
            r'\b(?:who|which\s+orgs?)\s+(?:is|are)\s+affected\b',
            r'\bwhat(?:\s+is|\S*s)\s+(?:happening|going\s+on)\b',
        ],
    },

    # ── COMPOUND: multi-table briefing / overview / snapshot ──────────
    {
        'id': 'COMPOUND',
        'patterns': [
            # Snapshot/overview
            r'\bsnapshot\b',
            r'\boverview\b',
            r'\bstatus\s+update\b',
            r'\bcurrent\s+state\b',
            r'\bstate\s+of\s+(?:things|affairs|data)\b',
            r'\bpulse\s+check\b',
            r'\bbig\s+picture\b',

            # Briefing/summary
            r'\bbrief\s+(?:me|us|the)\b',
            r'\bbrief(?:ing)?\b',
            r'\bcatch\s+(?:me|us)\s+up\b',
            r'\bweekly\s+(?:summary|report|review|update)\b',
            r'\bmonthly\s+(?:summary|report|review|update)\b',
            r'\b(?:daily|morning)\s+(?:summary|report|review|update)\b',
            r'\bweek\s+in\s+review\b',

            # Key findings / highlights
            r'\bkey\s+(?:takeaway|finding|metric|insight|highlight)',
            r'\bhighlight',
            r'\brecap\b',
            r'\bwhat\s+should\s+(?:i|we)\s+know\b',
            r'\bgive\s+(?:me|us)\s+(?:the\s+)?(?:highlight|summary|update)',
            r'\bgive\s+(?:me|us)\s+(?:an?\s+)?(?:update|overview|snapshot)',

            # General "how are things" questions
            r'\bhow\s+are\s+(?:we|things)\s+doing\b',
            r'\bhow\s+is\s+everything\b',
            r'\bhow\s+(?:are\s+)?things\b',
            r'\bwhat\s+happened\b',               # "what happened on Jan 1"
            r'\bwhat\s*(?:\'s|s)\s+(?:the\s+)?(?:overall|general)\b',
            r'\bexecutive\s+(?:summary|brief|report)\b',
            r'\bmanage(?:r|ment)\s+(?:summary|report|brief|update)\b',
            r'\bsummar(?:y|ize|ise)\b',
        ],
    },

    # ── SINGLE: everything else → fast_router sub-routes ──────────────
    # This is the default — no patterns needed.
    # Fast router handles: ranking, trend, breakdown, lookup, total, compare
]


def classify(question: str) -> str:
    """
    Classify a question into one of: GREETING, KUSTO, META, INVESTIGATE, COMPOUND, SINGLE.

    Returns the bucket ID string.
    """
    q = question.lower().strip()

    for bucket in _BUCKETS:
        for pattern in bucket['patterns']:
            if re.search(pattern, q):
                logger.info(f"Classifier [{bucket['id']}] matched: {pattern!r} ← \"{question[:60]}\"")
                return bucket['id']

    logger.info(f"Classifier [SINGLE] (default) ← \"{question[:60]}\"")
    return 'SINGLE'


def classify_with_detail(question: str) -> Tuple[str, Optional[str]]:
    """
    Like classify(), but also returns the matched pattern for debugging.
    """
    q = question.lower().strip()

    for bucket in _BUCKETS:
        for pattern in bucket['patterns']:
            if re.search(pattern, q):
                return bucket['id'], pattern

    return 'SINGLE', None


# ═══════════════════════════════════════════════════════════════════════════
# QUESTION BANK — 150 representative questions, the test suite
# Each tuple: (question, expected_bucket)
# ═══════════════════════════════════════════════════════════════════════════

QUESTION_BANK = [
    # ── GREETING (10) ─────────────────────────────────────────────────
    ("hi", "GREETING"),
    ("hello", "GREETING"),
    ("hey there", "GREETING"),
    ("good morning", "GREETING"),
    ("thanks", "GREETING"),
    ("thank you!", "GREETING"),
    ("what can you do", "GREETING"),
    ("ok", "GREETING"),
    ("cool", "GREETING"),
    ("great, thanks!", "GREETING"),

    # ── META (20) ─────────────────────────────────────────────────────
    ("which cluster am I talking to", "META"),
    ("what data do I have", "META"),
    ("how much data do I have", "META"),
    ("what tables are available", "META"),
    ("how old is this data", "META"),
    ("what time range am I looking at", "META"),
    ("am I connected", "META"),
    ("what config am I using", "META"),
    ("data status", "META"),
    ("what scope am I on", "META"),
    ("how fresh is this data", "META"),
    ("how stale is my data", "META"),
    ("what am I looking at", "META"),
    ("what are we connected to", "META"),
    ("which tables exist", "META"),
    ("connection status", "META"),
    ("how recent is this data", "META"),
    ("what period does this cover", "META"),
    ("how many tables do I have", "META"),
    ("what info do I have", "META"),

    # ── COMPOUND (30) ─────────────────────────────────────────────────
    ("weekend snapshot", "COMPOUND"),
    ("give me a snapshot of 30 days", "COMPOUND"),
    ("snapshot for today", "COMPOUND"),
    ("weekly summary", "COMPOUND"),
    ("monthly report", "COMPOUND"),
    ("how are we doing", "COMPOUND"),
    ("whats the current state", "COMPOUND"),
    ("brief me", "COMPOUND"),
    ("catch me up", "COMPOUND"),
    ("key takeaways", "COMPOUND"),
    ("what should I know", "COMPOUND"),
    ("give me the highlights", "COMPOUND"),
    ("executive summary", "COMPOUND"),
    ("management summary", "COMPOUND"),
    ("overview please", "COMPOUND"),
    ("status update", "COMPOUND"),
    ("week in review", "COMPOUND"),
    ("daily report", "COMPOUND"),
    ("morning summary", "COMPOUND"),
    ("give me a summary", "COMPOUND"),
    ("summarize everything", "COMPOUND"),
    ("what's the big picture", "COMPOUND"),
    ("state of things", "COMPOUND"),
    ("recap for me", "COMPOUND"),
    ("pulse check", "COMPOUND"),
    ("how is everything", "COMPOUND"),
    ("how are things doing", "COMPOUND"),
    ("brief the team", "COMPOUND"),
    ("give us an update", "COMPOUND"),
    ("what's the overall status", "COMPOUND"),

    # ── INVESTIGATE (30) ──────────────────────────────────────────────
    ("why is activity declining", "INVESTIGATE"),
    ("what caused the spike on Feb 24", "INVESTIGATE"),
    ("why did events drop", "INVESTIGATE"),
    ("any anomalies", "INVESTIGATE"),
    ("any issues today", "INVESTIGATE"),
    ("anything wrong", "INVESTIGATE"),
    ("is anything broken", "INVESTIGATE"),
    ("what changed this week", "INVESTIGATE"),
    ("diagnose the drop", "INVESTIGATE"),
    ("investigate EMEA decline", "INVESTIGATE"),
    ("which orgs are affected", "INVESTIGATE"),
    ("what went wrong", "INVESTIGATE"),
    ("why is EMEA down", "INVESTIGATE"),
    ("root cause of the decline", "INVESTIGATE"),
    ("any red flags", "INVESTIGATE"),
    ("any concerns", "INVESTIGATE"),
    ("what is happening with events", "INVESTIGATE"),
    ("why did active orgs drop", "INVESTIGATE"),
    ("what caused the crash on Monday", "INVESTIGATE"),
    ("health check", "INVESTIGATE"),
    ("health analysis", "INVESTIGATE"),
    ("is something off", "INVESTIGATE"),
    ("why are numbers down", "INVESTIGATE"),
    ("what's going on with the data", "INVESTIGATE"),
    ("investigate the spike", "INVESTIGATE"),
    ("why is there a dip", "INVESTIGATE"),
    ("are we worried about this trend", "INVESTIGATE"),
    ("diagnose this pattern", "INVESTIGATE"),
    ("any problems in EMEA", "INVESTIGATE"),
    ("what went wrong last week", "INVESTIGATE"),

    # ── SINGLE (40) ──────────────────────────────────────────────────
    ("top 10 orgs", "SINGLE"),
    ("bottom 5 orgs", "SINGLE"),
    ("show the trend", "SINGLE"),
    ("daily trend", "SINGLE"),
    ("events by region", "SINGLE"),
    ("browser breakdown", "SINGLE"),
    ("entity types", "SINGLE"),
    ("total events", "SINGLE"),
    ("how many active orgs", "SINGLE"),
    ("compare regions", "SINGLE"),
    ("top orgs in EMEA", "SINGLE"),
    ("how is EMEA doing", "SINGLE"),
    ("how is dde25578-c8f1-ee11-a1fa-000d3a doing", "SINGLE"),
    ("events last 7 days", "SINGLE"),
    ("this weeks data", "SINGLE"),
    ("show me top 10 organizations", "SINGLE"),
    ("which region has the most events", "SINGLE"),
    ("biggest orgs by event count", "SINGLE"),
    ("show active orgs over time", "SINGLE"),
    ("EMEA vs NAM", "SINGLE"),
    ("break down by browser", "SINGLE"),
    ("trend for last 30 days", "SINGLE"),
    ("compare this week to last", "SINGLE"),
    ("top entities", "SINGLE"),
    ("bottom regions", "SINGLE"),
    ("most active entities", "SINGLE"),
    ("show me Chrome usage", "SINGLE"),
    ("how is GBR doing", "SINGLE"),
    ("org ranking", "SINGLE"),
    ("daily active orgs", "SINGLE"),
    ("events per day", "SINGLE"),
    ("show me the data", "SINGLE"),
    ("average events per org", "SINGLE"),
    ("show all regions", "SINGLE"),
    ("list all entities", "SINGLE"),
    ("top 20 orgs by events", "SINGLE"),
    ("events last 14 days", "SINGLE"),
    ("show last month", "SINGLE"),
    ("give me 30 days data", "SINGLE"),
    ("how is SCORE doing", "SINGLE"),

    # ── KUSTO (10) ───────────────────────────────────────────────────
    ("which orgs use app_telemetry", "KUSTO"),
    ("show me raw events for org X in last hour", "KUSTO"),
    ("run this KQL: Events | count", "KUSTO"),
    ("events in the last 30 minutes", "KUSTO"),
    ("real-time event stream", "KUSTO"),
    ("live data for last 2 hours", "KUSTO"),
    ("Events | where timestamp > ago(1h) | count", "KUSTO"),
    ("execute KQL against production", "KUSTO"),
    ("raw events from this morning", "KUSTO"),
    ("last 15 minutes of data", "KUSTO"),
]


def run_test_bank() -> Tuple[int, int, list]:
    """
    Run every question in the bank against the classifier.
    Returns (pass_count, total, failures_list).
    """
    passed = 0
    total = len(QUESTION_BANK)
    failures = []

    for question, expected in QUESTION_BANK:
        result, pattern = classify_with_detail(question)
        if result == expected:
            passed += 1
        else:
            failures.append({
                'question': question,
                'expected': expected,
                'got': result,
                'matched_pattern': pattern,
            })

    return passed, total, failures


if __name__ == '__main__':
    passed, total, failures = run_test_bank()
    print(f"\n{'='*60}")
    print(f"QUESTION BANK TEST: {passed}/{total} ({100*passed/total:.0f}%)")
    print(f"{'='*60}\n")

    if failures:
        print(f"FAILURES ({len(failures)}):")
        for f in failures:
            print(f"  ❌ \"{f['question']}\"")
            print(f"     Expected: {f['expected']}, Got: {f['got']}")
            if f['matched_pattern']:
                print(f"     Matched: {f['matched_pattern']}")
            print()
    else:
        print("ALL PASS ✅")
