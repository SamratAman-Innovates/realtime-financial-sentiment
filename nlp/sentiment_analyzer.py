"""
Finance-tuned sentiment scoring.

General-purpose sentiment models under-react to financial language: "beat
estimates", "guidance cut", "short squeeze" carry strong directional meaning
that plain VADER misses. We extend VADER's lexicon with finance-specific
terms and expose a small, swappable interface (`analyze`) so this backend
can later be replaced with a transformer model (e.g. FinBERT) without
touching any of the Spark or producer code that calls it.
"""

from dataclasses import dataclass

import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# VADER's lexicon is downloaded once; safe to call repeatedly.
try:
    nltk.data.find("sentiment/vader_lexicon.zip")
except LookupError:
    nltk.download("vader_lexicon", quiet=True)


# Finance-specific lexicon additions, scored on VADER's -4..+4 scale.
FINANCE_LEXICON = {
    "beat estimates": 3.0,
    "beats estimates": 3.0,
    "beat expectations": 3.0,
    "misses estimates": -3.0,
    "missed estimates": -3.0,
    "guidance cut": -3.2,
    "raised guidance": 3.2,
    "raises guidance": 3.2,
    "bullish": 2.5,
    "bearish": -2.5,
    "short squeeze": 2.0,
    "sell-off": -2.3,
    "selloff": -2.3,
    "rally": 2.0,
    "plunge": -2.8,
    "plunges": -2.8,
    "soars": 2.6,
    "surges": 2.4,
    "downgrade": -2.2,
    "downgraded": -2.2,
    "upgrade": 2.2,
    "upgraded": 2.2,
    "layoffs": -2.4,
    "buyback": 1.8,
    "bankruptcy": -3.6,
    "record profit": 3.0,
    "record revenue": 3.0,
    "recall": -2.0,
    "lawsuit": -1.8,
    "antitrust": -1.6,
    "acquisition": 1.2,
    "merger": 1.0,
    "default": -3.0,
    "fraud": -3.4,
}


@dataclass
class SentimentResult:
    text: str
    compound: float          # -1.0 .. 1.0
    label: str                # "bullish" | "bearish" | "neutral"
    positive: float
    negative: float
    neutral: float


class FinancialSentimentAnalyzer:
    """Wraps VADER with a finance-domain lexicon boost."""

    def __init__(self):
        self._vader = SentimentIntensityAnalyzer()
        self._vader.lexicon.update(FINANCE_LEXICON)

    def analyze(self, text: str) -> SentimentResult:
        if not text or not text.strip():
            return SentimentResult(text=text, compound=0.0, label="neutral",
                                    positive=0.0, negative=0.0, neutral=1.0)

        scores = self._vader.polarity_scores(text)
        compound = scores["compound"]

        if compound >= 0.15:
            label = "bullish"
        elif compound <= -0.15:
            label = "bearish"
        else:
            label = "neutral"

        return SentimentResult(
            text=text,
            compound=compound,
            label=label,
            positive=scores["pos"],
            negative=scores["neg"],
            neutral=scores["neu"],
        )

    def analyze_batch(self, texts: list[str]) -> list[SentimentResult]:
        return [self.analyze(t) for t in texts]


def composite_signal(avg_sentiment: float, price_momentum_pct: float) -> str:
    """
    Fuses news sentiment with recent price momentum into a single
    plain-English signal for retail users.

    avg_sentiment: -1.0 .. 1.0 (from FinancialSentimentAnalyzer)
    price_momentum_pct: percent change over the aggregation window
    """
    score = (0.6 * avg_sentiment) + (0.4 * max(min(price_momentum_pct / 5.0, 1.0), -1.0))

    if score >= 0.35:
        return "Strong Bullish"
    if score >= 0.1:
        return "Bullish"
    if score <= -0.35:
        return "Strong Bearish"
    if score <= -0.1:
        return "Bearish"
    return "Mixed / Neutral"


if __name__ == "__main__":
    analyzer = FinancialSentimentAnalyzer()
    samples = [
        "Apple beats estimates and raises guidance for next quarter",
        "Tesla shares plunge after recall announcement",
        "Company reports flat earnings, in line with expectations",
    ]
    for s in samples:
        r = analyzer.analyze(s)
        print(f"{r.label:8s} ({r.compound:+.2f})  {s}")
