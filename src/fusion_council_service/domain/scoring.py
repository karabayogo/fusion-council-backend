"""Scoring functions for candidate evaluation and fusion synthesis."""

import math
from typing import Optional


def cosine_similarity(text_a: str, text_b: str) -> float:
    """Simple word-overlap cosine similarity between two texts.
    Used as a quick proxy for semantic similarity.
    Returns 0.0–1.0.
    """
    words_a = set(text_a.lower().split())
    words_b = set(text_b.lower().split())
    if not words_a or not words_b:
        return 0.0
    intersection = words_a & words_b
    if not intersection:
        return 0.0
    denom = math.sqrt(len(words_a)) * math.sqrt(len(words_b))
    return len(intersection) / denom if denom > 0 else 0.0


def compute_pairwise_agreement(candidates: list[dict]) -> float:
    """Compute pairwise agreement score among candidates (0.0–1.0).
    Higher = more agreement. Used to decide if debate is needed in council mode.
    Threshold for debate: agreement < 0.55
    """
    if len(candidates) < 2:
        return 1.0

    texts = [c.get("normalized_answer", "") or c.get("raw_answer", "") or "" for c in candidates]
    pairs = 0
    total_sim = 0.0
    for i in range(len(texts)):
        for j in range(i + 1, len(texts)):
            total_sim += cosine_similarity(texts[i], texts[j])
            pairs += 1

    return total_sim / pairs if pairs > 0 else 0.0


def select_best_candidate(candidates: list[dict]) -> Optional[dict]:
    """Select the best candidate from a list based on heuristics.
    Priority: succeeded > failed; then by output_tokens (longer = more thorough).
    """
    succeeded = [c for c in candidates if c.get("status") == "succeeded"]
    if not succeeded:
        # All failed — pick the one with the longest error message (most info)
        failed = [c for c in candidates if c.get("status") == "failed"]
        return failed[0] if failed else None

    # For now, pick the succeeded candidate with the longest answer
    succeeded.sort(key=lambda c: len(c.get("raw_answer", "") or ""), reverse=True)
    return succeeded[0]


def build_fusion_prompt(prompt: str, candidates: list[dict]) -> str:
    """Build the synthesis prompt for fusion mode.
    Combines the original prompt with candidate answers.
    """
    parts = [f"Original question: {prompt}\n"]
    parts.append("Below are answers from multiple AI models:\n")
    for i, c in enumerate(candidates, 1):
        alias = c.get("alias", f"model-{i}")
        answer = c.get("raw_answer", "") or c.get("normalized_answer", "") or "No answer"
        status = c.get("status", "unknown")
        parts.append(f"---\nModel {i} ({alias}, status: {status}):\n{answer}\n")
    parts.append("---\n")
    parts.append(
        "Synthesize a single best answer from the above. "
        "Where models disagree, explain the disagreement and give your best judgment. "
        "Cite which model(s) supported each claim where possible."
    )
    return "\n".join(parts)


def build_council_synthesis_prompt(
    prompt: str,
    first_opinions: list[dict],
    peer_reviews: list[dict],
    debate_candidates: Optional[list[dict]] = None,
) -> str:
    """Build the synthesis prompt for council mode."""
    parts = [f"Original question: {prompt}\n"]

    parts.append("## First Opinions\n")
    for i, c in enumerate(first_opinions, 1):
        alias = c.get("alias", f"model-{i}")
        answer = c.get("raw_answer", "") or "No answer"
        parts.append(f"### {alias}:\n{answer}\n")

    parts.append("## Peer Reviews\n")
    for i, c in enumerate(peer_reviews, 1):
        alias = c.get("alias", f"reviewer-{i}")
        answer = c.get("raw_answer", "") or "No review"
        parts.append(f"### {alias}:\n{answer}\n")

    if debate_candidates:
        parts.append("## Debate\n")
        for i, c in enumerate(debate_candidates, 1):
            alias = c.get("alias", f"debater-{i}")
            answer = c.get("raw_answer", "") or "No debate input"
            parts.append(f"### {alias}:\n{answer}\n")

    parts.append("---\n")
    parts.append(
        "You are the Council Chair. Synthesize a single authoritative answer. "
        "Address all major points of agreement and disagreement. "
        "Flag any unresolved contradictions. "
        "Assign a confidence level (high/medium/low) to each major claim."
    )
    return "\n".join(parts)


def build_verification_prompt(prompt: str, synthesis_answer: str) -> str:
    """Build the verification prompt for checking the synthesis."""
    return (
        f"Original question: {prompt}\n\n"
        f"Proposed answer:\n{synthesis_answer}\n\n"
        "You are a verification agent. Evaluate this answer for:\n"
        "1. Factual correctness\n"
        "2. Logical consistency\n"
        "3. Completeness relative to the question\n"
        "4. Whether you can confirm, abstain (insufficient evidence), or reject\n\n"
        "Reply with JSON: {\"verdict\": \"confirm\"|\"abstain\"|\"reject\", "
        "\"confidence\": 0.0-1.0, \"issues\": [...]}"
    )


def build_peer_review_prompt(prompt: str, original_answer: str, reviewer_alias: str) -> str:
    """Build a peer review prompt for council mode."""
    return (
        f"Original question: {prompt}\n\n"
        f"Answer to review (from another model):\n{original_answer}\n\n"
        f"You are {reviewer_alias}, a peer reviewer. Critically evaluate this answer. "
        "Check for errors, omissions, logical fallacies, and unsupported claims. "
        "Suggest improvements if any."
    )


def build_debate_prompt(prompt: str, opinions: list[dict]) -> str:
    """Build a debate prompt when models disagree significantly."""
    parts = [f"Original question: {prompt}\n"]
    parts.append("The following models have given conflicting answers. Debate the key points of disagreement:\n\n")
    for i, c in enumerate(opinions, 1):
        alias = c.get("alias", f"model-{i}")
        answer = c.get("raw_answer", "") or "No answer"
        parts.append(f"**{alias}** says:\n{answer}\n\n")
    parts.append(
        "Each participant: defend your position or concede if the other side has a stronger argument. "
        "Be concise. Focus on the specific points of disagreement."
    )
    return "\n".join(parts)