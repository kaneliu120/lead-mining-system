"""
GeminiEnricher — AI enrichment engine
Uses Gemini 2.0 Flash for deep analysis of LeadRaw, outputting sales insights
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import List, Optional

try:
    import google.generativeai as genai
except ImportError:
    genai = None    # type: ignore

from app.models.lead import EnrichedLead, LeadRaw

logger = logging.getLogger(__name__)

_ENRICH_PROMPT = """\
You are a B2B sales intelligence analyst specializing in Philippine SME businesses.
Analyze the following business information and provide enrichment data.

Business Name: {business_name}
Industry Keyword: {industry_keyword}
Address: {address}
Phone: {phone}
Website: {website}
Rating: {rating} ({review_count} reviews)
Additional Info: {metadata}

Return a JSON object with EXACTLY these fields (no extra text):
{{
  "industry_category": "string (e.g., Food & Beverage, Retail, Services)",
  "business_size": "micro|small|medium",
  "pain_points": ["string", ...],
  "value_proposition": "string (1-2 sentences about how we could help them)",
  "recommended_product": "string (specific product/service to pitch)",
  "outreach_angle": "string (specific hook/angle for first email)",
  "score": 0-100,
  "score_reason": "string (brief explanation)",
  "best_contact_time": "string (optional business hours insight)"
}}

Focus on Philippines market context. Score criteria:
- 90-100: Clear digital transformation need, decision-maker reachable
- 70-89: Good fit, moderate digital maturity  
- 50-69: Possible fit, needs qualification
- Below 50: Low priority
"""


class GeminiEnricher:
    """
    Gemini 2.0 Flash enrichment engine.
    Each enrich() call consumes ~500 input tokens, ~300 output tokens.
    Cost estimate: $0.075/1M input + $0.30/1M output ≈ $0.000128/call ≈ $0.13/1K calls
    """

    def __init__(
        self,
        api_key: str,
        model: str = "gemini-2.5-flash",
        max_concurrent: int = 10,
        temperature: float = 0.2,
    ):
        if genai is None:
            raise ImportError("google-generativeai not installed. Run: pip install google-generativeai")

        self.model_name = model
        self.temperature = temperature
        self._semaphore = asyncio.Semaphore(max_concurrent)

        genai.configure(api_key=api_key)
        # max_output_tokens set to a large value to prevent thinking models (gemini-2.5-flash etc.)
        # from consuming token quota on the thinking process and truncating the output JSON
        self._model = genai.GenerativeModel(
            model_name=model,
            generation_config=genai.types.GenerationConfig(
                temperature=temperature,
                max_output_tokens=8192,
                response_mime_type="application/json",
            ),
        )
        logger.info(f"GeminiEnricher initialized with model={model}")

    async def enrich(self, lead: LeadRaw) -> Optional[EnrichedLead]:
        """
        AI-enrich a single LeadRaw.
        Returns EnrichedLead; returns None on failure (caller decides whether to skip).
        """
        async with self._semaphore:
            prompt = _ENRICH_PROMPT.format(
                business_name=lead.business_name,
                industry_keyword=lead.industry_keyword,
                address=lead.address or "",
                phone=lead.phone or "",
                website=lead.website or "",
                rating=lead.rating if lead.rating is not None else "N/A",
                review_count=lead.review_count or 0,
                metadata=json.dumps(lead.metadata or {}, ensure_ascii=False)[:500],
            )
            try:
                # Gemini SDK is synchronous — run in thread pool
                response = await asyncio.to_thread(
                    self._model.generate_content, prompt
                )
                raw_json = response.text.strip()

                # Fix markdown code block (e.g. ```json\n{...}\n```)
                if raw_json.startswith("```"):
                    raw_json = raw_json.split("```")[1].removeprefix("json").strip()

                # Thinking models (e.g. gemini-2.5-flash) may prepend thinking text before the JSON —
                # use regex to extract the first complete JSON object
                if not raw_json.startswith("{"):
                    m = re.search(r'\{.*\}', raw_json, re.DOTALL)
                    if m:
                        raw_json = m.group(0)

                enrichment: dict = json.loads(raw_json)

            except json.JSONDecodeError as exc:
                logger.warning(
                    f"GeminiEnricher: JSON parse error for '{lead.business_name}': {exc}"
                )
                return None
            except Exception as exc:
                logger.error(
                    f"GeminiEnricher: API error for '{lead.business_name}': {exc}"
                )
                return None

            return EnrichedLead(
                # --- Inherited LeadRaw fields ---
                source=lead.source,
                business_name=lead.business_name,
                industry_keyword=lead.industry_keyword,
                address=lead.address,
                phone=lead.phone,
                website=lead.website,
                email=lead.email,
                rating=lead.rating,
                review_count=lead.review_count,
                lat=lead.lat,
                lng=lead.lng,
                google_maps_url=lead.google_maps_url,
                metadata=lead.metadata,
                # --- AI enrichment fields ---
                industry_category=enrichment.get("industry_category", ""),
                business_size=enrichment.get("business_size", ""),
                pain_points=enrichment.get("pain_points", []),
                value_proposition=enrichment.get("value_proposition", ""),
                recommended_product=enrichment.get("recommended_product", ""),
                outreach_angle=enrichment.get("outreach_angle", ""),
                score=max(0, min(100, int(enrichment.get("score", 0)))),
                score_reason=enrichment.get("score_reason", ""),
                best_contact_time=enrichment.get("best_contact_time", ""),
                enriched=True,
            )

    async def enrich_batch(
        self,
        leads: List[LeadRaw],
        skip_below_score: int = 0,
    ) -> List[EnrichedLead]:
        """
        Concurrently enrich multiple leads (limited by max_concurrent semaphore).
        skip_below_score: secondary filter — discard low-score results
        """
        tasks = [self.enrich(lead) for lead in leads]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        enriched = []
        for i, r in enumerate(results):
            if isinstance(r, Exception):
                logger.warning(
                    f"GeminiEnricher: enrich() failed for lead "
                    f"'{leads[i].business_name}': {r}"
                )
                continue
            if r is not None and r.score >= skip_below_score:
                enriched.append(r)

        logger.info(
            f"GeminiEnricher: enriched {len(enriched)}/{len(leads)} leads "
            f"(filter score >= {skip_below_score})"
        )
        return enriched

    async def health_check(self) -> bool:
        """Send a minimal request to verify the API Key is valid"""
        try:
            resp = await asyncio.to_thread(
                self._model.generate_content,
                '{"test": true} — respond with {"ok": true}',
            )
            return '"ok"' in resp.text or "ok" in resp.text.lower()
        except Exception:
            return False
