import asyncio
import logging
import time
from llm.LLMInterface import Chat, make_backend
from scripts.DataHandler import DataHandler
from scripts.RiskSummarizer import summarize_risk
from scripts.GeoScope import get_dataset_filters
from prompts.app_prompts import (
    get_decision_prompt,
    get_risk_summary_decision_prompt,
    get_conversational_meta_prompt,
    get_followup_prompt,
    get_loading_datasets_prompt,
    get_conversational_fallback_prompt
)
from llm.LLMParser import route_query_to_datasets_multi
from config.logger import logger
from scripts.ConversationalUnit import (
    DecideModeUnit, DecideReuseAddressesUnit, DecideReuseDatasetsUnit, ParseQueryUnit, DecideIntersectionAnalysisUnit,
    DataPreviewUnit, FilterDatasetsUnit, DecideRiskSummaryUnit, RiskSummaryUnit, DecideShowDataUnit, FollowupUnit,
    ConversationalAnswerUnit, ParsedResultFormatUnit, ResolveBBLsUnit, AggregateSurroundingBBLsUnit,
    BuildDatasetFiltersUnit, SurroundingDecisionUnit
)

class ConversationalAgent:
    def __init__(self, chat_backend=None, debug=False):
        self.llm_chat = chat_backend or Chat(make_backend(provider="gemini"))
        self.chat_history = []
        self.last_parsed_result = None
        self.last_context = None  # store final context for logging
        self.debug = debug
        # Instantiate units
        self.units = {
            "decide_mode": DecideModeUnit(self.llm_chat),
            "decide_reuse_addresses": DecideReuseAddressesUnit(self.llm_chat),
            "decide_reuse_datasets": DecideReuseDatasetsUnit(self.llm_chat),
            "parse_query": ParseQueryUnit(),
            "decide_intersection": DecideIntersectionAnalysisUnit(self.llm_chat),
            "parsed_result_format": ParsedResultFormatUnit(),
            "data_preview": DataPreviewUnit(self.llm_chat),
            "surrounding_decision": SurroundingDecisionUnit(self.llm_chat),
            "resolve_bbls": ResolveBBLsUnit(),
            "aggregate_surrounding": AggregateSurroundingBBLsUnit(),
            "build_dataset_filters": BuildDatasetFiltersUnit(),
            "filter_datasets": FilterDatasetsUnit(),
            "decide_risk_summary": DecideRiskSummaryUnit(self.llm_chat),
            "risk_summary": RiskSummaryUnit(),
            "decide_show_data": DecideShowDataUnit(self.llm_chat),
            "followup": FollowupUnit(self.llm_chat),
            "conversational_answer": ConversationalAnswerUnit(self.llm_chat)
        }

    @staticmethod
    def _record_timing(context, unit_key: str, duration: float) -> None:
        timings = context.setdefault("timings", {})
        timings[f"{unit_key}_secs"] = duration

    def warm_cache(self) -> None:
        """Prime geospatial resources so the first user request is fast."""
        try:
            from scripts.GeoBundle import geo_from_bbl
            from data.pluto import load_pluto_lookup

            # Warm PLUTO lookups and GeoBundle cache using a canonical Midtown BBL.
            load_pluto_lookup()
            geo_from_bbl("1000000001")
            logger.info("Geo resources warmed successfully.")
        except Exception as exc:
            logger.warning("Geo warm-up skipped due to error: %s", exc)

    async def _run_unit(self, unit_key: str, context):
        start = time.perf_counter()
        try:
            return await self.units[unit_key].run(context)
        finally:
            self._record_timing(context, unit_key, time.perf_counter() - start)

    async def stream(self, user_text):
        """Async generator yielding content chunks as soon as they are ready."""
        self.chat_history.append(user_text)
        history_str = "\n".join(self.chat_history)
        context = {
            "user_text": user_text,
            "chat_history": history_str,
            "llm_chat": self.llm_chat,
            "last_parsed_result": self.last_parsed_result,
            "timings": {},
        }
        # Decide mode
        context = await self._run_unit("decide_mode", context)
        mode = context["mode"]
        logger.info(f"Agent decision: {mode}")

        if mode == "conversational":
            context = await self._run_unit("conversational_answer", context)
            yield context["conversational_response"]
            if "parsed_result" in context:
                context = await self._run_unit("followup", context)
                yield context["followup_response"]
            self.last_context = context  # store final context
            return

        # Decide reuse of addresses and datasets
        context = await self._run_unit("decide_reuse_addresses", context)
        reuse_addresses_decision = context["reuse_addresses_decision"]
        logger.info(f"Reuse addresses decision: {reuse_addresses_decision}")

        context = await self._run_unit("decide_reuse_datasets", context)
        reuse_datasets_decision = context["reuse_datasets_decision"]
        logger.info(f"Reuse datasets decision: {reuse_datasets_decision}")

        need_fresh_parse = (
            not self.last_parsed_result
            or reuse_addresses_decision != "reuse"
            or reuse_datasets_decision != "reuse"
        )

        if need_fresh_parse:
            context = await self._run_unit("parse_query", context)
            parsed_result = context["parsed_result"]
            if reuse_addresses_decision == "reuse" and self.last_parsed_result:
                parsed_result["address"] = list(self.last_parsed_result.get("address") or [])
            if reuse_datasets_decision == "reuse" and self.last_parsed_result:
                parsed_result["dataset_names"] = list(self.last_parsed_result.get("dataset_names") or [])
                source_categories = (
                    self.last_parsed_result.get("categories")
                    or parsed_result.get("categories")
                    or []
                )
                parsed_result["categories"] = list(source_categories)
            context["parsed_result"] = parsed_result
            self.last_parsed_result = parsed_result
            logger.info(f"Parsed result: {parsed_result}")
        else:
            context["parsed_result"] = self.last_parsed_result or {}
            logger.info("Reusing last parsed datasets and addresses without reparsing.")

        context["last_parsed_result"] = self.last_parsed_result

        context = await self._run_unit("decide_intersection", context)
        intersection_decision = context.get("intersection_decision")
        logger.info(f"Intersection decision: {intersection_decision}")

        # Run initial fan-out in parallel: formatting, data preview, BBL resolution
        initial_tasks = [
            asyncio.create_task(self._run_unit("parsed_result_format", context)),
            asyncio.create_task(self._run_unit("data_preview", context)),
            asyncio.create_task(self._run_unit("resolve_bbls", context)),
        ]
        await asyncio.gather(*initial_tasks)

        # Format & yield parsed result
        if self.debug:
            yield context["formatted_parsed_result"]

        # Data preview intro
        if self.debug:
            yield context["llm_data_response"]

        # Surrounding decision (LLM) after BBL resolution
        context = await self._run_unit("surrounding_decision", context)
        surrounding_decision = context["surrounding_decision"]
        logger.info(f"Surrounding decision: {surrounding_decision}")

        aggregate_task = None
        if surrounding_decision in ("include_surrounding", "use_span"):
            aggregate_task = asyncio.create_task(self._run_unit("aggregate_surrounding", context))
        else:
            context["nearby_bbls"] = context.get("resolved_bbls", [])  # use only target

        if aggregate_task:
            await aggregate_task

        context = await self._run_unit("build_dataset_filters", context)

        # Filter datasets
        context = await self._run_unit("filter_datasets", context)
        filtered_datasets = context["filtered_datasets"]
        data_samples = context["data_samples"]

        # Decide risk summary and show data in parallel
        decision_tasks = [
            asyncio.create_task(self._run_unit("decide_risk_summary", context)),
            asyncio.create_task(self._run_unit("decide_show_data", context)),
        ]
        await asyncio.gather(*decision_tasks)

        risk_decision = context.get("risk_decision")
        logger.info(f"Risk summary decision: {risk_decision}")

        show_data_decision = context.get("show_data_decision")
        logger.info(f"Show data decision: {show_data_decision}")

        risk_summary_task = None
        if risk_decision == "risk_summary_needed":
            risk_summary_task = asyncio.create_task(self._run_unit("risk_summary", context))

        preview_tasks = []
        if show_data_decision == "show_data":
            preview_tasks = [
                asyncio.create_task(
                    asyncio.to_thread(
                        self._render_dataset_preview,
                        ds,
                        data_samples.get(ds.name),
                    )
                )
                for ds in filtered_datasets
            ]

        if risk_summary_task:
            await risk_summary_task
            yield f"{context['risk_summary']}"

        if preview_tasks:
            previews = await asyncio.gather(*preview_tasks)
            for ds_name, description, preview in previews:
                yield f"**{ds_name}**\n{description}\n\nPreview:\n{preview}"

        # Followup
        context = await self._run_unit("followup", context)
        yield context["followup_response"]
        self.last_context = context  # store final context for logging

    async def run(self, user_text):
        """Backward compatible: collect all streamed chunks into final response + followup."""
        parts = []
        async for chunk in self.stream(user_text):
            parts.append(chunk)
        # Last part is followup, separate it
        if parts:
            followup = parts[-1]
            response_parts = parts[:-1]
        else:
            followup = ""
            response_parts = []
        return response_parts, followup

    @staticmethod
    def _render_dataset_preview(ds, df_head):
        if hasattr(df_head, 'to_markdown'):
            try:
                preview = df_head.to_markdown(index=False)
            except Exception as exc:
                logger.warning(f"Failed to render markdown for {ds.name}: {exc}")
                preview = str(df_head)
        else:
            preview = str(df_head)
        return ds.name, ds.description, preview

    def set_debug(self, enabled: bool) -> None:
        self.debug = bool(enabled)
