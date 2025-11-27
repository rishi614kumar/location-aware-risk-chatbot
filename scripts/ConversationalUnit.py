import asyncio
from typing import Any, Dict, Set
from config.logger import logger


def _normalize_choice(value: str, allowed: Set[str], default: str) -> str:
    """Normalize model output to a valid keyword."""
    if not value:
        return default
    cleaned = value.strip().lower()
    cleaned = cleaned.splitlines()[0]
    cleaned = cleaned.split()[0]
    cleaned = cleaned.strip(".,;!:")
    return cleaned if cleaned in allowed else default

class ConversationalUnit:
    def __init__(self, name: str):
        self.name = name

    async def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError("Subclasses must implement run().")

# --- Individual units for each operation ---

class DecideModeUnit(ConversationalUnit):
    def __init__(self, llm_chat):
        super().__init__("decide_mode")
        self.llm_chat = llm_chat
    async def run(self, context):
        from prompts.app_prompts import get_decision_prompt
        user_text = context["user_text"]
        chat_history = context["chat_history"]
        decision_prompt = get_decision_prompt(user_text, chat_history)
        mode = (await asyncio.to_thread(self.llm_chat.ask, decision_prompt)).strip().lower()
        context["mode"] = mode
        return context

class DecideReuseAddressesUnit(ConversationalUnit):
    def __init__(self, llm_chat):
        super().__init__("decide_reuse_addresses")
        self.llm_chat = llm_chat

    async def run(self, context):
        from prompts.app_prompts import get_reuse_address_decision_prompt

        user_text = context["user_text"]
        chat_history = context["chat_history"]
        last_parsed_result = context.get("last_parsed_result") or {}
        last_addresses = last_parsed_result.get("address") or []

        if not last_addresses:
            decision = "reparse"
        else:
            prompt = get_reuse_address_decision_prompt(user_text, chat_history, last_addresses)
            raw_decision = await asyncio.to_thread(self.llm_chat.ask, prompt)
            decision = _normalize_choice(raw_decision, {"reuse", "reparse"}, "reuse")

        context["reuse_addresses_decision"] = decision
        return context


class DecideReuseDatasetsUnit(ConversationalUnit):
    def __init__(self, llm_chat):
        super().__init__("decide_reuse_datasets")
        self.llm_chat = llm_chat

    async def run(self, context):
        from prompts.app_prompts import get_reuse_dataset_decision_prompt

        user_text = context["user_text"]
        chat_history = context["chat_history"]
        last_parsed_result = context.get("last_parsed_result") or {}
        last_datasets = last_parsed_result.get("dataset_names") or []

        if not last_datasets:
            decision = "reparse"
        else:
            prompt = get_reuse_dataset_decision_prompt(user_text, chat_history, last_datasets)
            raw_decision = await asyncio.to_thread(self.llm_chat.ask, prompt)
            decision = _normalize_choice(raw_decision, {"reuse", "reparse"}, "reuse")

        context["reuse_datasets_decision"] = decision
        return context

class ParseQueryUnit(ConversationalUnit):
    def __init__(self):
        super().__init__("parse_query")
    async def run(self, context):
        from llm.LLMParser import route_query_to_datasets_multi
        user_text = context["user_text"]
        result = await asyncio.to_thread(route_query_to_datasets_multi, user_text)
        context["parsed_result"] = result
        return context

class DataPreviewUnit(ConversationalUnit):
    def __init__(self, llm_chat):
        super().__init__("data_preview")
        self.llm_chat = llm_chat
    async def run(self, context):
        from scripts.DataHandler import DataHandler
        from prompts.app_prompts import get_loading_datasets_prompt
        result = context["parsed_result"]
        datasets = result.get('dataset_names', [])
        handler = DataHandler(datasets)
        context["handler"] = handler
        prompt = get_loading_datasets_prompt(handler)
        context["llm_data_response"] = await asyncio.to_thread(self.llm_chat.ask, prompt)
        return context

class ResolveBBLsUnit(ConversationalUnit):
    def __init__(self):
        super().__init__("resolve_bbls")
    async def run(self, context):
        from scripts.GeoScope import resolve_geo_bundles_from_addresses, resolve_bbls_from_street_spans
        addresses = context.get("parsed_result", {}).get('address', [])
        resolution = await asyncio.to_thread(resolve_geo_bundles_from_addresses, addresses)
        resolved_bbls = resolution.bbls
        span_bbls = resolve_bbls_from_street_spans(resolution)
        bundle_lookup = {bundle.bbl: bundle for bundle in resolution.bundles if bundle and bundle.bbl}
        context["resolved_bbls"] = resolved_bbls
        context["span_bbls"] = span_bbls
        context["geo_bundles"] = resolution.bundles
        context["bundle_lookup"] = bundle_lookup
        logger.info(f"Resolved BBLs: {resolved_bbls}")
        if span_bbls:
            logger.info(f"Street span BBLs: {len(span_bbls)}")
        return context

class AggregateSurroundingBBLsUnit(ConversationalUnit):
    def __init__(self):
        super().__init__("aggregate_surrounding")
    async def run(self, context):
        from scripts.GeoScope import aggregate_surrounding_bbls
        resolved_bbls = context.get("resolved_bbls", [])
        span_bbls = context.get("span_bbls") or []

        if span_bbls:
            nearby_bbls = span_bbls
            context["force_nearby_span"] = True
        else:
            nearby_bbls = await asyncio.to_thread(aggregate_surrounding_bbls, resolved_bbls, True)
            context["force_nearby_span"] = False
        context["nearby_bbls"] = nearby_bbls
        logger.info(f"Nearby BBLs: {len(nearby_bbls)}")
        return context

class BuildDatasetFiltersUnit(ConversationalUnit):
    def __init__(self):
        super().__init__("build_dataset_filters")
    async def run(self, context):
        from scripts.GeoScope import build_dataset_filters_for_handler
        handler = context["handler"]
        resolved_bbls = context.get("resolved_bbls", [])
        nearby_bbls = context.get("nearby_bbls", [])
        bundle_lookup = context.get("bundle_lookup")
        force_nearby_span = context.get("force_nearby_span", False)
        dataset_filters = await asyncio.to_thread(
            build_dataset_filters_for_handler,
            handler,
            resolved_bbls,
            nearby_bbls,
            bundle_lookup=bundle_lookup,
            force_nearby=force_nearby_span,
        )
        context["dataset_filters"] = dataset_filters
        return context

class FilterDatasetsUnit(ConversationalUnit):
    def __init__(self, max_concurrent: int = 4):
        super().__init__("filter_datasets")
        self.max_concurrent = max_concurrent
    async def run(self, context):
        handler = context["handler"]
        dataset_filters = context["dataset_filters"]
        import pandas as pd

        semaphore = asyncio.Semaphore(max(1, self.max_concurrent))
        filtered_datasets = []
        data_samples = {}

        async def _process_dataset(ds):
            async with semaphore:
                filter_kwargs = dataset_filters.get(ds.name, {})
                where = filter_kwargs.get("where")
                limit = filter_kwargs.get("limit")
                try:
                    df_full = await asyncio.to_thread(ds.df_filtered, where, limit)
                except Exception as e:
                    logger.warning(f"Filtered fetch failed for {ds.name}: {e}; using empty DataFrame")
                    df_full = pd.DataFrame()

                if (df_full is None) or (hasattr(df_full, 'empty') and df_full.empty):
                    df_full = pd.DataFrame()

                df_head = df_full.head(5) if hasattr(df_full, 'head') else df_full
                return ds, where, limit, df_full, df_head

        tasks = [_process_dataset(ds) for ds in handler]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Dataset filtering task failed: {result}")
                continue

            ds, where, limit, df_full, df_head = result
            if where is not None or limit is not None:
                try:
                    object.__setattr__(ds, "_df_cache", df_full)
                except Exception as e:
                    logger.warning(f"Could not set _df_cache for {ds.name}: {e}")
            data_samples[ds.name] = df_head
            filtered_datasets.append(ds)

        handler._datasets = filtered_datasets
        context["filtered_datasets"] = filtered_datasets
        context["data_samples"] = data_samples
        return context

class DecideRiskSummaryUnit(ConversationalUnit):
    def __init__(self, llm_chat):
        super().__init__("decide_risk_summary")
        self.llm_chat = llm_chat
    async def run(self, context):
        from prompts.app_prompts import get_risk_summary_decision_prompt
        user_text = context["user_text"]
        chat_history = context["chat_history"]
        result = context["parsed_result"]
        risk_decision_prompt = get_risk_summary_decision_prompt(user_text, chat_history, result)
        risk_decision = (await asyncio.to_thread(self.llm_chat.ask, risk_decision_prompt)).strip().lower()
        context["risk_decision"] = risk_decision
        return context

class RiskSummaryUnit(ConversationalUnit):
    def __init__(self):
        super().__init__("risk_summary")
    async def run(self, context):
        from scripts.RiskSummarizer import summarize_risk
        user_text = context["user_text"]
        result = context["parsed_result"]
        handler = context["handler"]
        llm_chat = context["llm_chat"]
        risk_summary = await asyncio.to_thread(summarize_risk, user_text, result, handler, llm_chat)
        context["risk_summary"] = risk_summary
        return context

class DecideShowDataUnit(ConversationalUnit):
    def __init__(self, llm_chat):
        super().__init__("decide_show_data")
        self.llm_chat = llm_chat
    async def run(self, context):
        from prompts.app_prompts import get_show_data_decision_prompt
        user_text = context["user_text"]
        chat_history = context["chat_history"]
        result = context["parsed_result"]
        show_data_decision_prompt = get_show_data_decision_prompt(user_text, chat_history, result)
        show_data_decision = (await asyncio.to_thread(self.llm_chat.ask, show_data_decision_prompt)).strip().lower()
        context["show_data_decision"] = show_data_decision
        return context

class FollowupUnit(ConversationalUnit):
    def __init__(self, llm_chat):
        super().__init__("followup")
        self.llm_chat = llm_chat
    async def run(self, context):
        from prompts.app_prompts import get_followup_prompt
        result = context["parsed_result"]
        risk_summary = context.get("risk_summary")
        followup_prompt = get_followup_prompt(result, risk_summary)
        followup_response = await asyncio.to_thread(self.llm_chat.ask, followup_prompt)
        context["followup_response"] = followup_response
        return context

class ConversationalAnswerUnit(ConversationalUnit):
    def __init__(self, llm_chat):
        super().__init__("conversational_answer")
        self.llm_chat = llm_chat
    async def run(self, context):
        from prompts.app_prompts import get_conversational_answer_prompt
        user_text = context["user_text"]
        chat_history = context["chat_history"]
        response = await asyncio.to_thread(
            self.llm_chat.ask,
            get_conversational_answer_prompt(user_text, chat_history),
        )
        context["conversational_response"] = response
        return context

class ParsedResultFormatUnit(ConversationalUnit):
    def __init__(self):
        super().__init__("parsed_result_format")
    async def run(self, context):
        result = context["parsed_result"]
        categories = result.get('categories', [])
        datasets = result.get('dataset_names', [])
        addresses = result.get('address', [])
        confidence = result.get('confidence')
        cat_str = '\n'.join(f'- {c}' for c in categories) or 'None'
        ds_str = '\n'.join(f'- {d}' for d in datasets) or 'None'
        if addresses:
            addr_str = '\n\n'.join(
                '\n'.join([
                    f"  House Number: {a.get('house_number', '')}",
                    f"  Street Name:  {a.get('street_name', '')}",
                    f"  Borough:      {a.get('borough', '')}",
                    f"  Raw:          {a.get('raw', '')}",
                    f"  Notes:        {a.get('notes', '')}"
                ]) for a in addresses
            )
        else:
            addr_str = 'None'
        formatted = (
            f"**Categories:**\n{cat_str}\n\n"
            f"**Datasets:**\n{ds_str}\n\n"
            f"**Addresses:**\n{addr_str}\n\n"
            f"**Confidence:** {confidence}"
        )
        context["formatted_parsed_result"] = formatted
        return context

class SurroundingDecisionUnit(ConversationalUnit):
    def __init__(self, llm_chat):
        super().__init__("surrounding_decision")
        self.llm_chat = llm_chat
    async def run(self, context):
        from prompts.app_prompts import get_surrounding_decision_prompt
        user_text = context["user_text"]
        chat_history = context["chat_history"]
        parsed_result = context.get("parsed_result", {})
        span_bbls = context.get("span_bbls") or []
        decision_prompt = get_surrounding_decision_prompt(user_text, chat_history, parsed_result, span_bbls)
        decision = (await asyncio.to_thread(self.llm_chat.ask, decision_prompt)).strip().lower()

        valid_choices = {"include_surrounding", "target_only", "use_span"}
        if decision not in valid_choices:
            decision = "use_span" if span_bbls else "target_only"
        if decision == "use_span" and not span_bbls:
            decision = "target_only"
        context["surrounding_decision"] = decision
        return context
