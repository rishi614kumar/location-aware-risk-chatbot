from typing import Any, Dict
from config.logger import logger

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
        mode = self.llm_chat.ask(decision_prompt).strip().lower()
        context["mode"] = mode
        return context

class DecideReuseParsedUnit(ConversationalUnit):
    def __init__(self, llm_chat):
        super().__init__("decide_reuse_parsed")
        self.llm_chat = llm_chat
    async def run(self, context):
        from prompts.app_prompts import get_reuse_parsed_decision_prompt
        user_text = context["user_text"]
        chat_history = context["chat_history"]
        last_parsed_result = context.get("last_parsed_result")
        reuse_decision = "reparse"
        if last_parsed_result:
            reuse_decision_prompt = get_reuse_parsed_decision_prompt(user_text, chat_history, last_parsed_result)
            reuse_decision = self.llm_chat.ask(reuse_decision_prompt).strip().lower()
        context["reuse_decision"] = reuse_decision
        return context

class ParseQueryUnit(ConversationalUnit):
    def __init__(self):
        super().__init__("parse_query")
    async def run(self, context):
        from llm.LLMParser import route_query_to_datasets_multi
        user_text = context["user_text"]
        result = route_query_to_datasets_multi(user_text)
        context["parsed_result"] = result
        return context

class DataPreviewUnit(ConversationalUnit):
    def __init__(self, llm_chat):
        super().__init__("data_preview")
        self.llm_chat = llm_chat
    async def run(self, context):
        from scripts.DataHandler import DataHandler
        from prompts.app_prompts import get_loading_datasets_prompt
        import asyncio
        result = context["parsed_result"]
        datasets = result.get('dataset_names', [])
        handler = DataHandler(datasets)
        context["handler"] = handler
        context["llm_data_response"] = self.llm_chat.ask(get_loading_datasets_prompt(handler))
        return context

class ResolveBBLsUnit(ConversationalUnit):
    def __init__(self):
        super().__init__("resolve_bbls")
    async def run(self, context):
        from scripts.GeoScope import resolve_bbls_from_addresses
        import asyncio
        addresses = context.get("parsed_result", {}).get('address', [])
        resolved_bbls = await asyncio.to_thread(resolve_bbls_from_addresses, addresses)
        context["resolved_bbls"] = resolved_bbls
        logger.info(f"Resolved BBLs: {resolved_bbls}")
        return context

class AggregateSurroundingBBLsUnit(ConversationalUnit):
    def __init__(self):
        super().__init__("aggregate_surrounding")
    async def run(self, context):
        from scripts.GeoScope import aggregate_surrounding_bbls
        import asyncio
        resolved_bbls = context.get("resolved_bbls", [])
        nearby_bbls = await asyncio.to_thread(aggregate_surrounding_bbls, resolved_bbls, True)
        context["nearby_bbls"] = nearby_bbls
        logger.info(f"Nearby BBLs: {len(nearby_bbls)}")
        return context

class BuildDatasetFiltersUnit(ConversationalUnit):
    def __init__(self):
        super().__init__("build_dataset_filters")
    async def run(self, context):
        from scripts.GeoScope import build_dataset_filters_for_handler
        import asyncio
        handler = context["handler"]
        resolved_bbls = context.get("resolved_bbls", [])
        nearby_bbls = context.get("nearby_bbls", [])
        dataset_filters = await asyncio.to_thread(build_dataset_filters_for_handler, handler, resolved_bbls, nearby_bbls)
        context["dataset_filters"] = dataset_filters
        return context

class FilterDatasetsUnit(ConversationalUnit):
    def __init__(self):
        super().__init__("filter_datasets")
    async def run(self, context):
        handler = context["handler"]
        dataset_filters = context["dataset_filters"]
        import asyncio
        filtered_datasets = []
        data_samples = {}
        for ds in handler:
            filter_kwargs = dataset_filters.get(ds.name, {})
            where = filter_kwargs.get("where")
            limit = filter_kwargs.get("limit")
            try:
                df_full = await asyncio.to_thread(ds.df_filtered, where, limit)
            except Exception as e:
                logger.warning(f"Filtered fetch failed for {ds.name}: {e}; using empty DataFrame")
                import pandas as pd
                df_full = pd.DataFrame()
            # If result empty just keep empty (no fallback to unfiltered)
            if (df_full is None) or (hasattr(df_full, 'empty') and df_full.empty):
                import pandas as pd
                df_full = pd.DataFrame()
            df_head = df_full.head(5) if hasattr(df_full, 'head') else df_full
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
        risk_decision = self.llm_chat.ask(risk_decision_prompt).strip().lower()
        context["risk_decision"] = risk_decision
        return context

class RiskSummaryUnit(ConversationalUnit):
    def __init__(self):
        super().__init__("risk_summary")
    async def run(self, context):
        from scripts.RiskSummarizer import summarize_risk
        import asyncio
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
        show_data_decision = self.llm_chat.ask(show_data_decision_prompt).strip().lower()
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
        followup_response = self.llm_chat.ask(followup_prompt)
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
        response = self.llm_chat.ask(get_conversational_answer_prompt(user_text, chat_history))
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