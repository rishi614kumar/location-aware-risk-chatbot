import logging
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
    get_conversational_fallback_prompt,
    get_reuse_parsed_decision_prompt
)
from llm.LLMParser import route_query_to_datasets_multi
from config.logger import logger
from scripts.ConversationalUnit import (
    DecideModeUnit, DecideReuseParsedUnit, ParseQueryUnit, DataPreviewUnit, FilterDatasetsUnit,
    DecideRiskSummaryUnit, RiskSummaryUnit, DecideShowDataUnit, FollowupUnit, ConversationalAnswerUnit
)

class ConversationalAgent:
    def __init__(self, chat_backend=None):
        self.llm_chat = chat_backend or Chat(make_backend(provider="gemini"))
        self.chat_history = []
        self.last_parsed_result = None
        # Instantiate units
        self.units = {
            "decide_mode": DecideModeUnit(self.llm_chat),
            "decide_reuse_parsed": DecideReuseParsedUnit(self.llm_chat),
            "parse_query": ParseQueryUnit(),
            "data_preview": DataPreviewUnit(self.llm_chat),
            "filter_datasets": FilterDatasetsUnit(),
            "decide_risk_summary": DecideRiskSummaryUnit(self.llm_chat),
            "risk_summary": RiskSummaryUnit(),
            "decide_show_data": DecideShowDataUnit(self.llm_chat),
            "followup": FollowupUnit(self.llm_chat),
            "conversational_answer": ConversationalAnswerUnit(self.llm_chat)
        }

    async def run(self, user_text):
        self.chat_history.append(user_text)
        history_str = "\n".join(self.chat_history)
        context = {
            "user_text": user_text,
            "chat_history": history_str,
            "llm_chat": self.llm_chat,
            "last_parsed_result": self.last_parsed_result
        }
        # 1. Decide mode
        context = await self.units["decide_mode"].run(context)
        mode = context["mode"]
        logger.info(f"Agent decision: {mode}")

        if mode == "conversational":
            context = await self.units["conversational_answer"].run(context)
            response = context["conversational_response"]
            followup = ''
            if "parsed_result" in context:
                context = await self.units["followup"].run(context)
                followup = context["followup_response"]
            return response, followup

        # 1.5 Decide reuse/reparse
        context = await self.units["decide_reuse_parsed"].run(context)
        reuse_decision = context["reuse_decision"]
        logger.info(f"Reuse parsed decision: {reuse_decision}")
        if reuse_decision == "reuse" and self.last_parsed_result:
            context["parsed_result"] = self.last_parsed_result
            logger.info("Reusing last parsed datasets and addresses.")
        else:
            context = await self.units["parse_query"].run(context)
            self.last_parsed_result = context["parsed_result"]
            logger.info(f"Parsed result: {context['parsed_result']}")

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
        logger.info(f'Formatted parsing output: {formatted}')

        # Data preview
        context = await self.units["data_preview"].run(context)
        context = await self.units["filter_datasets"].run(context)
        llm_data_response = context["llm_data_response"]
        filtered_datasets = context["filtered_datasets"]
        data_samples = context["data_samples"]

        # Decide risk summary
        context = await self.units["decide_risk_summary"].run(context)
        risk_decision = context["risk_decision"]
        logger.info(f"Risk summary decision: {risk_decision}")
        risk_summary = None
        if risk_decision == "risk_summary_needed":
            context = await self.units["risk_summary"].run(context)
            risk_summary = context["risk_summary"]
            logger.info(f'Risk summary: {risk_summary}')

        # Decide show data
        context = await self.units["decide_show_data"].run(context)
        show_data_decision = context["show_data_decision"]
        logger.info(f"Show data decision: {show_data_decision}")
        data_preview_msgs = []
        if show_data_decision == "show_data":
            for ds in filtered_datasets:
                df_head = data_samples.get(ds.name)
                preview = df_head.to_markdown(index=False) if hasattr(df_head, 'to_markdown') else str(df_head)
                msg = f"**{ds.name}**\n{ds.description}\n\nPreview:\n{preview}"
                data_preview_msgs.append(msg)
                logger.info(f'Dataset {ds.name} preview included')

        # Followup
        context = await self.units["followup"].run(context)
        followup = context["followup_response"]

        # Compose final response
        response_parts = [formatted, llm_data_response]
        if data_preview_msgs:
            response_parts.extend(data_preview_msgs)
        if risk_summary:
            response_parts.append(f"**Risk Summary:**\n{risk_summary}")
        response = "\n\n".join(response_parts)
        return response, followup
