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

class ConversationalAgent:
    def __init__(self, chat_backend=None):
        self.llm_chat = chat_backend or Chat(make_backend(provider="gemini"))
        self.chat_history = []
        self.last_parsed_result = None

    async def run(self, user_text):
        self.chat_history.append(user_text)
        history_str = "\n".join(self.chat_history)
        # Step 1: Decide mode
        decision_prompt = get_decision_prompt(user_text, history_str)
        mode = self.llm_chat.ask(decision_prompt).strip().lower()
        logger.info(f"Agent decision: {mode}")

        if mode == "conversational":
            from prompts.app_prompts import get_conversational_answer_prompt
            response = self.llm_chat.ask(get_conversational_answer_prompt(user_text, history_str))
            logger.info(f"Conversational response: {response}")
            followup = self.llm_chat.ask(get_conversational_fallback_prompt())
            return response, followup

        # Step 1.5: Decide whether to reuse or reparse
        reuse_decision = "reparse"
        print("Last parsed result:", self.last_parsed_result)
        if self.last_parsed_result:
            reuse_decision_prompt = get_reuse_parsed_decision_prompt(user_text, history_str, self.last_parsed_result)
            reuse_decision = self.llm_chat.ask(reuse_decision_prompt).strip().lower()
        logger.info(f"Reuse parsed decision: {reuse_decision}")

        if reuse_decision == "reuse" and self.last_parsed_result:
            result = self.last_parsed_result
            logger.info("Reusing last parsed datasets and addresses.")
        else:
            result = route_query_to_datasets_multi(user_text)
            self.last_parsed_result = result
            logger.info(f"Parsed result: {result}")
        categories = result.get('categories', [])
        datasets = result.get('dataset_names', [])
        addresses = result.get('address', [])
        confidence = result.get('confidence')

        # Show parsing output
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
        handler = DataHandler(datasets)
        logger.info(f"Handler initialized with datasets: {handler.names}")
        llm_data_response = self.llm_chat.ask(get_loading_datasets_prompt(handler))
        logger.info(f'LLM Data Loading Response: {llm_data_response}')

        try:
            logger.info(f"Calling get_dataset_filters with addresses: {addresses}")
            import asyncio
            dataset_filters = await asyncio.to_thread(get_dataset_filters, addresses, handler)
            logger.info(f"Received dataset_filters: {dataset_filters}")
        except Exception as e:
            logger.error(f"Error in get_dataset_filters: {e}")
            dataset_filters = {}

        filtered_datasets = []
        data_samples = {}
        for ds in handler:
            try:
                logger.info(f"Processing dataset: {ds.name}")
                filter_kwargs = dataset_filters.get(ds.name, {})
                where = filter_kwargs.get("where")
                limit = filter_kwargs.get("limit")
                logger.info(f"Filter for {ds.name}: where={where}, limit={limit}")
                import asyncio
                df_full = await asyncio.to_thread(ds.df_filtered, where, limit)
                logger.info(f"Fetched dataframe for {ds.name}, shape: {getattr(df_full, 'shape', None)}")
                df_head = df_full.head(5)
                # Replace DataSet's _df_cache with filtered data if filtering was applied
                if where is not None or limit is not None:
                    object.__setattr__(ds, "_df_cache", df_full)
                data_samples[ds.name] = df_head
                filtered_datasets.append(ds)
            except Exception as e:
                logger.error(f'Error loading dataset {ds.name}: {e}')

        handler._datasets = filtered_datasets

        # Step 2: Decide if risk summary is needed
        risk_decision_prompt = get_risk_summary_decision_prompt(user_text, history_str, result)
        risk_decision = self.llm_chat.ask(risk_decision_prompt).strip().lower()
        logger.info(f"Risk summary decision: {risk_decision}")

        risk_summary = None
        if risk_decision == "risk_summary_needed":
            try:
                import asyncio
                risk_summary = await asyncio.to_thread(summarize_risk, user_text, result, handler, self.llm_chat)
                logger.info(f'Risk summary: {risk_summary}')
            except Exception as e:
                logger.error(f"Error in risk summarization: {e}")
                risk_summary = f"Error in risk summarization: {e}"

        # Step 2.5: Decide if data preview should be shown
        from prompts.app_prompts import get_show_data_decision_prompt
        show_data_decision_prompt = get_show_data_decision_prompt(user_text, history_str, result)
        show_data_decision = self.llm_chat.ask(show_data_decision_prompt).strip().lower()
        logger.info(f"Show data decision: {show_data_decision}")

        data_preview_msgs = []
        if show_data_decision == "show_data":
            for ds in filtered_datasets:
                df_head = data_samples.get(ds.name)
                preview = df_head.to_markdown(index=False) if hasattr(df_head, 'to_markdown') else str(df_head)
                msg = f"**{ds.name}**\n{ds.description}\n\nPreview:\n{preview}"
                data_preview_msgs.append(msg)
                logger.info(f'Dataset {ds.name} preview included')

        # Step 3: Followup
        followup_prompt = get_followup_prompt(result, risk_summary)
        followup_response = self.llm_chat.ask(followup_prompt)
        logger.info(f'Followup response: {followup_response}')

        # Compose final response
        response_parts = [formatted, llm_data_response]
        if data_preview_msgs:
            response_parts.extend(data_preview_msgs)
        if risk_summary:
            response_parts.append(f"**Risk Summary:**\n{risk_summary}")
        response = "\n\n".join(response_parts)
        return response, followup_response
