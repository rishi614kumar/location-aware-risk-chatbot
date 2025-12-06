"""
Centralized prompt and system context definitions for LLM interactions.
"""
from config.settings import DATASET_DESCRIPTIONS
from config.logger import logger
# --- Prompts for app.py ---

def get_first_message() -> str:
    """
    Returns the initial message prompt for the LLM chat in app.py.
    Edit the template below to change the initial message.
    """
    return (
        '''Hi there! I can help you analyze environmental and compliance risks for any NYC address or street segment'''
    )

def get_system_prompt() -> str:
    """
    Returns the system prompt for the LLM chat in app.py.
    Edit the template below to change the system context.
    """
    return (
        f'''You are a helpful assistant for NYC risk and compliance questions.
        Respond conversationally and concisely.
        When you receive a META-PROMPT following this format $(META-PROMPT: <text>) you will consider that text as an instruction for how to respond to the user.
        your first message is {get_first_message()}
        '''.strip()
    )

def get_conversational_meta_prompt(user_text: str) -> str:
    """
    Returns the conversational meta-prompt for the LLM response in app.py.
    Edit the template below to change how the LLM responds to user input.
    """
    return (
        f'''$(META-PROMPT: respond conversationally to the user.
        do not actually answer any queries that the user has,later stages will address the query.
        you just need to acknowledge the user's input and let them know you are processing it.
        query: {user_text})'''
    )

def get_followup_prompt(result, risk_summary) -> str:
    """
    Returns a prompt for the LLM to generate a context-aware follow-up question.
    Pass in the parsed results and the risk summary.
    """
    cats = ', '.join(result.get('categories', []))
    datasets = ', '.join(result.get('dataset_names', []))
    addresses = ', '.join(a.get('raw', '') for a in result.get('address', []))
    return (
        f'''$(META-PROMPT: Based on the parsed categories ({cats}), datasets ({datasets}), addresses ({addresses}), and the following risk summary:\n{risk_summary}\n\nSuggest a specific, context-aware follow-up question for the user. Only ask about relevant details, trends, or comparisons that would help the user get more insight. Only ask the user things that can be answered from the information available to you. Keep in mind that this is at the end of a conversation and should flow naturally from all the given information.)'''
    )

def get_loading_datasets_prompt(handler) -> str:
    """
    Returns a string describing what datasets are being loaded, based on the handler object.
    """
    if not hasattr(handler, 'names') or not handler.names:
        names = ["No datasets to load."]
    else:
        names = handler.names
    return (
        f'''$(META-PROMPT: Based on the following datasets being loaded: {', '.join(names)}, inform the user that the data is being fetched and processed. This may take a moment.)'''
    )

def get_conversational_fallback_prompt() -> str:
    """
    Returns a fallback conversational prompt when no data analysis is needed.
    """
    return (
        "$(META-PROMPT: Continue the conversation with the user. No data analysis is needed. Respond helpfully and conversationally.)"
    )

def get_decision_prompt(user_text, chat_history):
    """
    Prompt for LLM to decide between conversational mode and data querying mode.
    """
    return (
        f'''$(META-PROMPT: This is the user's query: \n'{user_text}'\n\n and this is the chat history: \n'{get_chat_history_string(chat_history)}'\n\n Based on this, decide if you should enter conversational mode (just answer the user's question) or data querying mode (parse new addresses, query new datasets, and analyze risk). 
        If the user is just following up on previous information or asking general questions that you are 100% sure that you can already answer WITHOUT fetching specific data for the user's query and address,
        choose conversational mode. 
        If the user is asking for specific information about a location, datasets, or categories that you cannot already answer based on the your knowledge and thechat history, choose data querying mode.
        If the user is asking to see the data, choose data querying mode.
        The datasets you have access to are geo-referenced datasets related to NYC risk and compliance and are as follows: {DATASET_DESCRIPTIONS}
        Respond with either 'conversational' or 'data_query'.)'''
    )

def get_risk_summary_decision_prompt(user_text, chat_history, parsed_result, show_data_decision):
    """
    Prompt for LLM to decide if a risk summary is needed based on query, history, and parsed results.
    """
    data_shown_string = "a preview of the fetched data has been shown to the user" if show_data_decision == "show_data" else "none of the fetched data has been shown to the user"
    return (
        f'''$(META-PROMPT: This is the user's query: \n'{user_text}'\n\n and this is the chat history: \n'{get_chat_history_string(chat_history)}'\n\n and this is the parsed result(s): \n'{parsed_result}'\n\n 
        and {data_shown_string} \n\n Based on this, decide if a summary and analysis of the data should be generated. 
        If the user is asking for specific information that is not already covered in the chat history or parsed results, or by a preview of the fetched data that has already been shown to the user, 
        choose 'data_summary_needed'. Also, if further analysis of the data is needed to still answer the user's question, choose 'data_summary_needed'.
        If the user's question has already been answered, orthe user is asking for general information or follow-ups that do not require an additional data summary and analysis, choose 'data_summary_not_needed'.
        Respond ONLY with either 'data_summary_needed' or 'data_summary_not_needed'.)'''
    )

def get_conversational_answer_prompt(user_text, chat_history=None):
    """
    Returns a prompt for the LLM to directly answer the user's question in conversational mode.
    Optionally includes chat history for context.
    """
    #history_part = f"\nChat history: {chat_history}" if chat_history else ""
    return (
        f"$(META-PROMPT: Answer the user's question directly and helpfully. Be accurate and conversational, answer based on their query and the chat history, if applicable.\n User query: \n'{user_text}'\n\n and this is the chat history: \n'{get_chat_history_string(chat_history)}'\n\n Based on this, answer the user's question directly and helpfully.)"
    )

def get_show_data_decision_prompt(user_text, chat_history, parsed_result):
    """
    Prompt for LLM to decide if the user wants to see the actual data preview.
    Respond with either 'show_data' or 'hide_data'.
    """
    return (
        f"$(META-PROMPT: This is the user's query: \n'{user_text}'\n\n and this is the chat history: \n'{get_chat_history_string(chat_history)}'\n\n and this is the parsed result(s): \n'{parsed_result}'\n\n Based on this, decide if the user is requesting to see the actual data preview. "
        "Note that later on, the user might possibly still get a summary and analysis of the data as well, so it "
        " is only necssary to show the data preview if the user requests it. Respond with 'show_data' if they want to see it, or 'hide_data' if they do not'.)"
    )

def get_reuse_address_decision_prompt(user_text, chat_history, last_addresses):
    """Prompt for deciding whether to reuse previously parsed addresses."""
    addresses_text = ", ".join(a.get('raw', '') or str(a) for a in last_addresses) if last_addresses else "None"
    return (
        f"$(META-PROMPT: This is the user's new message: \n'{user_text}'\n\n and this is the chat history: \n'{get_chat_history_string(chat_history)}'\n\n "
        f" The last known addresses are: '{addresses_text}' \n \n. Based on this, decide whether to reuse the existing addresses or extract new ones from the latest user message."
        " Default to 'reuse' when the user is continuing the same discussion without providing a clearly different address, intersection, neighborhood, borough, precinct, or other geographic reference."
        " Choose 'reparse' only when the user explicitly supplies a new or conflicting location needing fresh parsing. Respond with either 'reuse' or 'reparse'.)"
    )


def get_reuse_dataset_decision_prompt(user_text, chat_history, last_datasets):
    """Prompt for deciding whether to reuse previously parsed datasets."""
    datasets_text = ", ".join(last_datasets) if last_datasets else "None"
    return (
        f"$(META-PROMPT: This is the user's new message: \n'{user_text}'\n\n and this is the chat history: \n'{get_chat_history_string(chat_history)}'\n\n "
        f" The last selected datasets are: '{datasets_text}'\n\n. Based on this, decide whether to reuse these datasets or infer a new set from the latest user message."
        " Default to 'reuse' when the user is following up on the same analysis without asking for additional or different datasets."
        " Choose 'reparse' only if the user clearly requests different datasets, categories, or data views. Respond with either 'reuse' or 'reparse'.)"
    )


def get_intersection_analysis_decision_prompt(user_text, chat_history, parsed_result):
    """Prompt for deciding whether to perform intersection (street span) analysis."""
    addresses = parsed_result.get('address') or []
    address_summaries = []
    for address in addresses:
        raw = (address.get('raw') or '').strip()
        notes = (address.get('notes') or '').strip()
        pieces = [raw] if raw else []
        if notes:
            pieces.append(f"notes={notes}")
        if not pieces:
            street = (address.get('street_name') or '').strip()
            borough = (address.get('borough') or '').strip()
            house = (address.get('house_number') or '').strip()
            fallback = " ".join(part for part in [house, street, borough] if part)
            if fallback:
                pieces.append(fallback)
        address_summaries.append(" / ".join(pieces) if pieces else "<empty>")
    addresses_text = "; ".join(address_summaries) if address_summaries else "None"
    return (
        "$(META-PROMPT: Review the user's latest request, the chat history, and the parsed address details to decide whether the user is describing a street segment bounded by two intersections. "
        f"User message: '{user_text}'. Chat history: '{chat_history}'. Parsed addresses: '{addresses_text}'. "
        "Choose 'intersection' only when the user clearly references a street span between two intersections or provides cross-street bounds that require corridor analysis. "
        "Otherwise respond with 'direct' so that only the specific address or location is analyzed. Respond with either 'intersection' or 'direct'.)"
    )


def get_surrounding_decision_prompt(user_text, chat_history, parsed_result, span_bbls=None):
    """Prompt for deciding whether to include surrounding BBLs (spatial expansion), only the target, or a street-span corridor."""
    span_info = "Span BBLs detected: " + ", ".join(span_bbls or []) if span_bbls else "No span BBLs detected"
    return (
        "$(META-PROMPT: This is the user's query: \n'{user_text}'\n\n and this is the chat history: \n'{get_chat_history_string(chat_history)}'\n\n and this is the parsed result(s): \n'{parsed_result}'\n\n "
        + "and this is the span BBLs: \n'{span_bbls}'\n\n"
        "Based on this, decide the spatial scope for analysis.\n"
        "- Choose 'use_span' when the user specifies a street segment between two intersections; use the provided span BBLs if available.\n"
        "- Choose 'include_surrounding' when the user wants broader context (surrounding blocks, neighborhood, nearby risk, comparative analysis).\n"
        "- Choose 'target_only' when they want only the exact provided address/intersection without expansion.\n"
        "Respond with ONLY one of: 'use_span', 'include_surrounding', 'target_only'.)".strip()
    )

def get_chat_history_string(chat_history):
    """Print the chat history in a readable format."""
    if not chat_history:
        return ""
    history_string = ""
    for message in chat_history:
        # Handle both dict format and string format for backward compatibility
        if isinstance(message, dict):
            role = message.get('role', 'unknown')
            content = message.get('content', '')
            history_string += f"{role}: {content}\n"
        elif isinstance(message, str):
            logger.info(f"Unexpected message format: {message}, no dict with role and content")
            # Fallback for old format where messages were strings
            history_string += f"user: {message}\n"
        else:
            # Skip unexpected formats
            logger.info(f"Unexpected message format: {message}, not a dict or string")
            continue
    return history_string