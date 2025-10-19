"""
Centralized prompt and system context definitions for LLM interactions.
"""

# --- Prompts for app.py ---

def get_first_message() -> str:
    """
    Returns the initial message prompt for the LLM chat in app.py.
    Edit the template below to change the initial message.
    """
    return (
        '''Hi there! I can help you analyze environmental and compliance risks for any NYC address, street span, or neighborhood.'''
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
        do not actually answer any queries that the user has,
        later stages will address the query.
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
        f'''$(META-PROMPT: Based on the parsed categories ({cats}), datasets ({datasets}), addresses ({addresses}), and the following risk summary:\n{risk_summary}\n\nSuggest a specific, context-aware follow-up question for the user. Only ask about relevant details, trends, or comparisons that would help the user get more insight. Keep in mind that this is at the end of a conversation and should flow naturally from all the given information.)'''
    )

