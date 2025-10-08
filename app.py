from chainlit.input_widget import TextInput, Select

import json
import chainlit as cl
from chainlit.input_widget import TextInput, Select  # <-- add this

from scripts.geobundle import geo_from_address, geo_from_bbl

BOROUGHS = ["Manhattan", "Brooklyn", "Bronx", "Queens", "Staten Island"]

def bundle_to_json(bundle) -> str:
    try:
        return bundle.json(indent=2) if hasattr(bundle, "json") else json.dumps(bundle, indent=2)
    except Exception:
        return json.dumps({"error": "Failed to serialize bundle"}, indent=2)

@cl.on_chat_start
async def start():
    await cl.Message(
        content="**NYC Site Risk – Quick Lookup**\n\nChoose one:",
        author="assistant",
        actions=[
            cl.Action(name="address_lookup", value="address", label="Lookup by Address", payload={}),
            cl.Action(name="bbl_lookup", value="bbl", label="Lookup by BBL", payload={}),
        ],
    ).send()

@cl.action_callback("address_lookup")
async def on_address_action(action: cl.Action):
    # Use AskUserMessage with input widgets
    res = await cl.AskUserMessage(
        content="Enter address + borough.",
        timeout=600,
        inputs=[
            TextInput(id="Address", label="Address", initial="237 Park Ave", placeholder="Street address"),
            Select(id="Borough", label="Borough", values=BOROUGHS, initial_index=0),
        ],
    ).send()

    address = res.get("Address")
    borough = res.get("Borough")
    if not address or not borough:
        await cl.Message(content="Please provide both **Address** and **Borough**.", author="assistant").send()
        return

    msg = await cl.Message(content="Looking up…", author="assistant").send()
    try:
        bundle = geo_from_address(address, borough)
        await render_result(bundle, hint=f"Address: {address} | Borough: {borough}", prior=msg)
    except Exception as e:
        await msg.update(content=f"❌ Lookup failed: `{e}`")

@cl.action_callback("bbl_lookup")
async def on_bbl_action(action: cl.Action):
    res = await cl.AskUserMessage(
        content="Enter a 10-digit BBL (e.g., 1013007501).",
        timeout=600,
        inputs=[TextInput(id="BBL", label="BBL", initial="1013007501", placeholder="10-digit BBL")],
    ).send()

    bbl = (res or {}).get("BBL")
    if not bbl or len(str(bbl)) != 10:
        await cl.Message(content="Please provide a valid **10-digit BBL**.", author="assistant").send()
        return

    msg = await cl.Message(content="Looking up…", author="assistant").send()
    try:
        bundle = geo_from_bbl(str(bbl))
        await render_result(bundle, hint=f"BBL: {bbl}", prior=msg)
    except Exception as e:
        await msg.update(content=f"❌ Lookup failed: `{e}`")

async def render_result(bundle, hint: str, prior: cl.Message):
    data = bundle.model_dump() if hasattr(bundle, "model_dump") else dict(bundle)
    bbl = data.get("bbl"); precinct = data.get("precinct"); nta = data.get("nta")
    lat = data.get("latitude"); lon = data.get("longitude")

    actions = []
    if bbl:
        actions.append(cl.Action(name="copy_bbl", value=bbl, label="Copy BBL", payload={}))
    if lat and lon:
        gmaps = f"https://www.google.com/maps?q={lat},{lon}"
        actions.append(cl.Action(name="open_map", value=gmaps, label="Open in Google Maps", payload={}))

    lines = []
    if hint: lines.append(f"_{hint}_")
    lines += [
        f"**BBL:** `{bbl or '—'}`",
        f"**Precinct:** `{precinct or '—'}`",
        f"**NTA:** `{nta or '—'}`",
        f"**Coordinates:** `{lat if lat else '—'}, {lon if lon else '—'}`",
    ]
    card_md = "\n".join(lines)

    json_str = bundle_to_json(bundle)
    elements = [
        cl.Text(name="Raw JSON", content=json_str, display="accordion"),
        cl.File(name="bundle.json", content=json_str.encode("utf-8"), display="inline"),
    ]

    await prior.update(content=card_md, actions=actions, elements=elements, author="assistant")

@cl.action_callback("copy_bbl")
async def on_copy_bbl(action: cl.Action):
    await cl.Message(content=f"✅ Copied BBL `{action.value}` to clipboard (or select & copy).").send()

@cl.action_callback("open_map")
async def on_open_map(action: cl.Action):
    await cl.Message(content=f"[Open in Google Maps]({action.value})").send()
