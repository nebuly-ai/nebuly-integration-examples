"""
Export Copilot Enterprise interactions for *all* users of the tenant
using ONLY the Microsoft Graph *beta* SDK (async).
"""

import asyncio
import os
import httpx
from dotenv import load_dotenv
from typing import Dict, Any, List, Tuple
from datetime import datetime
import hashlib
import uuid

from azure.identity.aio import ClientSecretCredential
from msgraph_beta import GraphServiceClient
from msgraph_beta.generated.users.users_request_builder import UsersRequestBuilder

load_dotenv()

# --------------------------------------------------
# 0) CONFIGURATION
# --------------------------------------------------
TENANT_ID  = os.getenv("AZURE_TENANT_ID") or ""
CLIENT_ID  = os.getenv("AZURE_CLIENT_ID") or ""
CLIENT_SEC = os.getenv("AZURE_CLIENT_SECRET") or ""

SCOPES     = ["https://graph.microsoft.com/.default"]
BATCH_TOP  = 100          # $top for paging users and interactions
MAX_PAR    = 5            # concurrent requests
# GUID of the Microsoft 365 Copilot SKU (ensure this matches your tenant)
COPILOT_SKU = os.getenv("COPILOT_SKU") or "639dec6b-bb19-468b-871c-c5c441c4b0cb"
# GUIDs for Microsoft 365 Enterprise base plans that make Copilot export valid
BASE_E_SKUS = [
    "05e9a617-0261-4cee-bb44-138d3ef5d965",  # Microsoft 365 E3
    "c7df2760-2c81-4ef7-b578-a93e6bd394e2",  # Microsoft 365 E5
]

NEBULY_API_KEY   = os.getenv("NEBULY_API_KEY") or ""
NEBULY_ENDPOINT  = os.getenv("NEBULY_ENDPOINT") or "https://backend.nebuly.com/event-ingestion/api/v2/events/trace_interaction"  # use backend.eu.nebuly.com if needed

# --------------------------------------------------
# 1) CLIENT & CREDENTIAL
# --------------------------------------------------
cred   = ClientSecretCredential(
    tenant_id=TENANT_ID,
    client_id=CLIENT_ID,
    client_secret=CLIENT_SEC
)
client = GraphServiceClient(credentials=cred, scopes=SCOPES)

# --------------------------------------------------
# 2) UTILITY FUNCTIONS (SDK wrappers)
# --------------------------------------------------
async def fetch_all_users(select: str = "id,displayName,mail,assignedLicenses") -> List[Any]:
    """
    Returns the complete list of users (User objects) by paging with $top.
    """
    users = []

    query_params = UsersRequestBuilder.UsersRequestBuilderGetQueryParameters(
        select=select.split(','),
        top=BATCH_TOP,
        # Remove the complex filter - we'll filter in Python instead
    )
    request_config = UsersRequestBuilder.UsersRequestBuilderGetRequestConfiguration(
        query_parameters=query_params,
    )
    page = await client.users.get(request_configuration=request_config)

    if page and page.value:
        users.extend(page.value)
        while page.odata_next_link:
            page = await client.users.with_url(page.odata_next_link).get()
            if page and page.value:
                users.extend(page.value)
    
    # Filter users in Python: must have both base E3/E5 AND Copilot add-on
    filtered_users = []
    for user in users:
        if not user.assigned_licenses:
            continue
            
        has_base_license = False
        has_copilot = False
        
        for license in user.assigned_licenses:
            sku_id = str(license.sku_id)
            if sku_id in BASE_E_SKUS:
                has_base_license = True
            elif sku_id == COPILOT_SKU:
                has_copilot = True
                
        if has_base_license and has_copilot:
            filtered_users.append(user)
    
    return filtered_users


async def fetch_interactions_for_user(user_id: str) -> List[Dict[str, Any]]:
    """
    Downloads *all* Copilot interactions for a user via direct HTTP client.
    """
    items = []
    # Create a new credential for this request
    local_cred = ClientSecretCredential(
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SEC
    )
    token = await local_cred.get_token("https://graph.microsoft.com/.default")
    await local_cred.close()

    async with httpx.AsyncClient() as http_client:
        headers = {
            "Authorization": f"Bearer {token.token}",
            "Accept": "application/json"
        }
        params = {"$top": BATCH_TOP}
        base_url = f"https://graph.microsoft.com/beta/copilot/users/{user_id}/interactionHistory/getAllEnterpriseInteractions"

        response = await http_client.get(base_url, params=params, headers=headers)
        response.raise_for_status()
        response_data = response.json()

        if response_data and response_data.get("value"):
            items.extend(response_data["value"])

            while next_link := response_data.get("@odata.nextLink"):
                response = await http_client.get(next_link, headers=headers)
                response.raise_for_status()
                response_data = response.json()
                if response_data and response_data.get("value"):
                    items.extend(response_data["value"])

    return items

async def send_to_nebuly(prompt: Dict[str, Any], response: Dict[str, Any], user_email: str) -> None:
    """
    Transform Graph aiInteraction into Nebuly trace_interaction payload
    and POST it.
    """
    if not NEBULY_API_KEY:
        print("⚠️  Missing NEBULY_API_KEY – skipping export")
        return

    # --- tags extracted from Graph interaction objects ---------------------
    tags: Dict[str, Any] = {
        "app":        prompt.get("appClass"),                  # IPM.SkypeTeams.Message.Copilot.Word
        "conversation_type":prompt.get("conversationType"),          # appchat / bizchat
        "session_id":       prompt.get("sessionId"),
        "request_id":       prompt.get("requestId"),
        "locale":           prompt.get("locale"),
        "user_attachments_count":len(prompt.get("attachments", [])),
        "user_mentions_count":   len(prompt.get("mentions", [])),
        "user_links_count":      len(prompt.get("links", [])),
    }

    # --- retrieval traces ----------------------------------------------------
    retrieval_traces: List[Dict[str, Any]] = []

    # Attachments
    for att in response.get("attachments", []):
        source = att.get("name") or att.get("contentUrl") or "attachment"
        retrieval_traces.append({
            "source": source,
            "input":  att.get("contentUrl") or source,
            "outputs": [att.get("name") or source]
        })

    # Links
    for link in response.get("links", []):
        url = link.get("href") or link.get("url") or ""
        if url:
            retrieval_traces.append({
                "source": url,
                "input":  url,
                "outputs": [link.get("displayName") or url]
            })

    # Mentions
    for ment in response.get("mentions", []):
        text = ment.get("mentionText") or str(ment.get("id"))
        retrieval_traces.append({
            "source": text,
            "input":  text,
            "outputs": []
        })

    # --- naive field mapping ------------------------------------------------
    conv_id = prompt.get("sessionId") or prompt.get("conversationId") or str(uuid.uuid4())
    user_in = prompt.get("body", {}).get("content") or ""
    llm_out = response.get("body", {}).get("content") or ""
    started = prompt.get("createdDateTime") or datetime.utcnow().isoformat()
    ended   = response.get("completedDateTime") or response.get("createdDateTime") or started

    # fallback anonymized user id
    user_hash = hashlib.sha256(user_email.encode()).hexdigest()

    payload = {
        "interaction": {
            "conversation_id": conv_id,
            "input":  user_in,
            "output": llm_out,
            "time_start": started,
            "time_end":   ended,
            "end_user":   user_hash,
            "tags": tags,
            "feature_flag": []
        },
        "traces": retrieval_traces,
        "feedback_actions": [],
        "anonymize": True
    }

    async with httpx.AsyncClient(timeout=15) as nclient:
        res = await nclient.post(
            NEBULY_ENDPOINT,
            headers={
                "Authorization": f"Bearer {NEBULY_API_KEY}",
                "Content-Type": "application/json"
            },
            json=payload
        )
        if res.status_code >= 300:
            print(f"❌ Nebuly POST failed [{res.status_code}] {res.text[:120]}")

# --------------------------------------------------
# 3) MAIN PIPELINE
# --------------------------------------------------
async def main() -> None:
    users = await fetch_all_users()
    print(f"➡  Found {len(users)} users in the tenant\n")

    sem = asyncio.Semaphore(MAX_PAR)

    async def process(u: Any) -> None:
        async with sem:
            uid   = u.id
            email = (u.mail or u.user_principal_name or uid).replace(":", "_")
            print(f"[+] {email:<35} → download in progress…")


            try:
                data = await fetch_interactions_for_user(uid)
            except httpx.HTTPStatusError as e:
                # Skip users that trigger 403 (no Copilot licence / or tenant not enabled)
                if e.response.status_code == 403:
                    print(f"[!] {email:<35} → 403 – skipping")
                    return
                raise  # re‑raise anything that's not 403

            # Pair prompt & response by requestId
            pending: Dict[str, Dict[str, Any]] = {}
            paired: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []

            for inter in data:
                rid = inter.get("requestId")
                if not rid:
                    continue
                if inter.get("interactionType") == "userPrompt":
                    pending[rid] = inter
                elif inter.get("interactionType") == "aiResponse" and rid in pending:
                    paired.append((pending.pop(rid), inter))

            for prompt, resp in paired:
                await send_to_nebuly(prompt, resp, email)

            print(f"    pushed {len(paired):>4} prompt/response pairs to Nebuly ✅")

    await asyncio.gather(*(process(u) for u in users))

if __name__ == "__main__":
    asyncio.run(main())