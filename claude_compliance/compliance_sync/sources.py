from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from datetime import datetime

    from .compliance_client import ComplianceClient
from .config import datetime_to_timestamp_str
from .converter import build_message_pairs, pair_to_payload
from .models import (
    ChatSummary,
    LocalSession,
    RemoteSession,
    RemoteSessionListMetadata,
)
from .session_converter import (
    cut_session_interactions,
    resolve_session_end_user,
    session_interaction_to_payload,
)

_OPEN_REMOTE_STATUSES = {"pending", "active", "paused"}


@dataclass(frozen=True)
class ConversationRef:
    source: str
    conversation_id: str
    updated_at: datetime
    deleted: bool = False
    chat: ChatSummary | None = None
    local_session: LocalSession | None = None
    remote_session: RemoteSession | None = None
    metadata_json: str | None = None


@dataclass(frozen=True)
class PendingInteraction:
    key: str
    time_start: datetime
    closed: bool
    payload: dict[str, Any] | None


@dataclass
class ConversationResult:
    updated_at: datetime
    interactions: list[PendingInteraction] = field(default_factory=list)
    user_id: str | None = None
    remote_open: bool = False
    metadata_json: str | None = None
    gone: bool = False
    gone_reason: str | None = None


@dataclass(frozen=True)
class FetchRequest:
    ref: ConversationRef
    from_date: datetime | None
    now: datetime
    idle_minutes: int
    anonymize: bool


def listed_user_missing(ref: ConversationRef) -> bool:
    """True when the listing itself shows there is no attributable user."""
    if ref.chat is not None:
        return ref.chat.user is None
    if ref.local_session is not None:
        return ref.local_session.user is None
    if ref.remote_session is not None:
        session = ref.remote_session
        return session.user is None and session.started_by_user is None
    return False


def remote_listing_status(ref: ConversationRef) -> str | None:
    if ref.remote_session is not None:
        return ref.remote_session.status
    if not ref.metadata_json:
        return None
    status = json.loads(ref.metadata_json).get("status")
    if isinstance(status, str):
        return status
    return None


def _timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    return datetime_to_timestamp_str(value)


class ChatSource:
    name = "chats"

    def __init__(self, client: ComplianceClient, organization_uuid: str) -> None:
        self._client = client
        self._organization_uuid = organization_uuid

    def list_changed(
        self,
        listing_from: datetime | None,
        to_date: datetime | None,
    ) -> list[ConversationRef]:
        pages = self._client.iter_chats(
            [self._organization_uuid],
            updated_at_gte=_timestamp(listing_from),
            updated_at_lte=_timestamp(to_date),
        )
        return [
            ConversationRef(
                source=self.name,
                conversation_id=chat.id,
                updated_at=chat.updated_at,
                deleted=chat.deleted_at is not None,
                chat=chat,
            )
            for page in pages
            for chat in page.data
        ]

    def fetch(self, request: FetchRequest) -> ConversationResult:
        response = self._client.list_chat_messages(
            request.ref.conversation_id,
            created_at_gte=_timestamp(request.from_date),
        )
        if response.deleted_at is not None:
            return ConversationResult(
                updated_at=response.updated_at,
                gone=True,
                gone_reason="deleted",
            )
        chat = ChatSummary.model_validate(
            response.model_dump(
                exclude={"chat_messages", "has_more", "first_id", "last_id"}
            )
        )
        pairs = build_message_pairs(response.chat_messages, chat)
        interactions = [
            PendingInteraction(
                key=pair.assistant_message.id,
                time_start=pair.user_message.created_at,
                closed=True,
                payload=pair_to_payload(pair, anonymize=request.anonymize),
            )
            for pair in pairs
        ]
        user_id = chat.user.id if chat.user is not None else None
        return ConversationResult(
            updated_at=chat.updated_at,
            interactions=interactions,
            user_id=user_id,
        )


class LocalSessionSource:
    name = "local_sessions"

    def __init__(self, client: ComplianceClient, organization_uuid: str) -> None:
        self._client = client
        self._organization_uuid = organization_uuid

    def list_changed(
        self,
        listing_from: datetime | None,
        to_date: datetime | None,
    ) -> list[ConversationRef]:
        # The local list has no upper time bound. --to-date is applied per interaction.
        del to_date
        refs: list[ConversationRef] = []
        sessions = self._client.iter_local_sessions(
            updated_at_gte=_timestamp(listing_from),
        )
        for session in sessions:
            if session.organization_uuid != self._organization_uuid:
                continue
            refs.append(
                ConversationRef(
                    source=self.name,
                    conversation_id=session.id,
                    updated_at=session.updated_at,
                    local_session=session,
                )
            )
        return refs

    def fetch(self, request: FetchRequest) -> ConversationResult:
        session, messages = self._client.list_session_messages(
            "local",
            request.ref.conversation_id,
        )
        if not isinstance(session, LocalSession):
            raise TypeError("local transcript did not return a local session")
        return _session_result(
            session,
            messages,
            request=request,
            source="local_session",
            list_metadata=None,
            remote_open=False,
            metadata_json=None,
        )


class RemoteSessionSource:
    name = "remote_sessions"

    def __init__(self, client: ComplianceClient, organization_uuid: str) -> None:
        self._client = client
        self._organization_uuid = organization_uuid

    def list_changed(
        self,
        listing_from: datetime | None,
        to_date: datetime | None,
    ) -> list[ConversationRef]:
        # Remote lists filter on created_at only. --to-date is applied per interaction.
        del to_date
        refs: list[ConversationRef] = []
        sessions = self._client.iter_remote_sessions(
            [self._organization_uuid],
            created_at_gte=_timestamp(listing_from),
        )
        for session in sessions:
            metadata = RemoteSessionListMetadata.from_remote_session(session)
            refs.append(
                ConversationRef(
                    source=self.name,
                    conversation_id=session.id,
                    updated_at=session.updated_at,
                    remote_session=session,
                    metadata_json=metadata.model_dump_json(),
                )
            )
        return refs

    def fetch(self, request: FetchRequest) -> ConversationResult:
        session, messages = self._client.list_session_messages(
            "remote",
            request.ref.conversation_id,
        )
        if not isinstance(session, RemoteSession):
            raise TypeError("remote transcript did not return a remote session")
        metadata = _remote_metadata(request.ref, session)
        return _session_result(
            session,
            messages,
            request=request,
            source="remote_session",
            list_metadata=metadata,
            remote_open=session.status in _OPEN_REMOTE_STATUSES,
            metadata_json=metadata.model_dump_json(),
        )


def _remote_metadata(
    ref: ConversationRef,
    session: RemoteSession,
) -> RemoteSessionListMetadata:
    if ref.remote_session is not None:
        base = RemoteSessionListMetadata.from_remote_session(ref.remote_session)
    elif ref.metadata_json:
        base = RemoteSessionListMetadata.model_validate_json(ref.metadata_json)
    else:
        base = RemoteSessionListMetadata.from_remote_session(session)
    return base.model_copy(update={"status": session.status})


def _session_result(
    session: LocalSession | RemoteSession,
    messages: list[Any],
    *,
    request: FetchRequest,
    source: Literal["local_session", "remote_session"],
    list_metadata: RemoteSessionListMetadata | None,
    remote_open: bool,
    metadata_json: str | None,
) -> ConversationResult:
    status = session.status if isinstance(session, RemoteSession) else None
    cuts = cut_session_interactions(
        messages,
        remote=isinstance(session, RemoteSession),
        session_updated_at=session.updated_at,
        session_status=status,
        now=request.now,
        idle_minutes=request.idle_minutes,
    )
    interactions = [
        PendingInteraction(
            key=cut.key,
            time_start=cut.time_start,
            closed=cut.closed,
            payload=session_interaction_to_payload(
                cut,
                session,
                source=source,
                list_metadata=list_metadata,
                anonymize=request.anonymize,
            ),
        )
        for cut in cuts
    ]
    return ConversationResult(
        updated_at=session.updated_at,
        interactions=interactions,
        user_id=resolve_session_end_user(session, list_metadata),
        remote_open=remote_open,
        metadata_json=metadata_json,
    )
