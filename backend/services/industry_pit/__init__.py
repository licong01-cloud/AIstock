"""C-013 dual-authority industry PIT data contracts."""

from .contracts import (
    AlignmentState,
    AuthorityReceipt,
    AuthorityType,
    CandidateInterval,
    DualAuthorityResolution,
    IndustryPitContractError,
    KnowledgeTimePolicy,
    ResearchBasis,
    ResolutionRequest,
    TaxonomyIdentity,
    UnavailableReason,
)
from .resolver import IndustryPitResolver, resolve_dual_authority

__all__ = [
    "AlignmentState",
    "AuthorityReceipt",
    "AuthorityType",
    "CandidateInterval",
    "DualAuthorityResolution",
    "IndustryPitContractError",
    "IndustryPitResolver",
    "KnowledgeTimePolicy",
    "ResearchBasis",
    "ResolutionRequest",
    "TaxonomyIdentity",
    "UnavailableReason",
    "resolve_dual_authority",
]
