# A Little Background

Whenever an award or subaward is made, there are several federal agencies tied to the award
including, but not limited to:
* Awarding Agency - The agency that interacted with the recipient to make the award.
* Funding Agency - The agency that provided the bulk of the funding for the award.
* Reporting Agency - The agency that reported the award to the system of record (FSRS, DABS, etc).

These agencies may overlap or be missing entirely from the system of record.

Agencies can also fall into two tiers:
* Toptier Agency - Toptier Agencies are likely what most people think of when they think of
government agencies.  Examples include Department of Defense (DoD), Department of the Treasury
(TREAS), Department of Health and Human Services (HHS), etc.  There are about 195 of these. 
* Subtier Agency - Subtier Agencies are entities that fall within the purview of a Toptier Agency,
for example Office of Inspector General within the Department of the Treasury (TREAS) or National
Institutes of Health (NIH) within the Department of Health and Human Services (HHS).  There are
about 1,500 of these.

An agency can have many subtiers.  Oftentimes (but not always), one of those subtiers
represents the agency as a whole.  For example, 020 is Department of the Treasury.  As of
this writing, Treasury has 45 subtiers.  One of those subtiers, 2000, represents the entirety
of Treasury.  It is roughly equivalent to 020 but at the subtier level.  2004, on the other
hand, represents the Office of Inspector General within Treasury.  `toptier_flag` would be
True for 2000 but False for 2004.

In addition to providing agency details to various other components in the system, this module
also supports the agency typeahead search filter on the advanced search page.  Not all agencies
are available for selection.  The `user_selectable` field controls this functionality.
